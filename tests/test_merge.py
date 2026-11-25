"""合并引擎单测：用内存假分片，确定性地控制每批延迟与批次大小。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from app.merge import merge_latest

BATCH = 20


def make_row(alert_id: str, ts: float, shard: int) -> dict:
    return {
        "id": alert_id,
        "timestamp": ts,
        "level": "info",
        "source": f"host-{shard}",
        "message": alert_id,
        "shard": shard,
    }


@dataclass
class FakeSnapshot:
    shard_id: int
    rows: list[dict]
    batch_delay: float = 0.0
    batches_read: int = 0
    fail_at_batch: int | None = None  # 从 1 起；该批次睡眠超过给定时限
    _cursor_idx: int = 0
    closed: bool = False

    def head(self):
        if not self.rows:
            return None
        r = self.rows[0]
        return (float(r["timestamp"]), str(r["id"]))

    def count_visible(self) -> int:
        return len(self.rows)

    def read_batch(self, cursor):
        self.batches_read += 1
        if self.fail_at_batch is not None and self.batches_read >= self.fail_at_batch:
            # 模拟慢到超过查询时限的分片
            import time as _time

            _time.sleep(2.0)
        if self.batch_delay:
            import time as _time

            _time.sleep(self.batch_delay)
        chunk = self.rows[self._cursor_idx : self._cursor_idx + BATCH]
        self._cursor_idx += len(chunk)
        more = len(chunk) == BATCH and self._cursor_idx < len(self.rows)
        return type("Batch", (), {"rows": [dict(r) for r in chunk], "more": more})()

    def close(self) -> None:
        self.closed = True


def shard_rows(shard: int, keys: list[tuple[float, str]]) -> list[dict]:
    return [make_row(aid, ts, shard) for ts, aid in keys]


def expected_order(all_rows: list[dict], n: int) -> list[tuple[float, str]]:
    ordered = sorted(all_rows, key=lambda r: (r["timestamp"], r["id"]), reverse=True)
    return [(r["timestamp"], r["id"]) for r in ordered[:n]]


def result_keys(result) -> list[tuple[float, str]]:
    return [(r["timestamp"], r["id"]) for r in result.alerts]


# ---------- 基本合并 ----------

def test_merge_basic_order_and_tie_break():
    s0 = FakeSnapshot(0, shard_rows(0, [(10.0, "a"), (8.0, "c"), (5.0, "e")]))
    s1 = FakeSnapshot(1, shard_rows(1, [(9.0, "b"), (8.0, "z")]))  # 同 ts=8，z 应在 c 前
    s2 = FakeSnapshot(2, shard_rows(2, [(11.0, "x"), (7.0, "g")]))
    all_rows = s0.rows + s1.rows + s2.rows

    result = asyncio.run(merge_latest([s0, s1, s2], n=10, timeout=5))

    assert result.status == "exhausted"
    assert result.complete is True
    assert result_keys(result) == expected_order(all_rows, 10)
    # 同时间戳 8.0：id 倒序，z 在 c 前
    keys = result_keys(result)
    assert keys.index((8.0, "z")) < keys.index((8.0, "c"))


def test_merge_returns_first_n_and_skips_old_shard_batches():
    # 分片 2 持有全部最新数据，前 N 条确认后不应读分片 0/1 的明细
    s0 = FakeSnapshot(0, shard_rows(0, [(1.0 + i, f"old-{i:03d}") for i in range(40)]))
    s1 = FakeSnapshot(1, shard_rows(1, [(100.0 + i, f"mid-{i:03d}") for i in range(40)]))
    s2 = FakeSnapshot(2, shard_rows(2, [(1000.0 + i, f"new-{i:03d}") for i in range(40)]))

    result = asyncio.run(merge_latest([s0, s1, s2], n=15, timeout=5))

    assert result.status == "complete"
    assert result.count if hasattr(result, "count") else len(result.alerts) == 15
    assert len(result.alerts) == 15
    assert all(r["shard"] == 2 for r in result.alerts)
    # head 不算批次；分片 0/1 一批明细都没读，分片 2 只读 1 批（20 >= 15）
    assert s0.batches_read == 0
    assert s1.batches_read == 0
    assert s2.batches_read == 1


def test_merge_batch_size_caps_at_20_and_paginates():
    rows = shard_rows(0, [(100.0 - i, f"r-{i:03d}") for i in range(45)])
    s0 = FakeSnapshot(0, rows)
    s1 = FakeSnapshot(1, [])
    s2 = FakeSnapshot(2, [])

    result = asyncio.run(merge_latest([s0, s1, s2], n=45, timeout=5))

    # n 恰好等于全库行数：拿满 N 即 complete（不是 exhausted）
    assert result.status == "complete"
    assert result.complete is True
    assert len(result.alerts) == 45
    # 45 条 -> 20 + 20 + 5 共 3 批
    assert s0.batches_read == 3
    assert result_keys(result) == expected_order(rows, 45)


def test_merge_exhausted_when_fewer_than_n():
    rows = shard_rows(0, [(2.0, "a"), (1.0, "b")])
    result = asyncio.run(
        merge_latest([FakeSnapshot(0, rows), FakeSnapshot(1, []), FakeSnapshot(2, [])],
                     n=10, timeout=5)
    )
    assert result.status == "exhausted"
    assert result.complete is True
    assert len(result.alerts) == 2


def test_merge_global_order_across_paginated_shards():
    # 两个分片交错，且都需要翻批；验证跨批归并不乱序
    keys0 = [(float(i), f"s0-{i:03d}") for i in range(0, 60, 2)]   # 偶数
    keys1 = [(float(i), f"s1-{i:03d}") for i in range(1, 60, 2)]   # 奇数
    s0 = FakeSnapshot(0, shard_rows(0, sorted(keys0, reverse=True)))
    s1 = FakeSnapshot(1, shard_rows(1, sorted(keys1, reverse=True)))
    s2 = FakeSnapshot(2, [])
    all_rows = s0.rows + s1.rows

    result = asyncio.run(merge_latest([s0, s1, s2], n=60, timeout=5))

    assert result.status == "complete"
    assert result_keys(result) == expected_order(all_rows, 60)


def test_merge_empty_shards():
    s0 = FakeSnapshot(0, [])
    s1 = FakeSnapshot(1, shard_rows(1, [(3.0, "a"), (2.0, "b")]))
    s2 = FakeSnapshot(2, [])

    result = asyncio.run(merge_latest([s0, s1, s2], n=10, timeout=5))

    assert result.status == "exhausted"
    assert result_keys(result) == [(3.0, "a"), (2.0, "b")]
    assert result.runtimes[0].exhausted and result.runtimes[2].exhausted


# ---------- 超时 ----------

def test_merge_timeout_reports_prefix_and_blocker():
    # 分片 2 持有最新 key，但它第一批明细就慢到超时；
    # 在它的明细返回前，其它分片的行一条都不能确认
    s0 = FakeSnapshot(0, shard_rows(0, [(10.0, "a"), (9.0, "b")]))
    s1 = FakeSnapshot(1, shard_rows(1, [(11.0, "c")]))
    s2 = FakeSnapshot(2, shard_rows(2, [(100.0, "z")]), fail_at_batch=1)

    result = asyncio.run(merge_latest([s0, s1, s2], n=5, timeout=0.3))

    assert result.status == "timeout"
    assert result.complete is False
    assert result.alerts == []  # 属主未确认前不能输出任何行
    assert len(result.blockers) == 1
    assert result.blockers[0]["shard"] == 2
    assert result.blockers[0]["phase"] == "batch"


def test_merge_timeout_after_partial_prefix():
    # 先确认分片 2 的 25 条（跨两批），随后它在第三批卡住；
    # 应返回已确认的最长有序前缀（25 条中前 N 条），并指出分片 2 阻塞
    rows2 = shard_rows(2, [(1000.0 - i, f"z-{i:03d}") for i in range(45)])
    s0 = FakeSnapshot(0, shard_rows(0, [(1.0, "old")]))
    s1 = FakeSnapshot(1, shard_rows(1, [(2.0, "old2")]))

    class SlowAfterTwo(FakeSnapshot):
        def read_batch(self, cursor):
            if self.batches_read + 1 >= 3:
                import time as _time

                _time.sleep(2.0)
            return super().read_batch(cursor)

    s2 = SlowAfterTwo(2, rows2)

    # n=41 > 前两批容量 40，必然请求会睡眠 2s 的第三批；时限 0.15s 先到
    result = asyncio.run(merge_latest([s0, s1, s2], n=41, timeout=0.15))

    assert result.status == "timeout"
    assert len(result.alerts) == 40  # 前两批能确认的全部
    assert all(r["shard"] == 2 for r in result.alerts)
    assert result.blockers[0]["shard"] == 2
    # 旧分片明细从未被读取
    assert s0.batches_read == 0 and s1.batches_read == 0


def test_merge_slow_but_within_timeout_succeeds():
    s0 = FakeSnapshot(0, shard_rows(0, [(5.0, "a")]), batch_delay=0.05)
    s1 = FakeSnapshot(1, shard_rows(1, [(6.0, "b")]), batch_delay=0.05)
    s2 = FakeSnapshot(2, shard_rows(2, [(7.0, "c")]), batch_delay=0.05)

    result = asyncio.run(merge_latest([s0, s1, s2], n=3, timeout=2.0))

    # n=3 恰好等于总行数，拿满即 complete
    assert result.status == "complete"
    assert result.complete is True
    assert result_keys(result) == [(7.0, "c"), (6.0, "b"), (5.0, "a")]
