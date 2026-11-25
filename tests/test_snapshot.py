"""SQLite 快照测试：查询开始后追加的告警（补报）对本次查询不可见。"""
from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

# 让测试在独立临时库目录运行（与 test_api 隔离，避免跨文件数据污染）
_TMP = tempfile.mkdtemp(prefix="alert-db-snap-")
os.environ["ALERT_DB_DIR"] = _TMP
os.environ["ALERT_SHARD_DELAYS"] = "0,0,0"

from app.config import settings  # noqa: E402
from app.merge import merge_latest  # noqa: E402
from app.shard import Shard  # noqa: E402


@pytest.fixture()
def shards():
    parts = [Shard(i) for i in range(settings.shard_count)]
    for shard in parts:
        shard._write_conn.execute("DELETE FROM alerts")
        shard._write_conn.commit()
    for i, shard in enumerate(parts):
        for j in range(3):
            shard.append(f"s{i}-{j}", 100.0 + i * 10 + j, "info", "t", f"{i}-{j}")
    yield parts
    for shard in parts:
        shard.close()


def test_snapshot_excludes_late_append(shards):
    snapshots = [s.open_snapshot() for s in shards]
    try:
        # 快照打开之后才提交：时间戳比所有现有数据都新
        shards[1].append("LATE", 9999.0, "error", "late", "查询中的补报")

        result = asyncio.run(merge_latest(snapshots, n=100, timeout=5))
        ids = {r["id"] for r in result.alerts}
        assert "LATE" not in ids
        assert len(result.alerts) == 9  # 只看到开始时已提交的 9 条
        assert result.status == "exhausted"
    finally:
        for snap in snapshots:
            snap.close()

    # 新查询能看到补报
    snapshots2 = [s.open_snapshot() for s in shards]
    try:
        result = asyncio.run(merge_latest(snapshots2, n=100, timeout=5))
        assert result.alerts[0]["id"] == "LATE"
        assert len(result.alerts) == 10
    finally:
        for snap in snapshots2:
            snap.close()


def test_snapshot_order_and_visible_counts(shards):
    snapshots = [s.open_snapshot() for s in shards]
    try:
        result = asyncio.run(merge_latest(snapshots, n=2, timeout=5))
        assert len(result.alerts) == 2
        # 最新两条来自分片 2（120..122）
        assert [r["id"] for r in result.alerts] == ["s2-2", "s2-1"]
        assert [rt.visible_rows for rt in result.runtimes] == [3, 3, 3]
        # 只需要分片 2 的一批
        assert result.runtimes[2].snapshot.batches_read == 1
        assert result.runtimes[0].snapshot.batches_read == 0
        assert result.runtimes[1].snapshot.batches_read == 0
    finally:
        for snap in snapshots:
            snap.close()
