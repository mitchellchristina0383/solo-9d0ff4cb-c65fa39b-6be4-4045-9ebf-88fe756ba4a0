"""跨分片有序合并引擎。

协议（每个分片先给最新排序键，再给明细，每批最多 20 条）：
1. head：分片先声明本次可见的最新排序键，作为该分片尚未产出部分的“上界”；
2. batch：明细按 (timestamp, id) 倒序分批读取，每批最多 batch_size 条。

合并规则（惰性 k 路归并，确认一条才输出一条）：
- 每个分片记录一个“上界”：未读缓冲首条的 key；缓冲空但未耗尽时用上一批最后
  一条 key（exclusive_upper）；刚拿到 head 的分片上界即 head key。
- 每轮取所有活动分片里“上界最大”的属主：
  * 属主缓冲非空 -> 其首条已被其它所有分片的上界夹住，可立即确认输出，
    较旧分片的明细无需读取（“确认了前 N 条就立即返回”）；
  * 属主缓冲为空（pending）-> 必须先读它的下一批明细才能继续，
    此时任何其它分片的行都不能提前输出（属主下一批可能存在更靠前的行）。
- 到达时限仍不能补齐 N 条：返回已确认的最长有序前缀，并报告挡住后续的分片。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .shard import ShardSnapshot

SortKey = tuple[float, str]


@dataclass
class ShardRuntime:
    snapshot: Any
    shard_id: int
    upper: SortKey | None = None          # 该分片剩余数据的严格上界
    buffer: list[dict[str, Any]] = field(default_factory=list)
    cursor: SortKey | None = None          # 下一批明细的游标（上一批最后一条 key）
    more: bool = True                      # 分片是否可能还有数据
    head_key: SortKey | None = None
    visible_rows: int = 0
    head_done: bool = False
    exhausted: bool = False


@dataclass
class MergeResult:
    alerts: list[dict[str, Any]]
    complete: bool
    status: str                    # complete / exhausted / timeout
    blockers: list[dict[str, Any]]
    runtimes: list[ShardRuntime]
    snapshot_at: float
    deadline: float
    elapsed: float


def _key(row: dict[str, Any]) -> SortKey:
    return float(row["timestamp"]), str(row["id"])


async def _read_with_deadline(
    loop: asyncio.AbstractEventLoop,
    func,
    deadline: float,
) -> Any:
    remaining = max(deadline - loop.time(), 0.0)
    return await asyncio.wait_for(loop.run_in_executor(None, func), timeout=remaining)


async def _read_batch(
    loop: asyncio.AbstractEventLoop,
    owner: ShardRuntime,
    deadline: float,
):
    """带可取消人工延迟的明细批次读取（延迟只加在明细、不加在 head）。"""
    delay = getattr(owner.snapshot, "delay_seconds", 0.0)
    if delay:
        await asyncio.wait_for(asyncio.sleep(delay), timeout=max(deadline - loop.time(), 0.0))
    return await _read_with_deadline(
        loop, lambda: owner.snapshot.read_batch(owner.cursor), deadline
    )


async def merge_latest(snapshots: list[ShardSnapshot], n: int, timeout: float) -> MergeResult:
    """对已打开的分片快照做有序合并，最多返回确认后的前 n 条。"""
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    deadline = started_at + max(timeout, 0.0)
    snapshot_wall = time.time()

    runtimes = [
        ShardRuntime(snapshot=snap, shard_id=snap.shard_id) for snap in snapshots
    ]
    out: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []

    def _mark_timeout(phase: str, rts: list[ShardRuntime], reason: str) -> None:
        elapsed = loop.time() - started_at
        for rt in rts:
            blockers.append(
                {"shard": rt.shard_id, "phase": phase, "reason": reason, "elapsed": elapsed}
            )

    # ---- 阶段 1：并行取各分片最新排序键（head）与可见行数 ----
    try:
        heads = await asyncio.gather(
            *[_read_with_deadline(loop, rt.snapshot.head, deadline) for rt in runtimes]
        )
        counts = await asyncio.gather(
            *[_read_with_deadline(loop, rt.snapshot.count_visible, deadline) for rt in runtimes]
        )
    except (asyncio.TimeoutError, asyncio.CancelledError):
        unfinished = [rt for rt in runtimes if not rt.head_done]
        _mark_timeout("head", unfinished, "等待分片最新排序键超过查询时限")
        return MergeResult(out, False, "timeout", blockers, runtimes,
                           snapshot_wall, deadline, loop.time() - started_at)

    for rt, head, total in zip(runtimes, heads, counts):
        rt.head_done = True
        rt.head_key = head
        rt.visible_rows = int(total)
        rt.upper = head
        if head is None:
            rt.exhausted = True  # 空分片

    # ---- 阶段 2：惰性 k 路归并 ----
    while len(out) < n:
        active = [rt for rt in runtimes if not rt.exhausted and rt.upper is not None]
        if not active:
            return MergeResult(out, True, "exhausted", blockers, runtimes,
                               snapshot_wall, deadline, loop.time() - started_at)

        # 全局下一条必然来自当前上界最大的分片
        owner = max(active, key=lambda rt: rt.upper)  # type: ignore[arg-type]

        if owner.buffer:
            # 首条已被其它分片的上界夹住，确认输出
            row = owner.buffer.pop(0)
            row_out = dict(row)
            row_out["shard"] = owner.shard_id
            out.append(row_out)

            if not owner.buffer:
                last_key = _key(row)
                owner.cursor = last_key
                if owner.more:
                    owner.upper = last_key  # 后续数据严格小于该 key
                else:
                    owner.upper = None
                    owner.exhausted = True
            else:
                owner.upper = _key(owner.buffer[0])
            continue

        # 属主在等明细：读它之前先看时限
        if loop.time() >= deadline:
            _mark_timeout("batch", [owner], "到达查询时限，下一批明细尚未读取")
            return MergeResult(out, False, "timeout", blockers, runtimes,
                               snapshot_wall, deadline, loop.time() - started_at)

        try:
            batch = await _read_batch(loop, owner, deadline)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _mark_timeout("batch", [owner], "明细批次读取超过查询时限")
            return MergeResult(out, False, "timeout", blockers, runtimes,
                               snapshot_wall, deadline, loop.time() - started_at)

        owner.buffer = list(batch.rows)
        owner.more = batch.more
        if owner.buffer:
            owner.upper = _key(owner.buffer[0])
        elif not owner.more:
            owner.upper = None
            owner.exhausted = True

    return MergeResult(out, True, "complete", blockers, runtimes,
                       snapshot_wall, deadline, loop.time() - started_at)
