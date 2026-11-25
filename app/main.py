"""FastAPI 入口：告警录入、分片查询、补报测试。"""
from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
import sqlite3

from .config import settings, shard_delay
from .merge import merge_latest
from .models import AlertOut, AppendRequest, Blocker, LatestResponse, ShardStat
from .shard import Shard, ShardSnapshot


class ShardRegistry:
    def __init__(self) -> None:
        self.shards = [Shard(i) for i in range(settings.shard_count)]
        # 串行化同分片写入；快照读走独立连接，不受影响
        self._write_locks = [asyncio.Lock() for _ in self.shards]

    async def append(self, shard_id: int, req: AppendRequest) -> dict:
        if not 0 <= shard_id < len(self.shards):
            raise HTTPException(status_code=404, detail=f"分片 {shard_id} 不存在")
        lock = self._write_locks[shard_id]
        loop = asyncio.get_running_loop()
        async with lock:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.shards[shard_id].append(
                        req.id, req.timestamp, req.level, req.source, req.message
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise HTTPException(status_code=409, detail=f"告警编号已存在: {req.id}") from exc
        return {"stored": True, "shard": shard_id, "id": req.id}

    def open_snapshots(self) -> list[ShardSnapshot]:
        return [s.open_snapshot() for s in self.shards]

    async def close(self) -> None:
        for shard in self.shards:
            await asyncio.get_running_loop().run_in_executor(None, shard.close)


registry: ShardRegistry | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global registry
    registry = ShardRegistry()
    try:
        yield
    finally:
        assert registry is not None
        await registry.close()


app = FastAPI(
    title="分片告警查询服务",
    description=(
        "三个分片各持一份只追加 SQLite 库。查询只看开始时已提交的告警，"
        "按 (timestamp, id) 倒序合并；分片先给最新排序键再给明细，每批最多 20 条；"
        "确认前 N 条立即返回，超时返回最长有序前缀并指出阻塞分片。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "shards": settings.shard_count}


@app.post("/alerts/{shard_id}", summary="向指定分片录入（追加）一条告警")
async def append_alert(shard_id: int, req: AppendRequest) -> dict:
    assert registry is not None
    return await registry.append(shard_id, req)


@app.post("/alerts/{shard_id}/auto", summary="自动生成编号并追加一条告警（便于 /docs 补报测试）")
async def append_auto(
    shard_id: int,
    timestamp: float | None = Query(None, description="缺省取服务器当前时间"),
    level: str = Query("info"),
    source: str = Query("manual"),
    message: str = Query(""),
) -> dict:
    assert registry is not None
    req = AppendRequest(
        id=str(uuid.uuid4()),
        timestamp=time.time() if timestamp is None else timestamp,
        level=level,
        source=source,
        message=message,
    )
    return await registry.append(shard_id, req)


@app.get(
    "/alerts/latest",
    response_model=LatestResponse,
    summary="查询三个分片合并后的最近 N 条告警",
)
async def latest_alerts(
    n: int = Query(..., ge=1, le=10_000, description="需要的条数"),
    timeout: float = Query(
        ..., gt=0, le=120, description="查询时限（秒），到时未补齐则返回有序前缀"
    ),
) -> LatestResponse:
    assert registry is not None
    snapshots = registry.open_snapshots()
    try:
        result = await merge_latest(snapshots, n=n, timeout=timeout)
    finally:
        for snap in snapshots:
            snap.close()

    alerts = [
        AlertOut(
            id=row["id"],
            timestamp=row["timestamp"],
            shard=row["shard"],
            level=row["level"],
            source=row["source"],
            message=row["message"],
        )
        for row in result.alerts
    ]
    stats = [
        ShardStat(
            shard=rt.shard_id,
            visible_rows=rt.visible_rows,
            head_key=list(rt.head_key) if rt.head_key else None,
            batches_read=rt.snapshot.batches_read,
            delay_seconds=shard_delay(rt.shard_id),
        )
        for rt in result.runtimes
    ]
    return LatestResponse(
        alerts=alerts,
        count=len(alerts),
        requested_n=n,
        complete=result.complete,
        status=result.status,
        timed_out=result.status == "timeout",
        elapsed_ms=result.elapsed * 1000,
        snapshot_at=result.snapshot_at,
        shards=stats,
        blockers=[Blocker(**b) for b in result.blockers],
        batch_size=settings.batch_size,
    )
