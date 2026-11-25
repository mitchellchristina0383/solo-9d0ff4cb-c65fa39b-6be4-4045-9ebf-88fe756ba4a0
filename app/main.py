"""FastAPI 入口：告警录入、分片查询、分片巡检。"""
from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, HTTPException, Query, Request
import sqlite3

from .config import settings, shard_delay
from .merge import merge_latest
from .models import (
    AlertOut,
    AppendRequest,
    Blocker,
    LatestResponse,
    ProbeRequestBody,
    ProbeResponse,
    ShardStat,
)
from .probe import ProbeManager, serialize_job
from .shard import Shard, ShardSnapshot


class ShardRegistry:
    def __init__(self) -> None:
        self.shards = [Shard(i) for i in range(settings.shard_count)]
        # 串行化同分片写入；快照读走独立连接，不受影响
        self._write_locks = [asyncio.Lock() for _ in self.shards]

    def write_lock(self, shard_id: int) -> asyncio.Lock:
        return self._write_locks[shard_id]

    async def append(
        self, shard_id: int, req: AppendRequest, *, is_probe: bool = False
    ) -> dict:
        if not 0 <= shard_id < len(self.shards):
            raise HTTPException(status_code=404, detail=f"分片 {shard_id} 不存在")
        lock = self._write_locks[shard_id]
        loop = asyncio.get_running_loop()
        async with lock:
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self.shards[shard_id].append(
                        req.id, req.timestamp, req.level, req.source, req.message,
                        is_probe=is_probe,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise HTTPException(status_code=409, detail=f"告警编号已存在: {req.id}") from exc
        return {"stored": True, "shard": shard_id, "id": req.id}

    async def delete_probe(self, shard_id: int, probe_id: str) -> int:
        """删除单条探测行；与录入共用同一把分片写锁。"""
        loop = asyncio.get_running_loop()
        async with self._write_locks[shard_id]:
            return await loop.run_in_executor(
                None, self.shards[shard_id].delete_probe, probe_id
            )

    async def delete_all_probes(self, shard_id: int) -> int:
        loop = asyncio.get_running_loop()
        async with self._write_locks[shard_id]:
            return await loop.run_in_executor(
                None, self.shards[shard_id].delete_all_probes
            )

    def open_snapshots(self) -> list[ShardSnapshot]:
        return [s.open_snapshot() for s in self.shards]

    async def close(self) -> None:
        for shard in self.shards:
            await asyncio.get_running_loop().run_in_executor(None, shard.close)


registry: ShardRegistry | None = None
probes: ProbeManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global registry, probes
    registry = ShardRegistry()
    probes = ProbeManager(registry)
    # 重启/启动第一件事：清掉上一进程中断遗留的探测行
    await probes.cleanup_on_startup()
    try:
        yield
    finally:
        assert probes is not None and registry is not None
        await probes.shutdown()
        await registry.close()


app = FastAPI(
    title="分片告警查询服务",
    description=(
        "三个分片各持一份只追加 SQLite 库。查询只看开始时已提交的告警，"
        "按 (timestamp, id) 倒序合并；分片先给最新排序键再给明细，每批最多 20 条；"
        "确认前 N 条立即返回，超时返回最长有序前缀并指出阻塞分片。\n\n"
        "**分片巡检**：`POST /probe/{shard_id}` 走原录入链路写入一条探测告警，"
        "再开新快照按分批明细逻辑读回；只有读回才算通过。总时限到时先返回并指明"
        "卡在录入还是读回，后台仍会清理探测行；重启时也会清扫遗留探测行。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    assert probes is not None
    return {
        "status": "ok",
        "shards": settings.shard_count,
        "probe_startup_cleanup": probes.startup_cleanup,
        "probe_jobs_known": len(probes.jobs),
    }


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


# ---------------- 分片巡检 ----------------


async def _execute_probe(
    shard_id: int,
    request: Request,
    timeout: float | None,
    message: str | None,
    body: ProbeRequestBody | None,
) -> ProbeResponse:
    assert registry is not None and probes is not None
    if not 0 <= shard_id < settings.shard_count:
        raise HTTPException(status_code=404, detail=f"分片 {shard_id} 不存在")

    total_timeout = timeout
    if total_timeout is None and body is not None:
        total_timeout = (
            body.timeout
            if body.timeout is not None
            else body.total_timeout
            if body.total_timeout is not None
            else body.timeout_seconds
        )
    if total_timeout is None:
        raise HTTPException(
            status_code=422,
            detail="必须填写总时限：query 参数 timeout（秒）或 JSON body.timeout",
        )

    probe_message = message or (body.message if body is not None else None) or ""
    loop = asyncio.get_running_loop()
    job = await probes.start_and_wait(shard_id, float(total_timeout), probe_message)
    status_url = f"{request.base_url}probe/jobs/{job.job_id}"
    return ProbeResponse(
        **serialize_job(job, (loop.time() - job.started_loop) * 1000.0, status_url)
    )


@app.post(
    "/probe/{shard_id}",
    response_model=ProbeResponse,
    tags=["分片巡检"],
    summary="分片巡检：录入一条探测告警并分批读回校验（只有读回才算通过）",
)
@app.get("/probe/{shard_id}", response_model=ProbeResponse, include_in_schema=False)
# 语义等价的路径/方法别名（不在 /docs 重复展示），兼容不同调用习惯
@app.post("/shards/{shard_id}/probe", response_model=ProbeResponse, include_in_schema=False)
@app.get("/shards/{shard_id}/probe", response_model=ProbeResponse, include_in_schema=False)
@app.post("/alerts/{shard_id}/probe", response_model=ProbeResponse, include_in_schema=False)
@app.get("/alerts/{shard_id}/probe", response_model=ProbeResponse, include_in_schema=False)
async def probe_shard(
    shard_id: int,
    request: Request,
    timeout: float | None = Query(
        None,
        gt=0,
        le=120,
        description="【必填】总时限（秒）：录入+读回的总预算，到时先返回。"
        "也可改用 JSON body 的 timeout 字段。",
    ),
    message: str | None = Query(None, max_length=500, description="可选探测备注"),
    body: ProbeRequestBody | None = Body(
        None, description="可选：也可用 JSON body 传 timeout / message"
    ),
) -> ProbeResponse:
    return await _execute_probe(shard_id, request, timeout, message, body)


@app.post("/probe", response_model=ProbeResponse, include_in_schema=False)
@app.get("/probe", response_model=ProbeResponse, include_in_schema=False)
async def probe_by_query(
    request: Request,
    shard_id: int | None = Query(None, ge=0, description="要点名巡检的分片编号"),
    timeout: float | None = Query(None, gt=0, le=120),
    message: str | None = Query(None, max_length=500),
    body: ProbeRequestBody | None = Body(None),
) -> ProbeResponse:
    resolved_shard = shard_id
    if resolved_shard is None and body is not None:
        resolved_shard = body.shard_id
    if resolved_shard is None:
        raise HTTPException(
            status_code=422, detail="必须通过 path/query 或 JSON body.shard_id 点名分片"
        )
    return await _execute_probe(resolved_shard, request, timeout, message, body)


@app.get(
    "/probe/jobs/{job_id}",
    response_model=ProbeResponse,
    tags=["分片巡检"],
    summary="轮询某次巡检的后台最终结果（含超时后提交的录入与清理状态）",
)
async def get_probe_job(job_id: str, request: Request) -> ProbeResponse:
    assert probes is not None
    job = probes.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"巡检任务不存在: {job_id}")
    loop = asyncio.get_running_loop()
    status_url = f"{request.base_url}probe/jobs/{job.job_id}"
    return serialize_job(
        job, (loop.time() - job.started_loop) * 1000.0, status_url
    )  # type: ignore[return-value]
