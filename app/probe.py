"""分片巡检（probe）后台任务管理。

完整语义（对应值班员巡检需求）：

1. **走原录入链路**：``ProbeManager`` 构造一个普通 ``AppendRequest``，调用
   ``ShardRegistry.append(..., is_probe=True)`` —— 与 ``POST /alerts/{id}``
   完全相同的分片异步写锁、线程池卸载和 ``Shard.append`` INSERT，只是行上带
   ``is_probe=1`` 标记、id 使用 ``probe-`` 前缀。
2. **新快照 + 分批读回**：录入提交后 *新开* ``ShardSnapshot``，调用正式查询
   同款 ``read_batch``（行值游标、每批 20 条），逐批寻找同一个 probe id；
   只有找到该行才判定 ``passed``。
3. **总时限到时先返回**：HTTP 处理只在总时限内等待“结论事件”。到时立即返回
   ``status=timeout``，并用 ``stage`` 指明卡在 ``append``（录入）还是
   ``readback``（读回）。
4. **超时不取消后台任务**：HTTP 等待超时只会让请求先返回，后台 ``_run`` 任务
   继续。录入即使被 SQLite/异步写锁阻塞、在超时响应之后才提交，任务仍会在
   ``finally`` 中删除该探测行。可用 ``GET /probe/jobs/{job_id}`` 轮询最终结果。
5. **重启清理**：``cleanup_on_startup`` 在 lifespan 启动阶段删除所有
   ``is_probe=1`` 的行，兜住进程被 kill -9 / 崩溃 / 停机时留下的探测行。
6. **正式查询不可见**：``ShardSnapshot`` 的 head / count_visible / read_batch
   默认全部带 ``WHERE is_probe = 0``，可见条数、最新排序键、明细都排除探测行。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from .models import AppendRequest
from .shard import PROBE_ID_PREFIX

logger = logging.getLogger("app.probe")

STAGE_APPEND = "append"
STAGE_READBACK = "readback"
STAGE_CLEANUP = "cleanup"
STAGE_DONE = "done"

STATUS_PASSED = "passed"
STATUS_FAILED = "failed"
STATUS_TIMEOUT = "timeout"
STATUS_RUNNING = "running"

_STAGE_LABELS = {
    STAGE_APPEND: "录入",
    STAGE_READBACK: "读回",
}


@dataclass
class ProbeJob:
    job_id: str
    shard_id: int
    probe_id: str
    timeout: float
    message: str
    started_loop: float
    deadline_loop: float
    started_wall: float = field(default_factory=time.time)

    # 当前机器阶段：append / readback / cleanup / done
    phase: str = STAGE_APPEND
    # 结论一旦确定：passed / failed / timeout
    final_status: str | None = None
    # 失败或超时所在环节：append / readback
    failed_stage: str | None = None
    caller_timed_out: bool = False

    outcome_ready: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None

    # 录入阶段明细
    append_ok: bool | None = None
    append_elapsed_ms: float | None = None
    append_error: str | None = None

    # 读回阶段明细
    readback_ok: bool = False
    readback_batches: int = 0
    readback_elapsed_ms: float | None = None
    readback_error: str | None = None
    readback_found: dict[str, Any] | None = None

    # 清理明细
    cleanup_done: bool = False
    cleanup_deleted: int | None = None
    cleanup_ok: bool | None = None
    cleanup_error: str | None = None

    def current_blocking_stage(self) -> str:
        """还没有结论时，依据当前 phase 判断卡点。"""
        if self.phase == STAGE_READBACK:
            return STAGE_READBACK
        return STAGE_APPEND


class ProbeManager:
    """持有内存任务表；单进程部署（与单 uvicorn worker 对应）。"""

    def __init__(self, registry: Any) -> None:
        self.registry = registry
        self.jobs: dict[str, ProbeJob] = {}
        # 最近一次启动清理结果 {shard_id: deleted_rows}，供 /health 与核对
        self.startup_cleanup: dict[int, int] = {}

    # ---------------- 启动 / 关闭 ----------------

    async def cleanup_on_startup(self) -> dict[int, int]:
        """重启兜底：清掉上一进程中断留下的全部探测行。"""
        loop = asyncio.get_running_loop()
        result: dict[int, int] = {}
        for sid, shard in enumerate(self.registry.shards):
            try:
                async with self.registry.write_lock(sid):
                    deleted = await loop.run_in_executor(None, shard.delete_all_probes)
                result[sid] = int(deleted)
            except Exception as exc:  # 启动清理失败不应阻止服务启动
                logger.exception("startup probe cleanup failed on shard %s: %r", sid, exc)
                result[sid] = -1
        self.startup_cleanup = result
        if any(v > 0 for v in result.values()):
            logger.warning("startup removed leftover probe rows: %s", result)
        return result

    async def shutdown(self) -> None:
        """停机时尽量收尾；被硬杀的残留由下次启动清理兜底。"""
        tasks = [
            job.task
            for job in self.jobs.values()
            if job.task is not None and not job.task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

    # ---------------- 查询 / 发起 ----------------

    def get(self, job_id: str) -> ProbeJob | None:
        return self.jobs.get(job_id)

    async def start_and_wait(
        self, shard_id: int, timeout: float, message: str
    ) -> ProbeJob:
        """启动后台巡检，并在总时限内等待结论；超时不取消后台任务。"""
        loop = asyncio.get_running_loop()
        now = loop.time()
        job = ProbeJob(
            job_id=uuid.uuid4().hex,
            shard_id=shard_id,
            probe_id=f"{PROBE_ID_PREFIX}{uuid.uuid4().hex}",
            timeout=timeout,
            message=message or f"shard {shard_id} routine probe",
            started_loop=now,
            deadline_loop=now + timeout,
        )
        self.jobs[job.job_id] = job
        job.task = asyncio.create_task(self._run(job), name=f"probe-{job.job_id[:8]}")

        try:
            await asyncio.wait_for(job.outcome_ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # 关键：只结束本次 HTTP 等待，绝不取消 job.task。
            job.caller_timed_out = True

        # 结论已在时限内产生时，用剩余预算顺带等清理完成，
        # 让同步响应通常能直接带上 cleanup.deleted_rows=1。
        if job.final_status is not None and not job.finished.is_set():
            remaining = job.deadline_loop - loop.time()
            if remaining > 0:
                try:
                    await asyncio.wait_for(job.finished.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass
        return job

    # ---------------- 后台状态机 ----------------

    async def _run(self, job: ProbeJob) -> None:
        loop = asyncio.get_running_loop()
        snapshot = None
        try:
            # ===== 1) 录入：完全复用正式录入链路 =====
            job.phase = STAGE_APPEND
            t0 = loop.time()
            req = AppendRequest(
                id=job.probe_id,
                timestamp=time.time(),
                level="critical",
                source="shard-probe",
                message=job.message,
            )
            try:
                await self.registry.append(job.shard_id, req, is_probe=True)
                job.append_ok = True
                logger.info("probe %s appended to shard %s", job.probe_id, job.shard_id)
            except Exception as exc:  # 录入明确失败（不是超时）
                job.append_ok = False
                job.append_error = f"{type(exc).__name__}: {exc}"
                job.final_status = STATUS_FAILED
                job.failed_stage = STAGE_APPEND
                job.outcome_ready.set()
                return
            finally:
                job.append_elapsed_ms = (loop.time() - t0) * 1000.0

            # 录入把总时限耗尽（典型：写锁阻塞，探测行在超时响应后才提交）。
            # 调用方已拿到/即将拿到 append 超时；这里不再读回，直接进清理，
            # 但仍走 finally 保证“超时后才提交也会被删掉”。
            if loop.time() >= job.deadline_loop:
                job.final_status = STATUS_TIMEOUT
                job.failed_stage = STAGE_APPEND
                job.outcome_ready.set()
                return

            # ===== 2) 读回：新开快照，沿用分批 read_batch 逻辑 =====
            job.phase = STAGE_READBACK
            t0 = loop.time()
            try:
                snapshot = self.registry.shards[job.shard_id].open_snapshot()
                cursor: tuple[float, str] | None = None

                while True:
                    if loop.time() >= job.deadline_loop:
                        job.final_status = STATUS_TIMEOUT
                        job.failed_stage = STAGE_READBACK
                        job.readback_error = "到达总时限，尚未分批读回探测行"
                        break

                    # 与 merge 引擎一致：人为批次延迟在 asyncio 层等待，
                    # 时限一到立即打断睡眠（此时没有在线程中跑 SQLite，
                    # 不存在跨线程关闭连接的竞争）。
                    # 注意 wait_for 的 timeout 必须是“剩余总预算”而不是
                    # min(delay, remaining)：当预算充足时后者恰好等于 delay，
                    # wait_for 与 sleep 同时到期会产生取消竞争，把成功误判超时。
                    delay = float(getattr(snapshot, "delay_seconds", 0.0) or 0.0)
                    if delay > 0.0:
                        remaining = job.deadline_loop - loop.time()
                        if remaining <= 0:
                            job.final_status = STATUS_TIMEOUT
                            job.failed_stage = STAGE_READBACK
                            job.readback_error = "到达总时限，尚未读取下一批明细"
                            break
                        try:
                            await asyncio.wait_for(
                                asyncio.sleep(delay), timeout=remaining
                            )
                        except asyncio.TimeoutError:
                            job.final_status = STATUS_TIMEOUT
                            job.failed_stage = STAGE_READBACK
                            job.readback_error = (
                                f"到达总时限，第 {job.readback_batches + 1} 批明细仍在延迟/读取"
                            )
                            break

                    if loop.time() >= job.deadline_loop:
                        job.final_status = STATUS_TIMEOUT
                        job.failed_stage = STAGE_READBACK
                        job.readback_error = "到达总时限，尚未分批读回探测行"
                        break

                    # 真正的本地索引查询很快，不再套可能遗弃线程的 wait_for；
                    # include_probes=True 让本探测快照能读到自己的探测行。
                    batch = await loop.run_in_executor(
                        None,
                        lambda c=cursor: snapshot.read_batch(c, include_probes=True),
                    )
                    job.readback_batches += 1

                    found = next(
                        (row for row in batch.rows if row["id"] == job.probe_id), None
                    )
                    if found is not None:
                        job.readback_ok = True
                        job.readback_found = dict(found)
                        job.final_status = STATUS_PASSED
                        job.failed_stage = None
                        break

                    if not batch.more:
                        job.final_status = STATUS_FAILED
                        job.failed_stage = STAGE_READBACK
                        job.readback_error = "已按每批 20 条读完整个分片，仍未读回探测行"
                        break

                    last = batch.rows[-1]
                    cursor = (float(last["timestamp"]), str(last["id"]))

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                job.final_status = STATUS_FAILED
                job.failed_stage = STAGE_READBACK
                job.readback_error = f"{type(exc).__name__}: {exc}"
            finally:
                job.readback_elapsed_ms = (loop.time() - t0) * 1000.0
                if snapshot is not None:
                    snapshot.close()
                job.outcome_ready.set()

        except asyncio.CancelledError:
            # 停机取消：交给 finally 做最大努力清理；若没删掉，启动清理兜底。
            if job.final_status is None:
                job.phase = STAGE_CLEANUP
            raise
        finally:
            try:
                await self._cleanup(job)
            finally:
                job.phase = STAGE_DONE
                job.finished.set()

    async def _cleanup(self, job: ProbeJob) -> None:
        """删除探测行；与录入共用分片写锁，带少量重试。"""
        job.phase = STAGE_CLEANUP
        loop = asyncio.get_running_loop()
        try:
            for attempt in range(3):
                try:
                    deleted = await self.registry.delete_probe(
                        job.shard_id, job.probe_id
                    )
                    job.cleanup_deleted = int(deleted)
                    job.cleanup_ok = True
                    if job.append_ok and deleted == 0:
                        # 提交过却删 0 行不正常，记录但不影响主结论
                        job.cleanup_error = "DELETE 未命中任何行"
                    return
                except Exception as exc:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(0.15 * (attempt + 1))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job.cleanup_ok = False
            job.cleanup_error = f"{type(exc).__name__}: {exc}"
            logger.exception(
                "probe cleanup failed for %s on shard %s: %r",
                job.probe_id, job.shard_id, exc,
            )
        finally:
            job.cleanup_done = True


def serialize_job(job: ProbeJob, elapsed_ms: float, status_url: str) -> dict[str, Any]:
    """把任务状态转成 ProbeResponse 兼容 dict（POST 与 GET 共用）。"""
    in_progress = job.final_status is None
    if job.final_status == STATUS_PASSED:
        status = STATUS_PASSED
        passed = True
        stage = None
        detail = "录入成功，且已通过新快照分批读回同一条探测告警；探测行随后清理。"
    elif job.final_status in (STATUS_FAILED, STATUS_TIMEOUT):
        status = job.final_status
        passed = False
        stage = job.failed_stage or job.current_blocking_stage()
        if status == STATUS_TIMEOUT:
            detail = (
                f"总时限 {job.timeout:g}s 到时，巡检仍卡在【{_STAGE_LABELS.get(stage, stage)}】"
                f"阶段（{stage}）；请求先返回，后台仍会清理探测行。"
            )
        else:
            detail = f"巡检失败于【{_STAGE_LABELS.get(stage, stage)}】阶段（{stage}）。"
    else:
        # 结论未出：HTTP 总时限已到时返回 timeout；纯后台轮询时可能是 running
        passed = False
        stage = job.current_blocking_stage()
        status = STATUS_TIMEOUT if job.caller_timed_out else STATUS_RUNNING
        if job.caller_timed_out:
            detail = (
                f"总时限 {job.timeout:g}s 到时，巡检仍卡在【{_STAGE_LABELS.get(stage, stage)}】"
                f"阶段（{stage}）；请求先返回，后台继续等待并负责清理。"
            )
        else:
            detail = f"巡检进行中，当前处于【{_STAGE_LABELS.get(stage, stage)}】阶段（{stage}）。"

    if job.cleanup_done:
        cleanup = {
            "done": True,
            "deleted_rows": job.cleanup_deleted,
            "error": job.cleanup_error,
        }
    else:
        cleanup = {"done": False, "deleted_rows": None, "error": job.cleanup_error}

    return {
        "job_id": job.job_id,
        "shard": job.shard_id,
        "shard_id": job.shard_id,
        "probe_id": job.probe_id,
        "alert_id": job.probe_id,
        "status": status,
        "passed": passed,
        "ok": passed,
        "success": passed,
        "stage": stage if not passed else None,
        "stage_label": _STAGE_LABELS.get(stage) if stage and not passed else None,
        "phase": job.phase,
        "in_progress": in_progress or not job.finished.is_set(),
        "finished": job.finished.is_set(),
        "caller_timed_out": job.caller_timed_out,
        "timeout_seconds": job.timeout,
        "elapsed_ms": round(elapsed_ms, 2),
        "detail": detail,
        "append": {
            "ok": job.append_ok,
            "elapsed_ms": round(job.append_elapsed_ms, 2)
            if job.append_elapsed_ms is not None
            else None,
            "error": job.append_error,
            "committed": bool(job.append_ok),
        },
        "readback": {
            "ok": job.readback_ok,
            "elapsed_ms": round(job.readback_elapsed_ms, 2)
            if job.readback_elapsed_ms is not None
            else None,
            "batches_read": job.readback_batches,
            "found_id": job.probe_id if job.readback_ok else None,
            "error": job.readback_error,
        },
        "cleanup": cleanup,
        "status_url": status_url,
    }
