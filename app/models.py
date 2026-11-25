"""API 的请求/响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class AlertOut(BaseModel):
    id: str = Field(..., description="全局唯一告警编号")
    timestamp: float = Field(..., description="告警时间（Unix 秒）")
    shard: int = Field(..., description="来源分片编号（0 起）")
    level: str
    source: str
    message: str


class Blocker(BaseModel):
    shard: int = Field(..., description="挡住后续有序结果的分片")
    phase: str = Field(..., description="当时所处阶段：head 或 batch")
    reason: str
    elapsed: float = Field(..., description="查询已耗时（秒）")


class ShardStat(BaseModel):
    shard: int
    visible_rows: int = Field(..., description="本次快照可见的已提交告警数")
    head_key: list[float | str] | None = Field(
        None, description="分片最新排序键 [timestamp, id]；空库为 null"
    )
    batches_read: int = Field(..., description="本次实际读取的明细批次数")
    delay_seconds: float


class LatestResponse(BaseModel):
    alerts: list[AlertOut]
    count: int
    requested_n: int
    complete: bool = Field(..., description="true=已确认前 N 条或全库不足 N 条；false=超时截断")
    status: str = Field(..., description="complete / exhausted / timeout")
    timed_out: bool
    elapsed_ms: float
    snapshot_at: float = Field(..., description="查询开始时间，只看此前已提交的告警")
    shards: list[ShardStat]
    blockers: list[Blocker] = Field(
        default_factory=list, description="超时场景下挡住后续有序前缀的分片"
    )
    batch_size: int


class AppendRequest(BaseModel):
    id: str = Field(..., min_length=1)
    timestamp: float = Field(..., gt=0)
    level: str = "info"
    source: str = "manual"
    message: str = ""


class ProbeRequestBody(BaseModel):
    """POST /probe/{shard_id} 的可选 JSON 体；总时限也可只走 query。"""

    shard_id: int | None = Field(None, ge=0, description="仅 POST /probe 无根路径分片时使用")
    timeout: float | None = Field(None, gt=0, le=120, description="总时限（秒）")
    total_timeout: float | None = Field(None, gt=0, le=120, description="timeout 的别名")
    timeout_seconds: float | None = Field(None, gt=0, le=120, description="timeout 的别名")
    message: str | None = Field(None, max_length=500)


class ProbeStageInfo(BaseModel):
    ok: bool | None = Field(None, description="true=成功；false=失败；null=到时仍在进行")
    elapsed_ms: float | None = None
    error: str | None = None


class ProbeAppendInfo(ProbeStageInfo):
    committed: bool = Field(False, description="探测行是否已确认提交")


class ProbeReadbackInfo(BaseModel):
    ok: bool = False
    elapsed_ms: float | None = None
    batches_read: int = Field(0, description="沿用分批逻辑实际读了多少批")
    found_id: str | None = None
    error: str | None = None


class ProbeCleanupInfo(BaseModel):
    done: bool = Field(False, description="后台清理是否已结束")
    deleted_rows: int | None = Field(None, description="DELETE 实际删除行数（正常为 1）")
    error: str | None = None


class ProbeResponse(BaseModel):
    job_id: str
    shard: int
    shard_id: int = Field(..., description="shard 的同义字段")
    probe_id: str
    alert_id: str = Field(..., description="probe_id 的同义字段")
    status: str = Field(
        ..., description="passed=已读回；failed=链路明确失败；timeout=总时限到时；running=后台仍在跑"
    )
    passed: bool = Field(..., description="只有分批读回同一条探测行才为 true")
    ok: bool = Field(..., description="passed 的同义字段")
    success: bool = Field(..., description="passed 的同义字段")
    stage: str | None = Field(
        None, description="卡住或失败的环节：append=录入，readback=读回；通过时为 null"
    )
    stage_label: str | None = Field(None, description="stage 的中文说明：录入 / 读回")
    phase: str = Field(..., description="任务当前阶段：append/readback/cleanup/done")
    in_progress: bool
    finished: bool = Field(..., description="后台清理是否已全部结束")
    caller_timed_out: bool = Field(False, description="本次 HTTP 等待是否先于结论到时")
    timeout_seconds: float
    elapsed_ms: float
    detail: str
    append: ProbeAppendInfo
    readback: ProbeReadbackInfo
    cleanup: ProbeCleanupInfo
    status_url: str
