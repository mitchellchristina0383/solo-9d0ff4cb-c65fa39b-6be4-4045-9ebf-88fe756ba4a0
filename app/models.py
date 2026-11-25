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
