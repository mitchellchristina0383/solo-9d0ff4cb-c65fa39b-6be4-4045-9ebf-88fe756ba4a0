"""运行配置：全部可通过环境变量覆盖。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    shard_count: int = 3
    db_dir: str = os.environ.get("ALERT_DB_DIR", "/data")
    # 每个分片“明细读取”人为增加的延迟（秒），逗号分隔；样例里用于制造慢分片
    shard_delays: tuple[float, ...] = tuple(
        float(x) for x in os.environ.get("ALERT_SHARD_DELAYS", "0,0,0").split(",") if x.strip()
    )
    batch_size: int = int(os.environ.get("ALERT_BATCH_SIZE", "20"))
    # 查询时限的兜底默认值（秒），请求未带 timeout 时使用
    default_timeout: float = _env_float("ALERT_DEFAULT_TIMEOUT", 5.0)


settings = Settings()


def shard_delay(shard_id: int) -> float:
    delays = settings.shard_delays
    if shard_id < len(delays):
        return delays[shard_id]
    return 0.0
