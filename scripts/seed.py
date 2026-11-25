"""初始化三个分片的样例数据（幂等：已有数据则跳过）。

分布设计：
- 共 63 条，时间戳 1_000_000..1_000_062，按轮询落到 3 个分片；
- 每第 10 条制造一次同时间戳（timestamp 与前一条相同），
  用于验证同时间戳下按唯一编号 id 倒序决胜；
- 各分片条数不均（21/21/21 基础上再给分片 2 补 5 条最新告警），
  最新一条落在分片 2，便于观察“较旧分片明细不被读取”的早退行为。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.shard import Shard  # noqa: E402

BASE_TS = 1_000_000.0
TOTAL = 63


def main() -> None:
    shards = [Shard(i) for i in range(3)]
    snapshots = [s.open_snapshot() for s in shards]
    try:
        if any(snap.count_visible() > 0 for snap in snapshots):
            print("数据库已有数据，跳过种子写入。")
            return
    finally:
        for snap in snapshots:
            snap.close()

    rows: list[tuple[str, float, str, str, str]] = []
    for seq in range(TOTAL):
        ts = BASE_TS + seq
        if seq > 0 and seq % 10 == 0:
            ts = BASE_TS + seq - 1  # 与前一条同时间戳，靠 id 决胜
        alert_id = f"alert-{seq:04d}"
        rows.append((alert_id, ts, ["info", "warn", "error"][seq % 3],
                     f"host-{seq % 7}", f"样例告警 {seq}"))

    for seq, (alert_id, ts, level, source, message) in enumerate(rows):
        shards[seq % 3].append(alert_id, ts, level, source, message)

    # 分片 2 额外补 5 条最新告警，使其成为“最新分片”
    for extra in range(5):
        seq = TOTAL + extra
        shards[2].append(
            f"alert-extra-{extra:02d}",
            BASE_TS + TOTAL + extra,
            "warn",
            "host-latest",
            f"分片2专属最新告警 {extra}",
        )

    for i, shard in enumerate(shards):
        snap = shard.open_snapshot()
        try:
            total = snap.count_visible()
        finally:
            snap.close()
        print(f"shard {i}: {total} rows -> {shard.path}")
        shard.close()


if __name__ == "__main__":
    main()
