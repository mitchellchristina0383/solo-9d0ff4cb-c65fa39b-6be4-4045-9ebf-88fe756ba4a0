"""分片访问层：每个分片一份只追加的 SQLite 库。

查询语义：
- 打开快照时在独立连接上显式 BEGIN 并立即读一次，把 WAL 快照钉在查询开始
  时刻，整个查询复用同一连接；查询过程中其它连接新提交（补报）的告警不可见。
- 先 head() 取本分片最新排序键（上界），再 read_batch() 取明细，
  每批最多 batch_size（20）条，按 (timestamp, id) 倒序。
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Any

from .config import shard_delay, settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    timestamp REAL NOT NULL,
    level TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_ts_id ON alerts(timestamp DESC, id DESC);
"""


def db_path(shard_id: int) -> str:
    return os.path.join(settings.db_dir, f"shard_{shard_id}.db")


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL：允许写入进行时其它连接开快照读
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


class Shard:
    """一个分片的句柄；写入走长连接 + 异步锁（见 main 中的锁）。"""

    def __init__(self, shard_id: int) -> None:
        self.shard_id = shard_id
        self.path = db_path(shard_id)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._write_conn = _connect(self.path)
        self._write_conn.executescript(SCHEMA)
        self._write_conn.commit()

    def append(self, alert_id: str, timestamp: float, level: str, source: str, message: str) -> None:
        # 只追加：INSERT 冲突直接抛错，不做更新
        self._write_conn.execute(
            "INSERT INTO alerts (id, timestamp, level, source, message) VALUES (?, ?, ?, ?, ?)",
            (alert_id, timestamp, level, source, message),
        )
        self._write_conn.commit()

    def open_snapshot(self) -> "ShardSnapshot":
        return ShardSnapshot(self.shard_id, self.path)

    def close(self) -> None:
        self._write_conn.close()


@dataclass
class Batch:
    rows: list[dict[str, Any]]
    more: bool


class ShardSnapshot:
    """单次查询内的只读快照；查询结束必须 close()。"""

    def __init__(self, shard_id: int, path: str) -> None:
        self.shard_id = shard_id
        self._conn = _connect(path)
        # 显式开启事务并立刻发起一次读，把 WAL 读快照钉在当前时刻：
        # Python sqlite3 遗留隔离模式下普通 SELECT 处于 autocommit、不会固定快照，
        # 必须先 BEGIN 再读。此后整个查询复用该连接，
        # 查询过程中其它连接新提交的补报对本快照不可见。
        self._conn.execute("BEGIN")
        self._conn.execute("SELECT 1 FROM alerts").fetchall()
        self.batches_read = 0
        # 人工延迟放在异步层（merge 引擎 await），保证超时可即时取消，
        # 不占用线程、也不会在连接关闭后仍触碰连接
        self.delay_seconds = shard_delay(shard_id)
        self._closed = False

    def head(self) -> tuple[float, str] | None:
        """本分片当前可见的最新排序键；空库返回 None。"""
        row = self._conn.execute(
            "SELECT timestamp, id FROM alerts ORDER BY timestamp DESC, id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return float(row["timestamp"]), str(row["id"])

    def count_visible(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) AS c FROM alerts").fetchone()["c"])

    def read_batch(
        self, cursor: tuple[float, str] | None
    ) -> Batch:
        """读取一批明细（最多 20 条），按 (timestamp, id) 倒序。

        cursor 为上一批最后一条的排序键；None 表示从头读。
        人为延迟不在这里 sleep，而由 merge 引擎按 delay_seconds 异步等待，
        以便查询时限到点时立即取消（head() 不延迟）。
        """
        if cursor is None:
            sql = (
                "SELECT id, timestamp, level, source, message FROM alerts "
                "ORDER BY timestamp DESC, id DESC LIMIT ?"
            )
            params: tuple[Any, ...] = (settings.batch_size,)
        else:
            # 行值比较：严格排在 cursor 之前的下一批
            sql = (
                "SELECT id, timestamp, level, source, message FROM alerts "
                "WHERE (timestamp, id) < (?, ?) "
                "ORDER BY timestamp DESC, id DESC LIMIT ?"
            )
            params = (cursor[0], cursor[1], settings.batch_size)

        raw = self._conn.execute(sql, params).fetchall()
        self.batches_read += 1
        rows = [dict(r) for r in raw]
        more = len(rows) == settings.batch_size
        return Batch(rows=rows, more=more)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._conn.rollback()  # 结束只读事务
        self._conn.close()
