"""分片巡检测试：

-  happy path：原链路录入 -> 新快照分批读回 -> 清理；
-  正式查询（条数 / head_key / 明细 / API）始终排除探测行；
-  读回超时指明 readback 且后台清理；
-  异步写锁阻塞录入：超时先返回 append，释放后“超时后才提交”仍被清理；
-  真实 SQLite 跨连接写事务阻塞录入（端到端 HTTP）；
-  重启清理：启动清扫 is_probe=1 的遗留行；
-  分批 include_probes 翻页能在多批之后读回探测行。
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import time

import pytest
from fastapi.testclient import TestClient

_TMP = tempfile.mkdtemp(prefix="alert-probe-db-")
os.environ.setdefault("ALERT_DB_DIR", _TMP)
os.environ.setdefault("ALERT_SHARD_DELAYS", "0,0,0")

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402
from app.probe import ProbeManager  # noqa: E402
from app.shard import Shard, db_path  # noqa: E402


def _wipe_all() -> None:
    for i in range(settings.shard_count):
        shard = Shard(i)
        shard._write_conn.execute("DELETE FROM alerts")
        shard._write_conn.commit()
        shard.close()


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _clean_tables():
    _wipe_all()
    yield
    # 用例都自行等待后台任务结束；这里再兜底清一次，避免污染后续模块
    try:
        _wipe_all()
    except Exception:
        pass


def _wait_finished(client: TestClient, job_id: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        r = client.get(f"/probe/jobs/{job_id}")
        assert r.status_code == 200, r.text
        last = r.json()
        if last["finished"]:
            return last
        time.sleep(0.05)
    raise AssertionError(f"probe job {job_id} did not finish: {last}")


def _raw_scalar(shard_id: int, sql: str, params=()):
    conn = sqlite3.connect(db_path(shard_id))
    try:
        return conn.execute(sql, params).fetchone()[0]
    finally:
        conn.close()


def _probe_rows(shard_id: int) -> int:
    return _raw_scalar(shard_id, "SELECT COUNT(*) FROM alerts WHERE is_probe = 1")


# ---------------- Happy path ----------------


def test_probe_passes_readback_and_cleans(client):
    t0 = time.monotonic()
    r = client.post("/probe/0", params={"timeout": 5})
    wall = time.monotonic() - t0
    assert r.status_code == 200, r.text
    d = r.json()

    assert d["passed"] is True
    assert d["ok"] is True
    assert d["status"] == "passed"
    assert d["stage"] is None
    assert d["stage_label"] is None
    assert d["append"]["ok"] is True
    assert d["append"]["committed"] is True
    assert d["readback"]["ok"] is True
    assert d["readback"]["found_id"] == d["probe_id"]
    assert d["readback"]["batches_read"] >= 1
    assert wall < 5

    job = _wait_finished(client, d["job_id"])
    assert job["cleanup"]["done"] is True
    assert job["cleanup"]["deleted_rows"] == 1
    assert _probe_rows(0) == 0
    # 正式行没有被误伤
    assert _raw_scalar(0, "SELECT COUNT(*) FROM alerts WHERE is_probe = 0") == 0


def test_probe_path_aliases_and_body_timeout(client):
    # 别名路径 + JSON body 传总时限
    r = client.post("/alerts/1/probe", json={"timeout": 5})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["passed"] is True and d["success"] is True
    job = _wait_finished(client, d["job_id"])
    assert job["cleanup"]["deleted_rows"] == 1
    assert _probe_rows(1) == 0


def test_probe_get_alias_and_root_query_form(client):
    # GET 别名（兼容只愿发 GET 的调用方）
    r = client.get("/probe/0", params={"timeout": 5})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["passed"] is True
    _wait_finished(client, d["job_id"])
    assert _probe_rows(0) == 0

    # /probe?shard_id= 形态
    r = client.post("/probe", params={"shard_id": 2, "timeout": 5})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["passed"] is True, json.dumps(d, ensure_ascii=False)
    assert d["shard"] == 2 and d["passed"] is True
    _wait_finished(client, d["job_id"])
    assert _probe_rows(2) == 0

    # 无根路径 + JSON body 同时给 shard_id 与 timeout_seconds 别名
    r = client.post("/probe", json={"shard_id": 0, "timeout_seconds": 5})
    assert r.status_code == 200, r.text
    d = r.json()
    assert d["shard_id"] == 0 and d["alert_id"] == d["probe_id"] and d["success"] is True
    _wait_finished(client, d["job_id"])
    assert _probe_rows(0) == 0


def test_probe_unknown_shard_404(client):
    r = client.post("/probe/99", params={"timeout": 1})
    assert r.status_code == 404


def test_probe_requires_timeout(client):
    r = client.post("/probe/0")
    assert r.status_code == 422


# ---------------- 正式查询排除探测行 ----------------


def test_formal_snapshot_excludes_probe_in_count_head_batches():
    shard = Shard(1)
    shard.append("normal-1", 4_000_000_000.0, "info", "t", "")
    shard.append("probe-secret", 4_000_000_001.0, "critical", "probe", "", is_probe=True)
    try:
        snap = shard.open_snapshot()
        # 条数不算探测行
        assert snap.count_visible() == 1
        # 最新排序键不算探测行（否则应是 4000000001 / probe-secret）
        assert snap.head() == (4_000_000_000.0, "normal-1")
        batch = snap.read_batch(None)
        assert [r["id"] for r in batch.rows] == ["normal-1"]
        # 巡检读回使用 include_probes=True，同一套分批 SQL 能看到探测行
        batch_all = snap.read_batch(None, include_probes=True)
        assert {r["id"] for r in batch_all.rows} == {"normal-1", "probe-secret"}
        snap.close()
    finally:
        shard.close()


def test_formal_api_excludes_probe_rows(client):
    shard = Shard(1)
    shard.append("normal-api-1", 4_100_000_000.0, "info", "t", "")
    shard.append("probe-api-hidden", 4_100_000_001.0, "critical", "probe", "", is_probe=True)
    shard.close()

    r = client.get("/alerts/latest", params={"n": 1000, "timeout": 10})
    assert r.status_code == 200
    data = r.json()
    ids = [a["id"] for a in data["alerts"]]
    assert "probe-api-hidden" not in ids
    assert "normal-api-1" in ids

    stat1 = next(s for s in data["shards"] if s["shard"] == 1)
    assert stat1["visible_rows"] == 1
    assert stat1["head_key"] == [4_100_000_000.0, "normal-api-1"]


def test_probe_readback_paginates_across_batches():
    """探测行落在 25 条更新的正式行之后：include_probes 分批翻页仍能读回。"""
    shard = Shard(0)
    try:
        shard.append("probe-old", 1.0, "critical", "probe", "", is_probe=True)
        for i in range(25):
            shard.append(f"newer-{i:02d}", 1000.0 + i, "info", "t", "")

        snap = shard.open_snapshot()
        try:
            cursor = None
            seen: list[str] = []
            found = False
            for _ in range(10):  # 最多若干批，防止死循环
                batch = snap.read_batch(cursor, include_probes=True)
                seen.extend(r["id"] for r in batch.rows)
                if any(r["id"] == "probe-old" for r in batch.rows):
                    found = True
                    break
                if not batch.more:
                    break
                last = batch.rows[-1]
                cursor = (float(last["timestamp"]), str(last["id"]))
            assert found, f"分批翻页后应找到 probe-old，已读: {seen}"
            assert snap.batches_read >= 2  # 25 条新行 + 探测行，至少两批
        finally:
            snap.close()
    finally:
        shard.close()


# ---------------- 读回超时 ----------------


def test_readback_timeout_points_at_readback_and_cleans(client, monkeypatch):
    # 不依赖导入顺序：强制所有分片每批明细延迟 0.3s
    monkeypatch.setattr("app.shard.shard_delay", lambda sid: 0.3)

    t0 = time.monotonic()
    r = client.post("/probe/1", params={"timeout": 0.1})
    wall = time.monotonic() - t0
    assert r.status_code == 200, r.text
    d = r.json()

    assert wall < 2.0
    assert d["status"] == "timeout"
    assert d["passed"] is False
    assert d["stage"] == "readback"
    assert d["stage_label"] == "读回"
    assert d["append"]["ok"] is True
    assert d["in_progress"] is True or d["finished"] is True
    assert d["elapsed_ms"] >= 70  # 确实等了约一个总时限

    job = _wait_finished(client, d["job_id"], timeout=5)
    assert job["stage"] in (None, "readback")
    assert job["cleanup"]["done"] is True
    assert job["cleanup"]["deleted_rows"] == 1
    assert _probe_rows(1) == 0


# ---------------- 写锁阻塞：进程内 asyncio 写锁 ----------------


def test_asyncio_write_lock_blocks_append_until_release():
    async def scenario():
        from app.main import ShardRegistry

        reg = ShardRegistry()
        mgr = ProbeManager(reg)
        try:
            async with reg.write_lock(0):
                started = time.monotonic()
                job = await mgr.start_and_wait(0, timeout=0.2, message="lock-held")
                elapsed = time.monotonic() - started

                # 锁仍在本协程手里：总时限已到，先返回 append 超时
                assert 0.15 <= elapsed < 1.0
                assert job.caller_timed_out is True
                assert job.final_status is None
                assert job.phase == "append"
                assert job.current_blocking_stage() == "append"
                assert _probe_rows(0) == 0  # 还没提交

            # 释放锁后，后台任务完成“迟到的录入”并清理
            await asyncio.wait_for(job.finished.wait(), timeout=10)
            assert job.append_ok is True
            assert job.final_status == "timeout"
            assert job.failed_stage == "append"
            assert job.cleanup_done is True
            assert job.cleanup_deleted == 1
            assert _probe_rows(0) == 0
        finally:
            await reg.close()

    asyncio.run(scenario())


# ---------------- 写锁阻塞：真实 SQLite 跨连接写事务（端到端 HTTP） ----------------


def test_sqlite_write_transaction_blocks_probe_append_then_late_cleanup(client):
    shard_id = 2
    holder = sqlite3.connect(db_path(shard_id), timeout=5, check_same_thread=False)
    holder.isolation_level = None
    holder.execute("PRAGMA busy_timeout=5000")
    holder.execute("BEGIN IMMEDIATE")  # 拿走 WAL 唯一写锁

    def release_after(delay: float) -> None:
        time.sleep(delay)
        try:
            holder.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    timer = threading.Thread(target=release_after, args=(0.8,), daemon=True)
    timer.start()

    t0 = time.monotonic()
    r = client.post(f"/probe/{shard_id}", params={"timeout": 0.3})
    wall = time.monotonic() - t0

    assert r.status_code == 200, r.text
    d = r.json()
    assert wall < 1.5
    assert d["status"] == "timeout"
    assert d["passed"] is False
    assert d["stage"] == "append"
    assert d["stage_label"] == "录入"
    # 响应时录入仍悬而未决
    assert d["append"]["ok"] is None

    timer.join(timeout=3)
    holder.close()

    # 锁释放后：迟到提交 -> 后台清理 -> 行消失
    job = _wait_finished(client, d["job_id"], timeout=20)
    assert job["append"]["ok"] is True
    assert job["caller_timed_out"] is True
    assert job["cleanup"]["done"] is True
    assert job["cleanup"]["deleted_rows"] == 1
    assert _probe_rows(shard_id) == 0


# ---------------- 重启清理 ----------------


def test_startup_cleanup_removes_interrupted_probe_rows():
    # 模拟上一进程在探测行提交后、清理前被 kill：留下 is_probe=1 行
    shard = Shard(2)
    shard.append("orphan-probe-1", 1.0, "critical", "probe", "", is_probe=True)
    shard.append("keep-normal", 2.0, "info", "t", "")
    shard.close()

    async def scenario():
        from app.main import ShardRegistry

        reg = ShardRegistry()
        mgr = ProbeManager(reg)
        try:
            counts = await mgr.cleanup_on_startup()
            assert counts[2] == 1
        finally:
            await reg.close()

    asyncio.run(scenario())

    assert _probe_rows(2) == 0
    snap = Shard(2).open_snapshot()
    try:
        assert snap.count_visible() == 1
        assert snap.head() == (2.0, "keep-normal")
    finally:
        snap.close()
