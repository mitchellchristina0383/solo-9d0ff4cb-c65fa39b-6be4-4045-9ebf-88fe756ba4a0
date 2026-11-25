"""API 端到端测试：录入、查询、超时、查询中补报。"""
from __future__ import annotations

import asyncio
import os
import tempfile

import pytest
from fastapi.testclient import TestClient

_TMP = tempfile.mkdtemp(prefix="alert-api-db-")
os.environ["ALERT_DB_DIR"] = _TMP
# 分片 2 明细读取每批 0.15s，用于可控的超时演示
os.environ["ALERT_SHARD_DELAYS"] = "0,0,0.15"

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def _append(client, shard, alert_id, ts, **kw):
    payload = {"id": alert_id, "timestamp": ts, "level": "info",
               "source": "t", "message": "m", **kw}
    r = client.post(f"/alerts/{shard}", json=payload)
    assert r.status_code == 200, r.text


def test_health_and_append_duplicate_conflict(client):
    assert client.get("/health").json()["shards"] == 3
    _append(client, 0, "api-1", 10.0)
    r = client.post("/alerts/0", json={
        "id": "api-1", "timestamp": 11.0, "level": "info", "source": "t", "message": "dup"})
    assert r.status_code == 409


def test_latest_query_merges_and_completes(client):
    for i in range(25):
        _append(client, i % 3, f"api-seed-{i:02d}", 100.0 + i)

    r = client.get("/alerts/latest", params={"n": 10, "timeout": 5})
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 10
    assert data["complete"] is True
    assert data["status"] in ("complete", "exhausted")
    assert data["batch_size"] == 20
    keys = [(a["timestamp"], a["id"]) for a in data["alerts"]]
    assert keys == sorted(keys, reverse=True)
    # 最新一条
    assert data["alerts"][0]["id"] == "api-seed-24"
    # 分片信息齐全，head_key 均为 [ts, id] 二元组
    for stat in data["shards"]:
        assert stat["head_key"] is None or len(stat["head_key"]) == 2


def test_timeout_returns_prefix_with_blocker(client):
    # 向分片 2 写入一条全库最新告警，使它成为合并属主；
    # 其明细批次延迟 0.15s，给 0.05s 时限必然超时且一条都确认不了
    _append(client, 2, "api-timeout-latest", 9999.0)
    r = client.get("/alerts/latest", params={"n": 50, "timeout": 0.05})
    data = r.json()
    assert r.status_code == 200
    assert data["timed_out"] is True
    assert data["status"] == "timeout"
    assert data["complete"] is False
    assert data["blockers"], "超时必须指出阻塞分片"
    assert any(b["shard"] == 2 and b["phase"] == "batch" for b in data["blockers"])


def test_late_append_during_query_not_visible(client):
    # 先往慢分片（2，每批 0.15s）灌 100 条，制造一个持续多批的慢查询窗口
    for i in range(100):
        _append(client, 2, f"api-bulk-{i:03d}", 5000.0 + i)

    import threading

    box: dict = {}

    def run_query():
        box["resp"] = client.get("/alerts/latest", params={"n": 1000, "timeout": 15})

    t = threading.Thread(target=run_query)
    t.start()
    # 等待查询进入服务端处理（head + 第一批明细的窗口）
    t.join(0.25)
    assert t.is_alive(), "慢查询此时应当仍在进行"

    late = client.post("/alerts/1/auto", params={"message": "late-during-query"})
    assert late.status_code == 200
    late_id = late.json()["id"]

    t.join(timeout=20)
    assert not t.is_alive()
    data = box["resp"].json()
    assert all(a["id"] != late_id for a in data["alerts"]), "查询中的补报泄漏进了快照"

    # 新查询必须能看到补报
    r = client.get("/alerts/latest", params={"n": 1000, "timeout": 15})
    assert any(a["id"] == late_id for a in r.json()["alerts"])
