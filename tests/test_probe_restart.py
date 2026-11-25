"""重启清理的完整 lifespan 路径验证。

单独成文件：pytest 按文件名排序时本模块在 test_probe 之后，
上一个模块的 module-scoped TestClient 已退出 lifespan，不会出现两个
活动 lifespan 争抢全局 registry/probes。
"""
from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

_TMP = tempfile.mkdtemp(prefix="alert-probe-restart-")
os.environ.setdefault("ALERT_DB_DIR", _TMP)
os.environ.setdefault("ALERT_SHARD_DELAYS", "0,0,0")

from app.config import settings  # noqa: E402
from app.main import app  # noqa: E402
from app.shard import Shard  # noqa: E402


def _wipe_all() -> None:
    for i in range(settings.shard_count):
        shard = Shard(i)
        shard._write_conn.execute("DELETE FROM alerts")
        shard._write_conn.commit()
        shard.close()


@pytest.fixture(autouse=True)
def _clean():
    _wipe_all()
    yield
    _wipe_all()


def test_lifespan_restart_sweeps_committed_probe_rows():
    # 模拟“探测行已提交、清理前进程被中断”：三个分片各留一条探测行
    for sid in range(settings.shard_count):
        shard = Shard(sid)
        shard.append(
            f"orphan-{sid}", 1.0 + sid, "critical", "shard-probe",
            "left by killed process", is_probe=True,
        )
        shard.append(f"formal-{sid}", 100.0 + sid, "info", "t", "")
        shard.close()

    # “重启”：重新进入 lifespan，启动阶段必须清扫
    with TestClient(app) as client:
        health = client.get("/health").json()
        cleaned = health["probe_startup_cleanup"]
        # JSON 里整型键变成字符串；两种形态都兼容
        for sid in range(settings.shard_count):
            assert cleaned.get(str(sid), cleaned.get(sid)) == 1

        # 未知任务 404
        assert client.get("/probe/jobs/does-not-exist").status_code == 404

        # 正式查询完全正常，且只剩正式行
        data = client.get("/alerts/latest", params={"n": 50, "timeout": 5}).json()
        ids = [a["id"] for a in data["alerts"]]
        assert all(not i.startswith("orphan-") for i in ids)
        assert sorted(ids) == [f"formal-{i}" for i in range(settings.shard_count)]

    for sid in range(settings.shard_count):
        shard = Shard(sid)
        try:
            assert shard.open_snapshot().count_visible() == 1
        finally:
            shard.close()
