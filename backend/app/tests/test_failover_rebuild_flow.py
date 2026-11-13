"""failover_offset_rebuild 端到端闭环测试，全程本地临时 SQLite。"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def db_env(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    import app.config as config
    import app.db as db

    importlib.reload(config)
    monkeypatch.setattr(db, "DB_PATH", data_dir / "app.db")
    monkeypatch.setattr(db, "DATA_DIR", data_dir)

    from app import seed
    from app.repositories import cursors as cursor_repo
    from app.repositories import sink as sink_repo
    from app.services import apply_service, failover_rebuild_service as svc

    seed.init_db()
    return {
        "cursor_repo": cursor_repo,
        "sink_repo": sink_repo,
        "apply_service": apply_service,
        "svc": svc,
    }


def _stream_cursor(stream_id: int) -> int:
    from app.db import connect
    from app.repositories import streams as streams_repo

    conn = connect()
    try:
        return int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])
    finally:
        conn.close()


def test_rebuild_success_after_corrupt_cursor(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    # 1. 正常跑一轮：Orders CDC seq 1..5 全部应用，游标到 5
    run = apply_service.run_apply(1)
    assert run["status"] == "succeeded"
    assert _stream_cursor(1) == 5

    # 2. 故障注入：游标被改坏（清零瞎跑的典型反面场景）
    svc.demo_tamper_cursor(1, 0)
    assert _stream_cursor(1) == 0

    # 3. 发起重建：只扫证据，不动游标
    task = svc.start_rebuild(1)
    assert task["status"] == "awaiting_confirmation"
    assert task["adoptable"] is True
    assert task["candidate_cursor"] == 5
    assert _stream_cursor(1) == 0  # 未确认前游标保持坏值
    assert task["scan_counts"]["applied_seqs_rows"] == 5
    assert task["scan_counts"]["sink_records_rows"] >= 1
    assert any("候选位点" in b for b in task["basis"])

    # 4. 人工确认后才切换，且留审计
    decided = svc.confirm_rebuild(task["id"])
    assert decided["status"] == "confirmed"
    assert decided["cursor_before"] == 0
    assert decided["cursor_after"] == 5
    assert _stream_cursor(1) == 5
    events = [e["event"] for e in decided["audit"]["events"]]
    assert "scan_completed" in events and "cursor_switched" in events


def test_rebuild_rejected_on_sink_ledger_conflict(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    assert _stream_cursor(1) == 5
    svc.demo_tamper_cursor(1, 99)

    # 造矛盾：Sink 插入账本中不存在的 last_seq=99
    svc.demo_inject_sink_contradiction(1, 99)

    task = svc.start_rebuild(1)
    # 必须拒绝切换，且不允许标成成功
    assert task["status"] == "rejected"
    assert task["adoptable"] is False
    assert any("G2" in r for r in task["reasons"])
    assert _stream_cursor(1) == 99  # 坏游标原封不动

    # 被拒任务不允许再确认
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])

    # 清理矛盾数据后重新发起，应能成功恢复到 5
    svc.demo_clear_injected(1)
    task2 = svc.start_rebuild(1)
    assert task2["adoptable"] is True
    decided = svc.confirm_rebuild(task2["id"])
    assert decided["status"] == "confirmed"
    assert decided["cursor_after"] == 5
    assert _stream_cursor(1) == 5


def test_manual_reject_keeps_cursor_untouched(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(2)  # Inventory seq 1..3
    svc.demo_tamper_cursor(2, 0)

    task = svc.start_rebuild(2)
    assert task["candidate_cursor"] == 3
    rejected = svc.reject_rebuild(task["id"], reason="人工暂不切换")
    assert rejected["status"] == "rejected_manual"
    assert rejected["cursor_after"] == 0
    assert _stream_cursor(2) == 0

    # 终态任务不能重复决策
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])


def test_stale_confirmation_blocked_when_evidence_changes(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    task = svc.start_rebuild(1)
    assert task["adoptable"] is True

    # 挂起期间证据被污染，确认时必须重新复核并拦下
    svc.demo_inject_sink_contradiction(1, 99)
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])
    assert _stream_cursor(1) == 0


def test_task_persistence_and_listing(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    created = svc.start_rebuild(1)
    svc.confirm_rebuild(created["id"])

    # 重新开连接读取，验证持久化
    tasks = svc.list_tasks(1)
    assert len(tasks) == 1
    stored = tasks[0]
    assert stored["id"] == created["id"]
    assert stored["status"] == "confirmed"
    assert stored["evidence"]["applied_seqs"] == [1, 2, 3, 4, 5]
    assert svc.get_task(created["id"])["candidate_cursor"] == 5
