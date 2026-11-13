"""failover_offset_rebuild 服务层：扫证据 → 候选 → 人工确认才切换。"""

from __future__ import annotations

from typing import Any

from app.db import connect
from app.modules.failover_offset_rebuild import compute_candidate
from app.repositories import failover_rebuild as rebuild_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

DEMO_ENTITY_PREFIX = "__rebuild_demo__"


def start_rebuild(stream_id: int) -> dict[str, Any]:
    """扫描证据并生成候选位点。

    本步骤只读证据、写任务记录，**绝不触碰 apply_cursor**。门禁不过时任务
    直接落 ``rejected``，不允许标记成重建成功。
    """
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        cursor_before = int(stream["apply_cursor"] or 0)

        evidence = rebuild_repo.scan_evidence(conn, stream_id)
        result = compute_candidate(evidence)

        status = "awaiting_confirmation" if result.adoptable else "rejected"
        audit = {
            "events": [
                {
                    "event": "scan_completed",
                    "gate": result.gate,
                    "candidate": result.candidate,
                    "adoptable": result.adoptable,
                }
            ],
        }
        task_id = rebuild_repo.create_task(
            conn,
            stream_id=stream_id,
            status=status,
            gate=result.gate,
            adoptable=result.adoptable,
            candidate=result.candidate,
            cursor_before=cursor_before,
            scan_counts=evidence.scan_counts,
            evidence=result.detail,
            basis=result.basis,
            reasons=result.reasons,
            audit=audit,
        )
        conn.commit()
        return rebuild_repo.get_task(conn, task_id)  # type: ignore[return-value]
    finally:
        conn.close()


def _recheck(conn, task: dict[str, Any]) -> None:
    """决策前重新扫库，防止任务挂起期间证据已变化。"""
    evidence = rebuild_repo.scan_evidence(conn, task["stream_id"])
    result = compute_candidate(evidence)
    if not result.adoptable or result.candidate != task["candidate_cursor"]:
        raise ValueError(
            "证据已变化或不再满足门禁（gate="
            f"{result.gate}），候选位点失效，请重新发起重建："
            + "; ".join(result.reasons or ["candidate changed"])
        )


def confirm_rebuild(task_id: int) -> dict[str, Any]:
    """人工确认：复核通过后才写新位点，并留审计。"""
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        if task["status"] != "awaiting_confirmation":
            raise ValueError(f"task already terminal: {task['status']}")
        if not task["adoptable"]:
            raise ValueError("candidate not adoptable")

        _recheck(conn, task)

        stream = streams_repo.get_stream(conn, task["stream_id"])
        if not stream:
            raise ValueError("stream not found")
        cursor_before = int(stream["apply_cursor"] or 0)
        new_cursor = int(task["candidate_cursor"])

        # 唯一的写游标动作，与审计在同一事务内提交
        streams_repo.set_cursor(conn, task["stream_id"], new_cursor)
        audit = task["audit"]
        audit.setdefault("events", []).append(
            {
                "event": "cursor_switched",
                "cursor_before": cursor_before,
                "cursor_after": new_cursor,
                "decided_by": "manual_confirm",
            }
        )
        rebuild_repo.decide_task(
            conn,
            task_id=task_id,
            status="confirmed",
            cursor_after=new_cursor,
            audit=audit,
        )
        conn.commit()
        return rebuild_repo.get_task(conn, task_id)  # type: ignore[return-value]
    finally:
        conn.close()


def reject_rebuild(task_id: int, reason: str = "") -> dict[str, Any]:
    """人工拒绝：不改动游标，只把任务关闭并留审计。"""
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        if task["status"] != "awaiting_confirmation":
            raise ValueError(f"task already terminal: {task['status']}")

        stream = streams_repo.get_stream(conn, task["stream_id"])
        cursor_now = int((stream or {}).get("apply_cursor") or 0)
        audit = task["audit"]
        audit.setdefault("events", []).append(
            {
                "event": "manual_rejected",
                "reason": reason or "operator rejected candidate",
                "cursor_untouched": cursor_now,
            }
        )
        rebuild_repo.decide_task(
            conn,
            task_id=task_id,
            status="rejected_manual",
            cursor_after=cursor_now,
            audit=audit,
        )
        conn.commit()
        return rebuild_repo.get_task(conn, task_id)  # type: ignore[return-value]
    finally:
        conn.close()


def list_tasks(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return rebuild_repo.list_tasks(conn, stream_id)
    finally:
        conn.close()


def get_task(task_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        return task
    finally:
        conn.close()


# ---- 仅用于验收/演练的故障注入，不是主链能力 ----

def demo_tamper_cursor(stream_id: int, bad_cursor: int) -> dict[str, Any]:
    """把游标改成错误值（模拟 apply_cursor 丢失/被改坏）。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        before = int(stream["apply_cursor"] or 0)
        streams_repo.set_cursor(conn, stream_id, int(bad_cursor))
        conn.commit()
        return {"stream_id": stream_id, "cursor_before": before, "cursor_after": int(bad_cursor)}
    finally:
        conn.close()


def demo_inject_sink_contradiction(stream_id: int, phantom_seq: int) -> dict[str, Any]:
    """造一份 Sink/账本矛盾数据：插入账本中不存在的 Sink 行（触发 G2）。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        entity_key = f"{DEMO_ENTITY_PREFIX}s{stream_id}"
        sink_repo.upsert(
            conn,
            stream_id=stream_id,
            entity_key=entity_key,
            payload={"demo": "phantom sink row without ledger evidence"},
            seq=int(phantom_seq),
            event_ts="1970-01-01T00:00:00Z",
        )
        conn.commit()
        return {
            "stream_id": stream_id,
            "entity_key": entity_key,
            "phantom_last_seq": int(phantom_seq),
        }
    finally:
        conn.close()


def demo_clear_injected(stream_id: int) -> dict[str, Any]:
    """清理演练注入的 Sink 矛盾行。"""
    conn = connect()
    try:
        cur = conn.execute(
            "DELETE FROM sink_records WHERE stream_id=? AND entity_key LIKE ?",
            (stream_id, f"{DEMO_ENTITY_PREFIX}%"),
        )
        conn.commit()
        return {"stream_id": stream_id, "deleted": cur.rowcount}
    finally:
        conn.close()
