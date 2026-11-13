"""failover_offset_rebuild 的持久化层：证据扫描与任务记录。"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from app.modules.failover_offset_rebuild import Evidence


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def scan_evidence(conn: sqlite3.Connection, stream_id: int) -> Evidence:
    """扫描该流的全部证据：已应用账本、Sink、隔离区、变更账本。"""
    applied_rows = conn.execute(
        "SELECT seq FROM applied_seqs WHERE stream_id=? ORDER BY seq",
        (stream_id,),
    ).fetchall()
    sink_rows = conn.execute(
        "SELECT last_seq FROM sink_records WHERE stream_id=? ORDER BY last_seq",
        (stream_id,),
    ).fetchall()
    quarantine_rows = conn.execute(
        "SELECT seq FROM quarantine WHERE stream_id=? ORDER BY seq",
        (stream_id,),
    ).fetchall()
    ledger_rows = conn.execute(
        """
        SELECT seq FROM change_ledger
        WHERE stream_id=? AND disposition='applied' ORDER BY seq
        """,
        (stream_id,),
    ).fetchall()

    applied_seqs = [int(r["seq"]) for r in applied_rows]
    sink_last_seqs = [int(r["last_seq"]) for r in sink_rows]
    quarantine_seqs = [int(r["seq"]) for r in quarantine_rows]
    ledger_applied_seqs = [int(r["seq"]) for r in ledger_rows]

    counts = {
        "applied_seqs_rows": len(applied_seqs),
        "sink_records_rows": len(sink_last_seqs),
        "quarantine_rows": len(quarantine_seqs),
        "change_ledger_applied_rows": len(ledger_applied_seqs),
        "change_ledger_total_rows": int(
            conn.execute(
                "SELECT COUNT(*) c FROM change_ledger WHERE stream_id=?",
                (stream_id,),
            ).fetchone()["c"]
        ),
    }
    return Evidence(
        stream_id=stream_id,
        applied_seqs=applied_seqs,
        sink_last_seqs=sink_last_seqs,
        quarantine_seqs=quarantine_seqs,
        ledger_applied_seqs=ledger_applied_seqs,
        scan_counts=counts,
    )


def create_task(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    status: str,
    gate: str,
    adoptable: bool,
    candidate: int,
    cursor_before: int,
    scan_counts: dict[str, int],
    evidence: dict[str, Any],
    basis: list[str],
    reasons: list[str],
    audit: dict[str, Any],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO failover_rebuild_tasks(
            stream_id, status, gate, adoptable, candidate_cursor, cursor_before, cursor_after,
            scan_counts_json, evidence_json, basis_json, reasons_json, audit_json,
            created_at, decided_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            status,
            gate,
            1 if adoptable else 0,
            candidate,
            cursor_before,
            cursor_before,
            json.dumps(scan_counts, ensure_ascii=False),
            json.dumps(evidence, ensure_ascii=False),
            json.dumps(basis, ensure_ascii=False),
            json.dumps(reasons, ensure_ascii=False),
            json.dumps(audit, ensure_ascii=False),
            _now(),
            None,
        ),
    )
    return int(cur.lastrowid)


def decide_task(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    status: str,
    cursor_after: int | None,
    audit: dict[str, Any],
) -> None:
    conn.execute(
        """
        UPDATE failover_rebuild_tasks
        SET status=?, cursor_after=?, audit_json=?, decided_at=?
        WHERE id=?
        """,
        (
            status,
            cursor_after,
            json.dumps(audit, ensure_ascii=False),
            _now(),
            task_id,
        ),
    )


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = dict(row)
    for key in ("scan_counts_json", "evidence_json", "basis_json", "reasons_json", "audit_json"):
        item[key.removesuffix("_json")] = json.loads(item.pop(key) or "[]")
    item["adoptable"] = bool(item["adoptable"])
    return item


def get_task(conn: sqlite3.Connection, task_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM failover_rebuild_tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    return _row_to_dict(row) if row else None


def list_tasks(conn: sqlite3.Connection, stream_id: int | None = None) -> list[dict[str, Any]]:
    if stream_id is None:
        rows = conn.execute(
            "SELECT * FROM failover_rebuild_tasks ORDER BY id DESC LIMIT 100"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM failover_rebuild_tasks WHERE stream_id=? ORDER BY id DESC LIMIT 100",
            (stream_id,),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]
