from fastapi import APIRouter, HTTPException

from app.schemas.entities import (
    RebuildConfirmRequest,
    RebuildRejectRequest,
    RebuildStartRequest,
    RebuildTamperRequest,
)
from app.services import failover_rebuild_service as service

router = APIRouter(tags=["failover-rebuild"])


@router.get("/failover-rebuild/tasks")
def list_tasks(stream_id: int | None = None):
    return service.list_tasks(stream_id)


@router.get("/failover-rebuild/tasks/{task_id}")
def get_task(task_id: int):
    try:
        return service.get_task(task_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/failover-rebuild/tasks")
def start_rebuild(body: RebuildStartRequest):
    try:
        return service.start_rebuild(body.stream_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/failover-rebuild/tasks/{task_id}/confirm")
def confirm_rebuild(task_id: int, body: RebuildConfirmRequest | None = None):
    try:
        return service.confirm_rebuild(task_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/failover-rebuild/tasks/{task_id}/reject")
def reject_rebuild(task_id: int, body: RebuildRejectRequest | None = None):
    try:
        reason = body.reason if body else ""
        return service.reject_rebuild(task_id, reason)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


# ---- 演练/验收用故障注入接口（非主链） ----


@router.post("/failover-rebuild/demo/tamper-cursor")
def tamper_cursor(body: RebuildTamperRequest):
    try:
        return service.demo_tamper_cursor(body.stream_id, body.bad_cursor)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/failover-rebuild/demo/inject-sink-conflict")
def inject_sink_conflict(body: RebuildTamperRequest):
    try:
        return service.demo_inject_sink_contradiction(body.stream_id, body.bad_cursor)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/failover-rebuild/demo/clear")
def clear_demo(body: RebuildStartRequest):
    return service.demo_clear_injected(body.stream_id)
