"""流式转换产物发布与窗口入账接口。

- ``POST /api/stream_transform/batches`` 接收新订单批次并发布（与 ``/api/order_transform`` 等价）
- ``POST /api/order_transform/batches/{batch_no}/retry`` 续跑中断批次
- ``GET  /api/order_transform/orders/{source_order_id}`` 按源订单编号回查
- ``GET  /api/order_transform/windows/{window_no}`` 窗口只读已发布记录
- ``POST /api/order_transform/windows/{window_no}/settle`` 窗口入账并回写核对清单
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult
from app.services.order_transform import service

router = APIRouter(prefix="/api/order_transform", tags=["订单转换批次"])
alias_router = APIRouter(prefix="/api/stream_transform", tags=["流式转换"])


def _batch_not_found(batch_no: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"转换批次 {batch_no} 不存在")


@router.post("/batches", response_model=ActionResult)
def publish_batch(payload: dict[str, Any]) -> ActionResult:
    """接收一批新订单：先持久化原始值，再逐行校验发布，坏行隔离。"""
    summary, message, ok = service.publish(payload)
    if summary is None:
        return ActionResult(ok=False, message=message)
    summary = dict(summary)
    return ActionResult(ok=ok, message=message, entry=summary)


@alias_router.post("/batches", response_model=ActionResult)
def publish_batch_alias(payload: dict[str, Any]) -> ActionResult:
    return publish_batch(payload)


@router.get("/batches")
def list_batches(
    keyword: str | None = Query(default=None, description="批次号或窗口编号"),
    status: str | None = Query(default=None),
    window_no: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    items, total = service.list_batches(
        keyword=keyword, status=status, window_no=window_no, page=page, size=size)
    return {"items": items, "total": total, "page": page, "size": size}


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    batch = service.get_batch(batch_no)
    if batch is None:
        raise _batch_not_found(batch_no)
    return batch


@router.post("/batches/{batch_no}/retry", response_model=ActionResult)
def retry_batch(batch_no: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """落库失败后重试：从转换批找回原始值，只续跑未确认的行。"""
    summary, message, ok = service.retry(batch_no, payload or {})
    if summary is None:
        raise _batch_not_found(batch_no)
    summary = dict(summary)
    return ActionResult(ok=ok, message=message, entry=summary)


@alias_router.post("/batches/{batch_no}/retry", response_model=ActionResult)
def retry_batch_alias(batch_no: str, payload: dict[str, Any] | None = None) -> ActionResult:
    return retry_batch(batch_no, payload)


@router.post("/batches/{batch_no}/corrections", response_model=ActionResult)
def correct_batch(batch_no: str, payload: dict[str, Any]) -> ActionResult:
    summary, message, ok = service.correct_batch(batch_no, payload)
    if summary is None:
        raise _batch_not_found(batch_no)
    summary = dict(summary)
    return ActionResult(ok=ok, message=message, entry=summary)


@router.get("/batches/{batch_no}/checklist")
def get_checklist(batch_no: str) -> dict[str, Any]:
    if service.find_batch(batch_no) is None:
        raise _batch_not_found(batch_no)
    items = service.checklist(batch_no)
    return {"items": items, "total": len(items)}


@router.post("/batches/{batch_no}/records/{source_order_id}", response_model=ActionResult)
def correct_record(batch_no: str, source_order_id: str, payload: dict[str, Any]) -> ActionResult:
    """修正导入后隔离的原始字段；修正值单独保留，原始值不覆盖。"""
    view, message, ok = service.correct_record(batch_no, source_order_id, payload)
    if view is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=ok, message=message, entry=view)


@router.get("/orders/{source_order_id}")
def get_order(source_order_id: str) -> dict[str, Any]:
    """按源订单编号回查：刷新或重新进入窗口后仍返回已提交批次中的同一条记录。"""
    entry = service.get_order(source_order_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"源订单 {source_order_id} 不在任何转换批次中")
    return entry


@router.get("/windows")
def list_windows() -> dict[str, Any]:
    items = service.window_list()
    return {"items": items, "total": len(items)}


@router.get("/windows/{window_no}")
def get_window(window_no: str) -> dict[str, Any]:
    """窗口出数：仅对已发布记录聚合，并标出仍未到账的范围。"""
    return service.window_snapshot(window_no)


@alias_router.get("/windows/{window_no}")
def get_window_alias(window_no: str) -> dict[str, Any]:
    return get_window(window_no)


@router.post("/windows/{window_no}/settle", response_model=ActionResult)
def settle_window(window_no: str) -> ActionResult:
    """窗口实际入账后回写转换核对清单。"""
    snapshot, message = service.settle_window(window_no)
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)
