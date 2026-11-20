"""订单转换产物发布与窗口入账核对。

流式转换算子的输出不再直接复用固定样本：每一批新订单先以原始值落进
``order_transform_batch`` / ``order_transform_record``，逐行校验金额、事件时间和
用户字段后才算「已发布」。校验不通过的行进入隔离区，原始值与源订单编号完整保留，
修正或重试后可以继续发布，不会把坏值折成 0 元或当前时间。

窗口聚合只读取已发布记录；已隔离、待确认的行会以「未到账」口径单独报出。
窗口真正入账后回写核对清单，刷新或重新查询时按源订单编号仍能取回同一批次。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.store import store

BATCH_MODULE = "order_transform_batch"
RECORD_MODULE = "order_transform_record"

PUBLISHED = "已发布"
QUARANTINED = "已隔离"
PENDING = "待确认"

BATCH_PUBLISHED = "已发布"
BATCH_PARTIAL = "部分发布"
BATCH_PENDING = "待确认"
BATCH_ALL_QUARANTINED = "全部隔离"
BATCH_DUPLICATE = "重复提交"

CHECK_PENDING = "待入账"
CHECK_SETTLED = "已入账"
CHECK_QUARANTINED = "隔离待修正"
CHECK_UNCONFIRMED = "待确认"

DEFAULT_WINDOW = "WIN-TUMBLE-60s"

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "source_order_id": ("source_order_id", "源订单编号", "订单编号", "order_id"),
    "amount": ("amount", "金额", "订单金额"),
    "event_time": ("event_time", "时间", "订单时间", "事件时间", "timestamp"),
    "user_id": ("user_id", "用户ID", "用户", "用户编号"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _pick(values: dict[str, Any], field: str) -> tuple[bool, Any]:
    """按中英文字段别名取值，返回 (是否提供, 值)。"""
    for key in FIELD_ALIASES[field]:
        if key in values:
            return True, values[key]
    return False, None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_amount(value: Any) -> tuple[float | None, str]:
    """金额必须是可解析、有限且大于 0 的数值；异常值返回 None，绝不回退成 0。"""
    if value is None or isinstance(value, bool):
        return None, "金额缺失"
    text = str(value).strip()
    if not text:
        return None, "金额缺失"
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        return None, f"金额「{text}」不是有效数值"
    if not number.is_finite():
        return None, f"金额「{text}」不是有限数值"
    if number <= 0:
        return None, f"金额「{text}」必须大于 0"
    return float(number), ""


def _parse_event_time(value: Any) -> tuple[str | None, str]:
    """事件时间必须是带时区的 ISO 时间；无时区时间单独隔离，不回退成当前时间。"""
    if value is None or isinstance(value, bool):
        return None, "事件时间缺失"
    text = str(value).strip()
    if not text:
        return None, "事件时间缺失"
    normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        moment = datetime.fromisoformat(normalized)
    except ValueError:
        return None, f"事件时间「{text}」无法解析"
    if moment.tzinfo is None or moment.utcoffset() is None:
        return None, f"事件时间「{text}」缺少时区"
    return moment.isoformat(), ""


def _parse_user(value: Any) -> tuple[str | None, str]:
    if value is None:
        return None, "用户字段缺失"
    text = str(value).strip()
    if not text:
        return None, "用户字段缺失"
    return text, ""


def _record_id() -> int:
    return max((int(row.get("id", 0)) for row in store.rows(RECORD_MODULE)), default=0) + 1


def _batch_id() -> int:
    return max((int(row.get("id", 0)) for row in store.rows(BATCH_MODULE)), default=0) + 1


def _next_batch_no() -> str:
    existing = {str(row.get("batch_no", "")) for row in store.rows(BATCH_MODULE)}
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    seq = len(existing) + 1
    while True:
        candidate = f"OTB-{date}-{seq:04d}"
        if candidate not in existing:
            return candidate
        seq += 1


def _records_of(batch: dict[str, Any]) -> list[dict[str, Any]]:
    batch_id = int(batch.get("id", 0))
    return [row for row in store.rows(RECORD_MODULE) if int(row.get("batch_id", 0)) == batch_id]


def _all_records() -> list[dict[str, Any]]:
    return store.rows(RECORD_MODULE)


def _find_record_by_source(source_order_id: str) -> dict[str, Any] | None:
    text = _text(source_order_id)
    if not text:
        return None
    for row in _all_records():
        if _text(row.get("source_order_id")) == text:
            return row
    return None


def _check_status(record: dict[str, Any]) -> str:
    if record.get("booked"):
        return CHECK_SETTLED
    status = record.get("status")
    if status == PUBLISHED:
        return CHECK_PENDING
    if status == QUARANTINED:
        return CHECK_QUARANTINED
    return CHECK_UNCONFIRMED


def record_view(record: dict[str, Any], *, include_raw: bool = True) -> dict[str, Any]:
    """核对清单/批次明细里对外展示的一行，原始值始终可追溯。"""
    view: dict[str, Any] = {
        "id": int(record.get("id", 0)),
        "batch_no": record.get("batch_no", ""),
        "row_no": record.get("row_no"),
        "source_order_id": record.get("source_order_id", ""),
        "源订单编号": record.get("source_order_id", ""),
        "window_no": record.get("window_no", ""),
        "status": record.get("status", PENDING),
        "核对状态": _check_status(record),
        "amount": record.get("amount"),
        "event_time": record.get("event_time"),
        "user_id": record.get("user_id"),
        "channel": record.get("channel"),
        "region": record.get("region"),
        "reject_fields": list(record.get("reject_fields") or []),
        "reject_reason": record.get("reject_reason", ""),
        "corrected": bool(record.get("corrected")),
        "published_at": record.get("published_at"),
        "booked": bool(record.get("booked")),
        "booked_at": record.get("booked_at"),
        "booking_window_no": record.get("booking_window_no"),
    }
    if include_raw:
        view.update({
            "raw_amount": record.get("raw_amount"),
            "raw_event_time": record.get("raw_event_time"),
            "raw_user_id": record.get("raw_user_id"),
            "原始金额": record.get("raw_amount"),
            "原始时间": record.get("raw_event_time"),
            "原始用户": record.get("raw_user_id"),
            "corrected_amount": record.get("corrected_amount"),
            "corrected_event_time": record.get("corrected_event_time"),
            "corrected_user_id": record.get("corrected_user_id"),
        })
    return view


class OrderTransformService:
    # ------------------------------------------------------------------ 发布
    def publish(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        """接收并持久化一批订单，随后逐行校验发布。

        返回 (批次摘要, 提示, 是否完整成功)。即使中途模拟落库失败，批次和已接收的
        原始行也已持久化，可凭批次号继续重试或修正。
        """
        window_no = _text(values.get("window_no") or values.get("窗口编号")) or DEFAULT_WINDOW
        orders = values.get("orders")
        if orders is None:
            orders = values.get("records")
        if orders is None:
            orders = values.get("items")
        if isinstance(orders, str):
            try:
                orders = json.loads(orders)
            except json.JSONDecodeError:
                return None, "订单列表不是合法 JSON", False
        if not isinstance(orders, list) or not orders:
            return None, "订单列表为空，未生成转换批次", False
        normalized_orders = [item for item in orders if isinstance(item, dict)]
        if not normalized_orders:
            return None, "订单列表中没有可读取的订单对象", False

        idempotency_key = _text(values.get("idempotency_key") or values.get("幂等键"))
        if idempotency_key:
            for batch in store.rows(BATCH_MODULE):
                if _text(batch.get("idempotency_key")) == idempotency_key:
                    summary = self.batch_summary(batch)
                    if summary["pending_count"]:
                        return summary, (
                            f"幂等键命中批次 {batch.get('batch_no')}，该批仍有 "
                            f"{summary['pending_count']} 笔待确认，请执行重试续跑"
                        ), False
                    return summary, f"幂等键命中已有批次 {batch.get('batch_no')}，未重复发布", True

        requested_batch_no = _text(values.get("batch_no") or values.get("批次号"))
        if requested_batch_no:
            for batch in store.rows(BATCH_MODULE):
                if _text(batch.get("batch_no")) == requested_batch_no:
                    summary = self.batch_summary(batch)
                    return summary, f"批次 {requested_batch_no} 已存在，按原批次返回", True

        fail_after = values.get("fail_after")
        if fail_after is not None:
            try:
                fail_after = max(0, int(fail_after))
            except (TypeError, ValueError):
                return None, "fail_after 必须是整数", False

        batch = {
            "id": _batch_id(),
            "batch_no": requested_batch_no or _next_batch_no(),
            "window_no": window_no,
            "source": "stream_transform",
            "status": BATCH_PENDING,
            "remark": _text(values.get("remark") or values.get("备注")),
            "idempotency_key": idempotency_key,
            "received_count": len(normalized_orders),
            "duplicate_count": 0,
            "submitted_at": _now(),
            "confirmed_at": None,
            "settled_at": None,
            "pending": True,
            "abnormal": False,
        }
        store.rows(BATCH_MODULE).append(batch)

        duplicates: list[dict[str, Any]] = []
        recovered: list[dict[str, Any]] = []
        seen_in_batch: set[str] = set()
        for index, order in enumerate(normalized_orders, start=1):
            source_order_id = _text(_pick(order, "source_order_id")[1])
            if source_order_id:
                existing = _find_record_by_source(source_order_id)
                if existing is not None and existing.get("status") != PUBLISHED:
                    # 上一批里还没确认或已隔离的同一源订单：就地带入本次原始值重新校验，
                    # 既不新开一行，也不会把修正后的订单当成重复丢掉。
                    if self._merge_correction(existing, order):
                        self._validate_and_apply(existing)
                        owner = self.find_batch(str(existing.get("batch_no", "")))
                        if owner is not None:
                            self._refresh_batch(owner)
                    recovered.append({
                        "source_order_id": source_order_id,
                        "batch_no": existing.get("batch_no"),
                        "status": existing.get("status"),
                        "reject_reason": existing.get("reject_reason", ""),
                    })
                    seen_in_batch.add(source_order_id)
                    continue
                if existing is not None or source_order_id in seen_in_batch:
                    duplicates.append({
                        "source_order_id": source_order_id,
                        "batch_no": existing.get("batch_no") if existing else batch["batch_no"],
                        "status": existing.get("status") if existing else PENDING,
                    })
                    continue
                seen_in_batch.add(source_order_id)
            _, raw_amount = _pick(order, "amount")
            _, raw_event_time = _pick(order, "event_time")
            _, raw_user_id = _pick(order, "user_id")
            record = {
                "id": _record_id(),
                "batch_id": batch["id"],
                "batch_no": batch["batch_no"],
                "row_no": index,
                "source_order_id": source_order_id,
                "raw_amount": raw_amount,
                "raw_event_time": raw_event_time,
                "raw_user_id": raw_user_id,
                "corrected_amount": None,
                "corrected_event_time": None,
                "corrected_user_id": None,
                "amount": None,
                "event_time": None,
                "user_id": None,
                "status": PENDING,
                "reject_fields": [],
                "reject_reason": "",
                "corrected": False,
                "corrected_at": None,
                "window_no": window_no,
                "channel": order.get("channel") or order.get("渠道"),
                "region": order.get("region") or order.get("地区"),
                "published_at": None,
                "booked": False,
                "booked_at": None,
                "booking_window_no": None,
                "pending": True,
                "abnormal": False,
            }
            store.rows(RECORD_MODULE).append(record)

        batch["duplicate_count"] = len(duplicates) + len(recovered)
        batch["duplicates"] = duplicates
        batch["recovered_orders"] = recovered

        interrupted = False
        processed = 0
        for record in _records_of(batch):
            if record.get("status") != PENDING:
                continue
            if fail_after is not None and processed >= fail_after:
                interrupted = True
                break
            self._validate_and_apply(record)
            processed += 1

        self._refresh_batch(batch)
        summary = self.batch_summary(batch)
        summary["duplicates"] = duplicates
        if interrupted:
            return (
                summary,
                "批次已落库，但在确认过程中写入失败；原始值与源订单编号已保留，可重试续跑",
                False,
            )
        published = summary["published_count"]
        quarantined = summary["quarantined_count"]
        if summary["total_count"] == 0:
            if recovered:
                healed = sum(1 for item in recovered if item["status"] == PUBLISHED)
                return summary, (
                    f"本批 {len(normalized_orders)} 笔均为重复提交，未重复入账；"
                    f"其中 {healed} 笔在原批次中就地带入新值后已发布"
                ), True
            return summary, f"本批 {len(normalized_orders)} 笔均为已发布订单的重复提交，未重复入账", True
        if quarantined and published:
            return summary, f"批次 {batch['batch_no']} 部分发布：{published} 笔已发布，{quarantined} 笔已隔离待修正", True
        if quarantined:
            return summary, f"批次 {batch['batch_no']} 全部隔离：{quarantined} 笔原始值已保留，可修正后重试", True
        return summary, f"批次 {batch['batch_no']} 已发布 {published} 笔", True

    def retry(self, batch_no: str, values: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str, bool]:
        """续跑中断批次：只处理仍未确认的行，已确认/已发布的订单不会重复入账。"""
        batch = self.find_batch(batch_no)
        if batch is None:
            return None, f"转换批次 {batch_no} 不存在", False
        values = values or {}
        corrections = values.get("corrections")
        if corrections is None:
            corrections = values.get("records")
        if corrections:
            self._apply_corrections(batch, corrections)

        processed = 0
        refreshed = 0
        for record in _records_of(batch):
            if record.get("status") != PENDING:
                continue
            repaired = self._validate_and_apply(record)
            processed += 1
            refreshed += 1 if repaired else 0

        self._refresh_batch(batch)
        summary = self.batch_summary(batch)
        if summary["pending_count"]:
            return summary, f"批次 {batch_no} 仍有 {summary['pending_count']} 笔待确认，可再次重试", False
        return summary, f"批次 {batch_no} 已续跑完成：本次确认 {processed} 笔，发布 {refreshed} 笔", True

    def correct_record(
        self, batch_no: str, source_order_id: str, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str, bool]:
        """修正隔离行的字段；原始值保留，修正值通过校验后转为已发布。"""
        batch = self.find_batch(batch_no)
        if batch is None:
            return None, f"转换批次 {batch_no} 不存在", False
        record = None
        for row in _records_of(batch):
            if _text(row.get("source_order_id")) == _text(source_order_id):
                record = row
                break
        if record is None:
            return None, f"批次 {batch_no} 中找不到源订单 {source_order_id}", False
        if record.get("booked"):
            return record_view(record), f"订单 {source_order_id} 已入账，不能再修正", False
        if not self._merge_correction(record, values):
            return record_view(record), "没有提供可修正的金额、时间或用户字段", False
        self._validate_and_apply(record)
        self._refresh_batch(batch)
        summary = self.batch_summary(batch)
        view = record_view(record)
        view["batch"] = summary
        if record.get("status") == PUBLISHED:
            return view, f"订单 {source_order_id} 修正后已发布", True
        return view, f"订单 {source_order_id} 修正后仍未通过校验：{record.get('reject_reason', '')}", False

    def correct_batch(self, batch_no: str, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        batch = self.find_batch(batch_no)
        if batch is None:
            return None, f"转换批次 {batch_no} 不存在", False
        corrections = values.get("corrections")
        if corrections is None:
            corrections = values.get("records")
        if not corrections:
            return self.batch_summary(batch), "没有提供修正内容", False
        handled = self._apply_corrections(batch, corrections)
        self._refresh_batch(batch)
        summary = self.batch_summary(batch)
        return summary, f"批次 {batch_no} 已处理 {handled} 条修正，发布 {summary['published_count']} 笔", True

    # ------------------------------------------------------------------ 查询
    def find_batch(self, batch_no: str) -> dict[str, Any] | None:
        text = _text(batch_no)
        for batch in store.rows(BATCH_MODULE):
            if _text(batch.get("batch_no")) == text:
                return batch
        return None

    def list_batches(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        window_no: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(BATCH_MODULE))
        if keyword:
            rows = [
                row for row in rows
                if keyword.casefold() in _text(row.get("batch_no")).casefold()
                or keyword.casefold() in _text(row.get("window_no")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if window_no:
            rows = [row for row in rows if _text(row.get("window_no")) == _text(window_no)]
        rows.sort(key=lambda row: int(row.get("id", 0)), reverse=True)
        total = len(rows)
        start = (page - 1) * size
        return [self.batch_summary(row) for row in rows[start:start + size]], total

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        batch = self.find_batch(batch_no)
        if batch is None:
            return None
        summary = self.batch_summary(batch)
        summary["records"] = [record_view(row) for row in _records_of(batch)]
        summary["checklist"] = self.checklist(batch_no)
        return summary

    def get_order(self, source_order_id: str) -> dict[str, Any] | None:
        record = _find_record_by_source(source_order_id)
        if record is None:
            return None
        view = record_view(record)
        batch = self.find_batch(str(record.get("batch_no", "")))
        view["window_no"] = record.get("window_no", "")
        view["batch_status"] = batch.get("status") if batch else None
        view["batch_submitted_at"] = batch.get("submitted_at") if batch else None
        return view

    def checklist(self, batch_no: str) -> list[dict[str, Any]]:
        batch = self.find_batch(batch_no)
        if batch is None:
            return []
        return [record_view(row) for row in _records_of(batch)]

    def batch_summary(self, batch: dict[str, Any]) -> dict[str, Any]:
        records = _records_of(batch)
        published = [row for row in records if row.get("status") == PUBLISHED]
        quarantined = [row for row in records if row.get("status") == QUARANTINED]
        pending = [row for row in records if row.get("status") == PENDING]
        booked = [row for row in published if row.get("booked")]
        return {
            "id": int(batch.get("id", 0)),
            "batch_no": batch.get("batch_no", ""),
            "批次号": batch.get("batch_no", ""),
            "window_no": batch.get("window_no", ""),
            "窗口编号": batch.get("window_no", ""),
            "source": batch.get("source", "stream_transform"),
            "status": batch.get("status", BATCH_PENDING),
            "状态": batch.get("status", BATCH_PENDING),
            "remark": batch.get("remark", ""),
            "received_count": int(batch.get("received_count", len(records))),
            "total_count": len(records),
            "published_count": len(published),
            "quarantined_count": len(quarantined),
            "pending_count": len(pending),
            "duplicate_count": int(batch.get("duplicate_count", 0)),
            "booked_count": len(booked),
            "published_amount": round(sum(float(row.get("amount") or 0) for row in published), 2),
            "submitted_at": batch.get("submitted_at"),
            "confirmed_at": batch.get("confirmed_at"),
            "settled_at": batch.get("settled_at"),
            "duplicates": list(batch.get("duplicates") or []),
            "recovered_orders": list(batch.get("recovered_orders") or []),
        }

    # ------------------------------------------------------------------ 窗口
    def published_records(self, window_no: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in _all_records() if row.get("status") == PUBLISHED]
        if window_no is not None:
            rows = [row for row in rows if _text(row.get("window_no")) == _text(window_no)]
        return rows

    def window_snapshot(self, window_no: str) -> dict[str, Any]:
        """只对已发布记录出数，并把未到账（隔离/待确认）范围单独标出。"""
        records = [
            row for row in _all_records()
            if _text(row.get("window_no")) == _text(window_no)
        ]
        published = [row for row in records if row.get("status") == PUBLISHED]
        pending = [row for row in records if row.get("status") in (QUARANTINED, PENDING)]
        booked = [row for row in published if row.get("booked")]
        unbooked = [row for row in published if not row.get("booked")]

        times = [_parse_stored_time(row.get("event_time")) for row in published]
        times = [moment for moment in times if moment is not None]
        pending_times = [
            _parse_stored_time(row.get("event_time") or row.get("raw_event_time") or row.get("corrected_event_time"))
            for row in pending
        ]
        pending_times = [moment for moment in pending_times if moment is not None]

        pending_range: dict[str, Any] | None = None
        range_source = "event_time"
        if pending:
            if pending_times:
                start, end = min(pending_times), max(pending_times)
            else:
                submitted = [
                    _parse_stored_time(row.get("submitted_at"))
                    for row in store.rows(BATCH_MODULE)
                    if _text(row.get("window_no")) == _text(window_no)
                ]
                submitted = [moment for moment in submitted if moment is not None]
                start = min(submitted) if submitted else None
                end = max(submitted) if submitted else None
                range_source = "batch_submitted_at"
            if start is not None and end is not None:
                pending_range = {"from": start.isoformat(), "to": end.isoformat(), "source": range_source}

        reasons: dict[str, int] = {}
        for row in pending:
            reason = _text(row.get("reject_reason")) or ("落库中断待确认" if row.get("status") == PENDING else "未知原因")
            reasons[reason] = reasons.get(reason, 0) + 1

        batches = [
            self.batch_summary(batch) for batch in store.rows(BATCH_MODULE)
            if _text(batch.get("window_no")) == _text(window_no)
        ]
        return {
            "ok": True,
            "window_no": window_no,
            "窗口编号": window_no,
            "published": {
                "count": len(published),
                "amount": round(sum(float(row.get("amount") or 0) for row in published), 2),
                "user_count": len({_text(row.get("user_id")) for row in published}),
                "first_event_time": min(times).isoformat() if times else None,
                "last_event_time": max(times).isoformat() if times else None,
                "booked_count": len(booked),
                "unbooked_count": len(unbooked),
            },
            "settled": {
                "count": len(booked),
                "amount": round(sum(float(row.get("amount") or 0) for row in booked), 2),
            },
            "pending": {
                "count": len(pending),
                "order_ids": [_text(row.get("source_order_id")) for row in pending],
                "batch_nos": sorted({_text(row.get("batch_no")) for row in pending}),
                "range": pending_range,
                "reasons": reasons,
                "unbooked_published": len(unbooked),
            },
            "batches": batches,
            "checklist": [record_view(row) for row in records],
        }

    def settle_window(self, window_no: str) -> tuple[dict[str, Any] | None, str]:
        """窗口实际入账：把已发布记录标记入账并回写转换核对清单。"""
        window_no = _text(window_no)
        if not window_no:
            return None, "缺少窗口编号"
        published = [
            row for row in _all_records()
            if _text(row.get("window_no")) == window_no and row.get("status") == PUBLISHED
        ]
        if not published:
            snapshot = self.window_snapshot(window_no)
            return snapshot, f"窗口 {window_no} 暂无可入账的已发布记录"
        now = _now()
        settled_now = 0
        settled_amount = 0.0
        for record in published:
            if record.get("booked"):
                continue
            record["booked"] = True
            record["booked_at"] = now
            record["booking_window_no"] = window_no
            record["pending"] = False
            settled_now += 1
            settled_amount += float(record.get("amount") or 0)
        affected = {
            _text(record.get("batch_no")) for record in published
        }
        for batch_no in affected:
            batch = self.find_batch(batch_no)
            if batch is None:
                continue
            records = _records_of(batch)
            if records and all(
                row.get("status") != PUBLISHED or row.get("booked") for row in records
            ):
                batch["settled_at"] = now
            self._refresh_batch(batch)
        snapshot = self.window_snapshot(window_no)
        snapshot["settled_now"] = settled_now
        snapshot["settled_amount"] = round(settled_amount, 2)
        if settled_now:
            message = f"窗口 {window_no} 已入账 {settled_now} 笔，金额 {round(settled_amount, 2)}"
        else:
            message = f"窗口 {window_no} 已发布记录均已入账，未重复回写"
        return snapshot, message

    def window_list(self) -> list[dict[str, Any]]:
        """窗口口径清单：窗口编号、批次与已发布/未到账计数。"""
        names: set[str] = set()
        for batch in store.rows(BATCH_MODULE):
            names.add(_text(batch.get("window_no")))
        for record in _all_records():
            names.add(_text(record.get("window_no")))
        names.discard("")
        items = []
        for name in sorted(names):
            snapshot = self.window_snapshot(name)
            items.append({
                "window_no": name,
                "批次数量": len(snapshot["batches"]),
                "已发布笔数": snapshot["published"]["count"],
                "已发布金额": snapshot["published"]["amount"],
                "未到账笔数": snapshot["pending"]["count"],
                "已入账笔数": snapshot["settled"]["count"],
            })
        return items

    # ------------------------------------------------------------------ 内部
    def _validate_and_apply(self, record: dict[str, Any]) -> bool:
        """校验一条记录的有效字段并落状态；返回本次是否从隔离/待确认变为已发布。"""
        amount_source = record.get("corrected_amount")
        if amount_source is None:
            amount_source = record.get("raw_amount")
        time_source = record.get("corrected_event_time")
        if time_source is None:
            time_source = record.get("raw_event_time")
        user_source = record.get("corrected_user_id")
        if user_source is None:
            user_source = record.get("raw_user_id")

        amount, amount_error = _parse_amount(amount_source)
        event_time, time_error = _parse_event_time(time_source)
        user_id, user_error = _parse_user(user_source)

        errors: list[str] = []
        reject_fields: list[str] = []
        if amount_error:
            errors.append(amount_error)
            reject_fields.append("amount")
        if time_error:
            errors.append(time_error)
            reject_fields.append("event_time")
        if user_error:
            errors.append(user_error)
            reject_fields.append("user_id")

        was_published = record.get("status") == PUBLISHED
        record["amount"] = amount
        record["event_time"] = event_time
        record["user_id"] = user_id
        if errors:
            record["status"] = QUARANTINED
            record["reject_fields"] = reject_fields
            record["reject_reason"] = "；".join(errors)
            record["published_at"] = None
            record["pending"] = True
            record["abnormal"] = True
            return False

        record["status"] = PUBLISHED
        record["reject_fields"] = []
        record["reject_reason"] = ""
        record["pending"] = False
        record["abnormal"] = False
        if not was_published or not record.get("published_at"):
            record["published_at"] = _now()
        return not was_published

    def _merge_correction(self, record: dict[str, Any], values: dict[str, Any]) -> bool:
        handled = False
        for field in ("amount", "event_time", "user_id"):
            provided, value = _pick(values, field)
            if not provided:
                continue
            record[f"corrected_{field}"] = value
            handled = True
        if handled:
            record["corrected"] = True
            record["corrected_at"] = _now()
        return handled

    def _apply_corrections(self, batch: dict[str, Any], corrections: Any) -> int:
        if isinstance(corrections, dict):
            entries = []
            for source_order_id, payload in corrections.items():
                item = dict(payload) if isinstance(payload, dict) else {"event_time": payload}
                item.setdefault("source_order_id", source_order_id)
                entries.append(item)
        elif isinstance(corrections, list):
            entries = [item for item in corrections if isinstance(item, dict)]
        else:
            return 0
        handled = 0
        records = _records_of(batch)
        for item in entries:
            source_order_id = _text(_pick(item, "source_order_id")[1])
            target = None
            for record in records:
                if _text(record.get("source_order_id")) == source_order_id:
                    target = record
                    break
            if target is None or target.get("booked"):
                continue
            if self._merge_correction(target, item):
                self._validate_and_apply(target)
                handled += 1
        return handled

    def _refresh_batch(self, batch: dict[str, Any]) -> None:
        records = _records_of(batch)
        published = [row for row in records if row.get("status") == PUBLISHED]
        quarantined = [row for row in records if row.get("status") == QUARANTINED]
        pending = [row for row in records if row.get("status") == PENDING]
        if pending:
            batch["status"] = BATCH_PENDING
        elif not records:
            batch["status"] = BATCH_DUPLICATE
        elif not published:
            batch["status"] = BATCH_ALL_QUARANTINED
        elif quarantined:
            batch["status"] = BATCH_PARTIAL
        else:
            batch["status"] = BATCH_PUBLISHED
        batch["pending"] = bool(pending or quarantined)
        batch["abnormal"] = bool(quarantined)
        if not pending:
            batch["confirmed_at"] = batch.get("confirmed_at") or _now()
        else:
            batch["confirmed_at"] = None


def _parse_stored_time(value: Any) -> datetime | None:
    """仅用于展示范围：能解析且带时区的时间才参与范围计算。"""
    text = _text(value)
    if not text:
        return None
    normalized = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        moment = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if moment.tzinfo is None or moment.utcoffset() is None:
        return None
    return moment


service = OrderTransformService()
