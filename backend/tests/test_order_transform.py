"""订单转换批次发布、隔离、续跑与窗口入账的行为断言。"""
import pytest

from app.services.order_transform import (
    OrderTransformService, PUBLISHED, QUARANTINED, PENDING,
)
from app.services.pipeline_orchestrator import run_window_metric_pipeline
from app.store import store

WINDOW = "WIN-TUMBLE-60s"


@pytest.fixture()
def svc():
    return OrderTransformService()


def orders(**extra):
    payload = {
        "window_no": WINDOW,
        "orders": [
            {"源订单编号": "NEW-1", "金额": "88.80", "时间": "2026-11-16T09:00:00Z", "用户ID": "U9001", "渠道": "app"},
            {"源订单编号": "NEW-2", "金额": "不是数字", "时间": "2026-11-16T09:00:10Z", "用户ID": "U9002"},
            {"源订单编号": "NEW-3", "金额": 0, "时间": "2026-11-16T09:00:20", "用户ID": "U9003"},
            {"源订单编号": "NEW-4", "金额": 12, "时间": "2026-11-16T01:00:30Z", "用户ID": ""},
            {"源订单编号": "NEW-5", "金额": 300, "时间": "2026-11-16T09:00:40Z", "用户ID": "U9005", "渠道": "h5"},
        ],
    }
    payload.update(extra)
    return payload


class TestPublishAndIsolation:
    def test_bad_values_isolated_not_zero_or_now(self, svc):
        batch, _, ok = svc.publish(orders())
        assert ok is True
        assert batch["received_count"] == 5
        assert batch["published_count"] == 2
        assert batch["quarantined_count"] == 3
        assert batch["status"] == "部分发布"
        rows = {row["source_order_id"]: row for row in svc.checklist(batch["batch_no"])}
        bad_amount = rows["NEW-2"]
        assert bad_amount["status"] == QUARANTINED
        assert bad_amount["amount"] is None  # 不是 0
        assert bad_amount["raw_amount"] == "不是数字"
        bad_time = rows["NEW-3"]
        assert bad_time["status"] == QUARANTINED
        assert bad_time["event_time"] is None  # 不是当前时间
        assert set(bad_time["reject_fields"]) == {"amount", "event_time"}
        assert "缺少时区" in bad_time["reject_reason"]
        assert rows["NEW-4"]["reject_fields"] == ["user_id"]

    def test_published_amount_ignores_quarantine(self, svc):
        batch, _, _ = svc.publish(orders())
        assert batch["published_amount"] == 388.8

    def test_batch_is_persisted_in_store(self, svc):
        batch, _, _ = svc.publish(orders())
        stored = [row for row in store.rows("order_transform_batch")
                  if row["batch_no"] == batch["batch_no"]]
        assert len(stored) == 1
        assert len(stored[0]["duplicates"]) == 0
        records = [row for row in store.rows("order_transform_record")
                   if row["batch_no"] == batch["batch_no"]]
        assert {row["source_order_id"] for row in records} == {
            "NEW-1", "NEW-2", "NEW-3", "NEW-4", "NEW-5"}

    def test_empty_orders_rejected(self, svc):
        batch, message, ok = svc.publish({"orders": []})
        assert batch is None and ok is False
        assert "为空" in message


class TestInterruptionAndRetry:
    def test_failed_write_keeps_raw_values(self, svc):
        batch, message, ok = svc.publish(orders(fail_after=2))
        assert ok is False
        assert batch["status"] == "待确认"
        assert batch["published_count"] == 1
        assert batch["quarantined_count"] == 1
        assert batch["pending_count"] == 3
        pending = [row for row in svc.checklist(batch["batch_no"]) if row["status"] == PENDING]
        assert {row["source_order_id"] for row in pending} == {"NEW-3", "NEW-4", "NEW-5"}
        assert all(row["raw_amount"] is not None or row["raw_event_time"] is not None
                   for row in pending)

    def test_retry_resumes_without_duplicates(self, svc):
        batch, _, _ = svc.publish(orders(fail_after=2))
        resumed, _, ok = svc.retry(batch["batch_no"])
        assert ok is True
        assert resumed["published_count"] == 2
        assert resumed["quarantined_count"] == 3
        assert resumed["pending_count"] == 0
        total = len([row for row in store.rows("order_transform_record")
                     if row["batch_no"] == batch["batch_no"]])
        assert total == 5
        again, _, ok_again = svc.retry(batch["batch_no"])
        assert ok_again is True
        assert again["published_count"] == 2
        assert len([row for row in store.rows("order_transform_record")
                    if row["batch_no"] == batch["batch_no"]]) == 5

    def test_idempotency_key_replays_same_batch(self, svc):
        first, _, _ = svc.publish(orders(idempotency_key="ext-1"))
        replay, message, ok = svc.publish(orders(idempotency_key="ext-1", fail_after=0))
        assert ok is True
        assert replay["batch_no"] == first["batch_no"]
        assert "幂等键" in message
        assert len(store.rows("order_transform_batch")) == 2  # 基线 1 条 + 本批 1 条

    def test_duplicate_source_order_not_double_published(self, svc):
        svc.publish(orders())
        before = svc.window_snapshot(WINDOW)["published"]["count"]
        dup, message, _ = svc.publish({"orders": [
            {"源订单编号": "NEW-1", "金额": 88.8, "时间": "2026-11-16T09:00:00Z", "用户ID": "U9001"},
        ]})
        assert dup["duplicate_count"] == 1
        assert dup["total_count"] == 0
        assert svc.window_snapshot(WINDOW)["published"]["count"] == before

    def test_resubmitted_quarantined_order_is_revalidated_in_place(self, svc):
        first, _, _ = svc.publish(orders())
        healed, message, ok = svc.publish({"orders": [
            {"源订单编号": "NEW-2", "金额": 10, "时间": "2026-11-16T09:00:10Z", "用户ID": "U9002"},
        ]})
        assert ok is True
        assert healed["duplicate_count"] == 1
        assert healed["total_count"] == 0
        assert healed["recovered_orders"][0]["source_order_id"] == "NEW-2"
        assert healed["recovered_orders"][0]["status"] == PUBLISHED
        assert "就地" in message
        record = svc.get_order("NEW-2")
        assert record["batch_no"] == first["batch_no"]
        assert record["status"] == PUBLISHED
        assert record["raw_amount"] == "不是数字"
        assert record["amount"] == 10.0
        old = svc.get_batch(first["batch_no"])
        assert old["published_count"] == 3
        assert old["quarantined_count"] == 2
        # 重新提交不会额外增加记录行
        assert len([row for row in store.rows("order_transform_record")
                    if row["source_order_id"] == "NEW-2"]) == 1


class TestCorrection:
    def test_correction_publishes_without_losing_raw(self, svc):
        batch, _, _ = svc.publish(orders())
        view, message, ok = svc.correct_record(
            batch["batch_no"], "NEW-3",
            {"金额": 55, "时间": "2026-11-16T09:00:20+08:00"})
        assert ok is True
        assert view["status"] == PUBLISHED
        assert view["amount"] == 55.0
        assert view["raw_amount"] == 0
        assert view["raw_event_time"] == "2026-11-16T09:00:20"
        assert view["corrected"] is True
        assert view["batch"]["published_count"] == 3

    def test_partial_batch_correction_and_recount(self, svc):
        batch, _, _ = svc.publish(orders())
        summary, _, ok = svc.correct_batch(batch["batch_no"], {"corrections": [
            {"源订单编号": "NEW-2", "金额": 10},
            {"源订单编号": "NEW-4", "用户ID": "U9004"},
        ]})
        assert ok is True
        assert summary["published_count"] == 4
        assert summary["quarantined_count"] == 1
        assert summary["published_amount"] == 88.8 + 10 + 12 + 300

    def test_bad_correction_stays_quarantined(self, svc):
        batch, _, _ = svc.publish(orders())
        view, _, ok = svc.correct_record(batch["batch_no"], "NEW-3", {"金额": "0"})
        assert ok is False
        assert view["status"] == QUARANTINED


class TestWindowAndChecklist:
    def test_window_only_counts_published_and_marks_pending_range(self, svc):
        batch, _, _ = svc.publish(orders())
        snapshot = svc.window_snapshot(WINDOW)
        assert snapshot["published"]["count"] == 12  # 基线 10 + 本批 2
        assert snapshot["published"]["amount"] == pytest.approx(1827.7 + 388.8)
        assert snapshot["pending"]["count"] == 3
        assert set(snapshot["pending"]["order_ids"]) == {"NEW-2", "NEW-3", "NEW-4"}
        # 隔离行里的可解析时间参与范围计算：NEW-4 的 01:00:30 是范围下限
        assert snapshot["pending"]["range"]["from"] == "2026-11-16T01:00:30+00:00"
        assert snapshot["pending"]["range"]["to"] == "2026-11-16T09:00:10+00:00"
        assert snapshot["pending"]["batch_nos"] == [batch["batch_no"]]
        assert snapshot["pending"]["unbooked_published"] == 12

    def test_settle_writes_back_checklist_once(self, svc):
        batch, _, _ = svc.publish(orders())
        snapshot, message = svc.settle_window(WINDOW)
        assert snapshot["settled_now"] == 12
        assert snapshot["settled"]["count"] == 12
        assert all(row["核对状态"] == "已入账" for row in snapshot["checklist"]
                   if row["status"] == PUBLISHED)
        assert all(row["核对状态"] == "隔离待修正" for row in snapshot["checklist"]
                   if row["status"] == QUARANTINED)
        again, message = svc.settle_window(WINDOW)
        assert again["settled_now"] == 0
        assert again["settled"]["count"] == 12
        assert "未重复回写" in message
        stored = [row for row in store.rows("order_transform_record")
                  if row["batch_no"] == batch["batch_no"] and row["status"] == PUBLISHED]
        assert all(row["booked"] for row in stored)

    def test_settled_record_cannot_be_corrected(self, svc):
        batch, _, _ = svc.publish(orders())
        svc.settle_window(WINDOW)
        _, message, ok = svc.correct_record(batch["batch_no"], "NEW-1", {"金额": 1})
        assert ok is False
        assert "已入账" in message

    def test_lookup_by_source_order_id_stable(self, svc):
        batch, _, _ = svc.publish(orders())
        first = svc.get_order("NEW-1")
        svc.correct_record(batch["batch_no"], "NEW-3", {"金额": 5})
        svc.settle_window(WINDOW)
        again = svc.get_order("NEW-1")
        assert first["batch_no"] == again["batch_no"] == batch["batch_no"]
        assert again["amount"] == 88.8
        assert again["核对状态"] == "已入账"
        assert again["raw_amount"] == "88.80"


class TestPipelineUsesPublishedBatches:
    def test_window_pipeline_reads_persisted_batch(self, svc):
        svc.publish(orders())
        result = run_window_metric_pipeline()
        assert result["total_records"] == 12
        assert result[WINDOW]["记录数"] == 12
        assert result[WINDOW]["未到账笔数"] == 3
        assert result[WINDOW]["gmv"] == pytest.approx(2216.5)
        assert result["metrics"]["METR-ORDER-COUNT"] == 12

    def test_new_window_gets_its_own_snapshot(self, svc):
        svc.publish(orders(window_no="WIN-CUSTOM-1m"))
        result = run_window_metric_pipeline()
        assert result["WIN-CUSTOM-1m"]["记录数"] == 2
        assert result[WINDOW]["记录数"] == 10
        assert result["pending_total"] == 3


class TestRouteSurface:
    def test_publish_and_window_endpoints_registered(self):
        from app.main import app

        paths = set(app.openapi()["paths"])
        assert "/api/order_transform/batches" in paths
        assert "/api/stream_transform/batches" in paths
        assert "/api/order_transform/batches/{batch_no}/retry" in paths
        assert "/api/order_transform/batches/{batch_no}/records/{source_order_id}" in paths
        assert "/api/order_transform/orders/{source_order_id}" in paths
        assert "/api/order_transform/windows" in paths
        assert "/api/order_transform/windows/{window_no}" in paths
        assert "/api/order_transform/windows/{window_no}/settle" in paths
        # 原有 DAG 图与导出接口保持不变
        assert "/api/stream_transform" in paths
        assert "/api/stream_transform/export" in paths

    def test_idempotent_replay_of_interrupted_batch_reports_pending(self, svc):
        first, _, _ = svc.publish(orders(idempotency_key="ext-2", fail_after=2))
        replay, message, ok = svc.publish(orders(idempotency_key="ext-2"))
        assert ok is False
        assert replay["batch_no"] == first["batch_no"]
        assert "重试续跑" in message
