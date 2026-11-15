"""补丁流水线 · HTTP 接口与页面端到端测试。"""
from __future__ import annotations


def test_pipeline_page_renders_empty_state(client):
    client.post("/api/pipeline/reset")
    page = client.get("/pipeline")
    assert page.status_code == 200
    assert "字段级合并与跨表原子发布" in page.text
    assert "还没有补丁" in page.text


def test_health_exposes_pipeline_counts(client):
    client.post("/api/pipeline/reset")
    body = client.get("/health").json()
    assert body["pipeline"] == {
        "events": 0,
        "staging_keys": 0,
        "live_keys": 0,
        "pending_adjudications": 0,
        "published_changes": 0,
    }


def test_seed_sample_patch_flow_end_to_end(client):
    client.post("/api/pipeline/reset")
    seed = client.post("/api/pipeline/seed").json()
    assert seed["archive"]["inserted"] == 15
    assert seed["counts"]["events"] == 15

    view = client.get("/api/pipeline/view").json()
    statuses = {c["change_id"]: c["status"] for c in view["changes"]}
    # 跨表变更已原子发布；冲突变更被阻塞
    assert statuses["CHG-20261115-01"] == "published"
    assert statuses["CHG-20261115-02"] == "blocked"

    # 半行补丁：O7001 的 amount 未被清空，status 已更新且进入现网
    live = {(r["table"], r["pk"]): r for r in view["live"]}
    fields = {f["field"]: f["value"] for f in live[("orders", "O7001")]["fields"]}
    assert fields["amount"] == 8800 and fields["qty"] == 2 and fields["status"] == "paid"

    # 跨表：客户侧也在同一变更里发布
    cfields = {f["field"]: f["value"] for f in live[("customers", "C7001")]["fields"]}
    assert cfields["tier"] == "platinum"

    # 待裁决项：先到值 9900 保留
    pending = [a for a in view["adjudications"] if a["status"] == "pending"]
    assert {a["field"] for a in pending} == {"amount", "updated_at"}
    amount_item = next(a for a in pending if a["field"] == "amount")
    assert amount_item["kept_value"] == 9900 and amount_item["incoming_value"] == 9955


def test_blocked_change_rejects_publish_with_400(client):
    client.post("/api/pipeline/reset")
    client.post("/api/pipeline/seed")
    resp = client.post("/api/pipeline/publish", json={"change_id": "CHG-20261115-02"})
    assert resp.status_code == 400
    assert "待裁决" in resp.json()["detail"]


def test_adjudication_endpoint_unblocks_and_publishes(client):
    client.post("/api/pipeline/reset")
    client.post("/api/pipeline/seed")
    view = client.get("/api/pipeline/view").json()
    pending = [a for a in view["adjudications"] if a["status"] == "pending"]
    for item in pending:
        resp = client.post(
            "/api/pipeline/adjudicate",
            json={"item_id": item["item_id"], "choose": "incoming", "note": "确认新口径"},
        )
        assert resp.status_code == 200
    view2 = client.get("/api/pipeline/view").json()
    statuses = {c["change_id"]: c["status"] for c in view2["changes"]}
    assert statuses["CHG-20261115-02"] == "published"
    live = {(r["table"], r["pk"]): r for r in view2["live"]}
    amount = next(f for f in live[("orders", "O7002")]["fields"] if f["field"] == "amount")
    assert amount["value"] == 9955


def test_retry_seed_is_idempotent_and_publish_count_stable(client):
    client.post("/api/pipeline/reset")
    first = client.post("/api/pipeline/seed").json()
    successes_first = client.get("/health").json()["pipeline"]["published_changes"]
    second = client.post("/api/pipeline/seed").json()
    assert second["archive"]["inserted"] == 0
    assert client.get("/health").json()["pipeline"]["published_changes"] == successes_first

    # 显式重复发布返回 skipped_duplicate，不新增成功台账
    dup = client.post("/api/pipeline/publish", json={"change_id": "CHG-20261115-01"}).json()
    assert dup["entry"]["status"] == "skipped_duplicate"
    assert client.get("/health").json()["pipeline"]["published_changes"] == successes_first
    assert first["archive"]["attempted"] == 15


def test_fault_injection_returns_409_then_resume_from_checkpoint(client):
    client.post("/api/pipeline/reset")
    failed = client.post("/api/pipeline/seed", json={"fail_after_line": 3})
    assert failed.status_code == 409
    body = failed.json()
    assert body["retryable"] is True
    cp = {(c["source"]): c["last_line_no"] for c in body["view"]["checkpoints"]}
    assert cp["orders-lib.jsonl"] == 3
    # 失败时现网为空（保留上一版）
    assert body["view"]["counts"]["live_keys"] == 0

    resumed = client.post("/api/pipeline/seed")
    assert resumed.status_code == 200
    view = resumed.json()["view"]
    statuses = {c["change_id"]: c["status"] for c in view["changes"]}
    assert statuses["CHG-20261115-01"] == "published"
    # 检查点最终推进到全部来源末尾
    cps = {c["source"]: c["last_line_no"] for c in view["checkpoints"]}
    assert cps["orders-lib.jsonl"] == 6
    assert cps["customers-lib.jsonl"] == 3
    assert cps["inventory-lib.jsonl"] == 6


def test_inline_ingest_half_arrived_change_is_waiting(client):
    client.post("/api/pipeline/reset")
    resp = client.post(
        "/api/pipeline/ingest",
        json={
            "events": [
                {
                    "table": "orders", "pk": "OX1", "version": 1,
                    "ts": "2026-11-15T10:00:00Z", "op": "c",
                    "after": {"order_id": "OX1", "amount": 100},
                    "change_id": "CX-1", "source": "inline.jsonl", "line_no": 1,
                }
            ],
            "manifests": [
                {"change_id": "CX-1", "expected_tables": ["orders", "customers"]}
            ],
        },
    )
    assert resp.status_code == 200
    view = resp.json()["view"]
    chg = next(c for c in view["changes"] if c["change_id"] == "CX-1")
    assert chg["status"] == "waiting" and chg["missing_tables"] == ["customers"]
    # 半到达不进现网
    assert view["counts"]["live_keys"] == 0
    blocked = client.post("/api/pipeline/publish", json={"change_id": "CX-1"})
    assert blocked.status_code == 400 and "未到齐" in blocked.json()["detail"]


def test_ingest_validation_error_is_400(client):
    client.post("/api/pipeline/reset")
    resp = client.post(
        "/api/pipeline/ingest",
        json={"events": [{"table": "orders", "pk": "X", "version": 0,
                          "ts": "2026-11-15T10:00:00Z", "op": "u", "after": {}}]},
    )
    assert resp.status_code in (400, 422)


def test_pipeline_page_lists_provenance_adjudication_and_before_after(client):
    client.post("/api/pipeline/reset")
    client.post("/api/pipeline/seed")
    page = client.get("/pipeline").text
    assert "业务变更跨表闸门" in page
    assert "字段级来源对照" in page
    assert "前值 → 后值" in page
    assert "orders-lib.jsonl" in page          # 字段来源坐标
    assert "9900" in page and "9955" in page   # 待裁决前后值
    assert "来源检查点" in page
    assert "发布台账" in page


def test_publishes_log_endpoint(client):
    client.post("/api/pipeline/reset")
    client.post("/api/pipeline/seed")
    rows = client.get("/api/pipeline/publishes").json()["publishes"]
    successes = [r for r in rows if r["status"] == "success"]
    assert successes
    assert all(r["field_changes"] for r in successes)


def test_legacy_merge_still_works_alongside_pipeline(client):
    """新流水线不影响旧的整行合并/水位线/门禁能力。"""
    resp = client.post(
        "/api/merge",
        json={"tables": ["orders", "customers", "inventory"], "schema_version": 2},
    )
    assert resp.status_code == 200
    summary = resp.json()["summary"]
    assert summary["conflicts"] == 3 and summary["late"] == 8
