"""补丁流水线 · SQLite 持久化与编排（检查点续跑 / 原子发布 / 幂等 / 回滚）。"""
from __future__ import annotations

import pytest

from app import patch_runner, patch_store, store
from app.patch_changes import ChangeSpec
from app.patch_engine import PatchError, parse_patch_event


@pytest.fixture()
def pconn(tmp_path, monkeypatch):
    monkeypatch.setenv("CDCWATER_DB", str(tmp_path / "patch-test.sqlite3"))
    conn = store.connect()
    patch_store.init_patch_tables(conn)
    yield conn
    conn.close()


def ev(line_no, table, pk, minute, op="u", after=None, version=1, change_id=None,
       source=None):
    return parse_patch_event(
        {
            "table": table,
            "pk": pk,
            "version": version,
            "ts": f"2026-11-15T09:{minute:02d}:00Z",
            "op": op,
            "after": {} if after is None else after,
            "change_id": change_id,
        },
        source or f"{table}-lib.jsonl",
        line_no,
        line_no,
    )


def evv(line_no, table, pk, version, minute, op="u", after=None, change_id=None,
        source=None):
    """版本参数前置的构造器，避免与 (line_no, table, pk, minute) 位置混淆。"""
    return ev(line_no, table, pk, minute, op, after, version=version,
              change_id=change_id, source=source)


# --- 检查点续跑 -------------------------------------------------------------


def test_resume_skips_confirmed_events_and_does_not_double_apply(pconn):
    events = [
        evv(1, "orders", "O1", 1, 1, "c", {"amount": 10}),
        evv(2, "orders", "O1", 2, 2, "u", {"amount": 20}),
        evv(3, "orders", "O1", 3, 3, "u", {"amount": 30}),
    ]
    # 故障注入：处理完第 2 行中断
    with pytest.raises(PatchError):
        patch_runner.ingest_events(pconn, events, auto_publish=False, fail_after_line=2)

    cp = patch_store.get_checkpoint(pconn, "orders-lib.jsonl")
    assert cp["last_line_no"] == 2
    # 失败时没有任何发布
    assert patch_store.patch_counts(pconn)["published_changes"] == 0

    # 续跑：只处理第 3 行，前两行不重复
    result = patch_runner.ingest_events(pconn, events, auto_publish=False)
    assert result["archive"]["skipped"] == 3          # 归档幂等全部跳过
    staged = patch_store.fetch_area_rows(pconn, "staging")
    fields = {f["field"]: f for f in staged[("orders", "O1")]["fields"]}
    assert fields["amount"]["value"] == 30
    assert patch_store.get_checkpoint(pconn, "orders-lib.jsonl")["last_line_no"] == 3


def test_half_row_is_persisted_without_clearing_other_fields(pconn):
    events = [
        evv(1, "orders", "O2", 1, 1, "c", {"order_id": "O2", "amount": 100, "qty": 2}),
        evv(2, "orders", "O2", 2, 2, "u", {"qty": 4}),
    ]
    patch_runner.ingest_events(pconn, events, auto_publish=False)
    staged = patch_store.fetch_area_rows(pconn, "staging")
    fields = {f["field"]: f["value"] for f in staged[("orders", "O2")]["fields"]}
    assert fields == {"order_id": "O2", "amount": 100, "qty": 4}


# --- 跨表闸门 + 原子发布 -----------------------------------------------------


def test_cross_table_change_publishes_atomically_only_after_all_tables(pconn):
    manifests = {"CHG-1": ChangeSpec("CHG-1", ["orders", "customers"], "联动")}
    first = [ev(1, "orders", "O3", 1, "u", {"status": "paid"}, change_id="CHG-1")]
    patch_runner.ingest_events(pconn, first, manifests=manifests, auto_publish=False)
    changes = {c["change_id"]: c for c in patch_store.list_changes(pconn)}
    assert changes["CHG-1"]["status"] == "waiting"
    # 半到达：现网必须为空
    assert patch_store.fetch_area_rows(pconn, "live") == {}

    second = [ev(1, "customers", "C3", 2, "u", {"tier": "gold"}, change_id="CHG-1",
                 source="customers-lib.jsonl")]
    result = patch_runner.ingest_events(pconn, second, manifests=manifests)
    assert [p["change_id"] for p in result["publishes"]] == ["CHG-1"]
    live = patch_store.fetch_area_rows(pconn, "live")
    assert set(live) == {("orders", "O3"), ("customers", "C3")}
    assert patch_store.successful_publish_exists(pconn, "CHG-1")


def test_publish_records_before_after_field_values(pconn):
    # 两个版本归在同一变更：键首次进现网，字段记 inserted，after 取最终获胜版本
    events = [
        evv(1, "orders", "O4", 1, 1, "c", {"amount": 10}, change_id="CHG-BA"),
        evv(2, "orders", "O4", 2, 2, "u", {"amount": 25}, change_id="CHG-BA"),
    ]
    manifests = {"CHG-BA": ChangeSpec("CHG-BA", ["orders"], "前后值对照")}
    result = patch_runner.ingest_events(pconn, events, manifests=manifests)
    assert result["publishes"], "变更应自动发布"
    entry = next(p for p in result["publishes"] if p["change_id"] == "CHG-BA")
    amount = next(c for c in entry["field_changes"] if c["field"] == "amount")
    assert amount["kind"] == "inserted"
    assert amount["before_value"] is None and amount["after_value"] == 25


def test_publish_records_update_when_live_already_has_value(pconn):
    # 先发布 v1，再用第二个变更发布 v2：第二次发布出现真正的 updated 前后值
    patch_runner.ingest_events(
        pconn, [evv(1, "orders", "O4B", 1, 1, "c", {"amount": 10}, change_id="CHG-BA1")]
    )
    second = patch_runner.ingest_events(
        pconn, [evv(2, "orders", "O4B", 2, 2, "u", {"amount": 25}, change_id="CHG-BA2")]
    )
    entry = next(p for p in second["publishes"] if p["change_id"] == "CHG-BA2")
    change = next(c for c in entry["field_changes"]
                  if c["field"] == "amount" and c["kind"] == "updated")
    assert change["before_value"] == 10 and change["after_value"] == 25


def test_blocked_change_cannot_publish_and_live_keeps_last_version(pconn):
    events = [
        ev(1, "orders", "O5", 1, "c", {"amount": 100}, change_id="CHG-2"),
        ev(2, "orders", "O5", 1, "u", {"amount": 200}, change_id="CHG-2"),
    ]
    patch_runner.ingest_events(pconn, events, auto_publish=False)
    with pytest.raises(PatchError):
        patch_runner.publish_change(pconn, "CHG-2")
    # 现网未被污染，暂存保留先到值
    assert patch_store.fetch_area_rows(pconn, "live") == {}
    staged = patch_store.fetch_area_rows(pconn, "staging")
    amount = next(f for f in staged[("orders", "O5")]["fields"] if f["field"] == "amount")
    assert amount["value"] == 100


def test_adjudication_unblocks_and_publishes(pconn):
    events = [
        ev(1, "orders", "O6", 1, "c", {"amount": 100}, change_id="CHG-3"),
        ev(2, "orders", "O6", 1, "u", {"amount": 200}, change_id="CHG-3"),
    ]
    patch_runner.ingest_events(pconn, events, auto_publish=False)
    item_id = "orders|O6|amount|v1"
    out = patch_runner.adjudicate(pconn, item_id, "incoming", "采用新口径")
    assert out["adjudication"]["status"] == "resolved_incoming"
    assert [p["change_id"] for p in out["publishes"]] == ["CHG-3"]
    live = patch_store.fetch_area_rows(pconn, "live")
    amount = next(f for f in live[("orders", "O6")]["fields"] if f["field"] == "amount")
    assert amount["value"] == 200


# --- 幂等：重试不增加发布次数 ------------------------------------------------


def test_retry_does_not_increase_publish_count(pconn):
    events = [ev(1, "orders", "O7", 1, "c", {"amount": 1}, change_id="CHG-4")]
    first = patch_runner.ingest_events(pconn, events)
    assert len(first["publishes"]) == 1
    second = patch_runner.ingest_events(pconn, events)
    # 第二次没有新的成功发布
    assert second["publishes"] == [] or all(
        p["status"] == "skipped_duplicate" for p in second["publishes"]
    )
    successes = [p for p in patch_store.list_publish_log(pconn) if p["status"] == "success"]
    assert len(successes) == 1

    # 显式重发同一变更 → skipped_duplicate，仍不增加成功条数
    dup = patch_runner.publish_change(pconn, "CHG-4")
    assert dup["status"] == "skipped_duplicate"
    successes = [p for p in patch_store.list_publish_log(pconn) if p["status"] == "success"]
    assert len(successes) == 1


def test_tombstone_switch_and_resurrect_are_reflected_in_live(pconn):
    events = [
        evv(1, "inventory", "I8", 1, 1, "c", {"sku": "S8", "on_hand": 5}),
        evv(2, "inventory", "I8", 2, 2, "d", {}, change_id="CHG-D"),
        evv(3, "inventory", "I8", 1, 2, "u", {"on_hand": 1}),   # 墓碑后低版本忽略
        evv(4, "inventory", "I8", 3, 3, "c", {"sku": "S8", "on_hand": 9}, change_id="CHG-D"),
    ]
    patch_runner.ingest_events(pconn, events)
    live = patch_store.fetch_area_rows(pconn, "live")
    bucket = live[("inventory", "I8")]
    assert bucket["deleted"] is False            # 最终复活
    on_hand = next(f for f in bucket["fields"] if f["field"] == "on_hand")
    assert on_hand["value"] == 9


def test_pipeline_view_aggregates_provenance_and_compare(pconn):
    patch_runner.ingest_events(
        pconn, [ev(1, "orders", "O9", 1, "c", {"amount": 7}, change_id="CHG-5")]
    )
    view = patch_runner.pipeline_view(pconn)
    assert view["counts"]["events"] == 1
    compare = next(f for f in view["field_compare"] if f["pk"] == "O9" and f["field"] == "amount")
    assert compare["staging_value"] == 7 and compare["live_value"] == 7
    assert compare["staging_source"] == "orders-lib.jsonl"
    assert compare["drift"] is False
    assert view["checkpoints"][0]["last_line_no"] == 1


def test_unknown_change_publish_raises(pconn):
    with pytest.raises(PatchError):
        patch_runner.publish_change(pconn, "CHG-GHOST")


def test_rollback_on_publish_failure_keeps_live_untouched(pconn, monkeypatch):
    # 先不自动发布，保证后续手动发布真正进入事务
    patch_runner.ingest_events(
        pconn,
        [ev(1, "orders", "OA", 1, "c", {"amount": 1}, change_id="CHG-R")],
        auto_publish=False,
    )
    # 让发布事务内的一步抛错
    original = patch_runner.ps.upsert_key_state

    def boom(*args, **kwargs):
        raise RuntimeError("模拟现网写入失败")

    monkeypatch.setattr(patch_runner.ps, "upsert_key_state", boom)
    with pytest.raises(PatchError):
        patch_runner.publish_change(pconn, "CHG-R")
    monkeypatch.setattr(patch_runner.ps, "upsert_key_state", original)

    # 回滚后：变更未发布，且没有残留 failed 之后的脏现网
    changes = {c["change_id"]: c for c in patch_store.list_changes(pconn)}
    assert changes["CHG-R"]["status"] == "ready"
    # 重试可成功（现网保留上一版 = 空 → 正常写入）
    entry = patch_runner.publish_change(pconn, "CHG-R")
    assert entry["status"] == "success"
    assert ("orders", "OA") in patch_store.fetch_area_rows(pconn, "live")
