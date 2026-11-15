"""补丁流水线 · 字段级合并引擎（纯函数，不依赖数据库）。

覆盖：半行补丁不误删字段、按字段版本覆盖、同值去重、同版本不同值隔离待裁决、
低版本丢弃、墓碑/忽略/复活、指纹幂等、人工裁决、更高版本让旧裁决项自动失效。
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.patch_engine import (
    Adjudication,
    PatchError,
    StagingArea,
    adjudication_key,
    event_fingerprint,
    parse_patch_event,
)


def ev(line_no, pk, version, minute, op="u", after=None, table="orders",
       change_id=None, source="orders-lib.jsonl"):
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
        source,
        line_no,
        line_no,
    )


def field_map(area, table, pk):
    return dict(area.keys[(table, pk)].fields)


# --- 半行补丁：未携带字段不能被清空 -----------------------------------------


def test_half_row_patch_keeps_uncarried_fields():
    area = StagingArea()
    area.apply(ev(1, "O1", 1, 1, "c", {"order_id": "O1", "amount": 100, "status": "pending"}))
    area.apply(ev(2, "O1", 2, 2, "u", {"status": "paid"}))  # 只带一个字段
    fields = field_map(area, "orders", "O1")
    assert fields["status"].value == "paid"
    assert fields["amount"].value == 100            # 未携带字段原样保留
    assert fields["order_id"].value == "O1"
    assert fields["status"].version == 2
    assert fields["amount"].version == 1            # 未携带字段版本不动
    assert area.stats.field_apply == 4
    assert area.stats.field_conflict == 0


def test_new_field_joins_without_touching_others():
    area = StagingArea()
    area.apply(ev(1, "O2", 1, 1, "c", {"amount": 10}))
    area.apply(ev(2, "O2", 2, 2, "u", {"note": "加急"}))
    fields = field_map(area, "orders", "O2")
    assert set(fields) == {"amount", "note"}
    assert fields["amount"].value == 10


# --- 字段版本 ---------------------------------------------------------------


def test_higher_field_version_overwrites_only_that_field():
    area = StagingArea()
    area.apply(ev(1, "O3", 1, 1, "c", {"amount": 10, "qty": 1}))
    area.apply(ev(2, "O3", 3, 2, "u", {"amount": 30}))
    fields = field_map(area, "orders", "O3")
    assert fields["amount"].value == 30 and fields["amount"].version == 3
    assert fields["qty"].value == 1 and fields["qty"].version == 1


def test_lower_field_version_is_superseded_and_changes_nothing():
    area = StagingArea()
    area.apply(ev(1, "O4", 2, 1, "c", {"amount": 20}))
    area.apply(ev(2, "O4", 1, 2, "u", {"amount": 5}))
    fields = field_map(area, "orders", "O4")
    assert fields["amount"].value == 20
    assert area.stats.field_superseded == 1
    rec = area.records[-1]
    assert rec.decision == "field_superseded" and rec.before_value == 20


def test_same_version_same_value_is_silent_dedupe():
    area = StagingArea()
    area.apply(ev(1, "O5", 1, 1, "c", {"amount": 10}))
    area.apply(ev(2, "O5", 1, 2, "u", {"amount": 10}))
    assert area.stats.field_dedupe == 1
    assert area.stats.field_conflict == 0
    assert field_map(area, "orders", "O5")["amount"].line_no == 1  # 来源坐标不变


def test_same_version_different_value_is_quarantined_and_keeps_first():
    area = StagingArea()
    area.apply(ev(1, "O6", 1, 1, "c", {"amount": 100, "qty": 1}))
    area.apply(ev(2, "O6", 1, 2, "u", {"amount": 200, "qty": 1}))
    assert area.stats.field_conflict == 1
    pending = area.pending_items()
    assert len(pending) == 1
    item = pending[0]
    assert item.field_name == "amount"
    assert item.kept_value == 100 and item.incoming_value == 200
    assert item.kept_line_no == 1 and item.incoming_line_no == 2
    # 先到值保留在暂存，不被后到覆盖
    assert field_map(area, "orders", "O6")["amount"].value == 100
    # 无冲突字段照常去重（字段按名排序：amount 冲突、qty 同值去重）
    decisions = {(r.field_name, r.decision) for r in area.records}
    assert ("qty", "field_dedupe") in decisions
    assert ("amount", "field_conflict") in decisions


def test_conflict_only_on_actually_differing_fields():
    area = StagingArea()
    area.apply(ev(1, "O7", 1, 1, "c", {"a": 1, "b": 2}))
    area.apply(ev(2, "O7", 1, 2, "u", {"a": 1, "b": 9}))
    assert area.stats.field_conflict == 1
    assert area.pending_items()[0].field_name == "b"


# --- 裁决 -------------------------------------------------------------------


def test_adjudicate_kept_vs_incoming():
    area = StagingArea()
    area.apply(ev(1, "O8", 1, 1, "c", {"amount": 100}))
    area.apply(ev(2, "O8", 1, 2, "u", {"amount": 200}))
    item_id = adjudication_key("orders", "O8", "amount", 1)

    area.adjudicate(item_id, "incoming", "采用后到", "2026-11-15T10:00:00Z")
    item = area.adjudications[item_id]
    assert item.status == "resolved_incoming" and item.decided_value == 200
    assert field_map(area, "orders", "O8")["amount"].value == 200
    assert field_map(area, "orders", "O8")["amount"].source == "orders-lib.jsonl"

    with pytest.raises(PatchError):
        area.adjudicate(item_id, "kept", None, "2026-11-15T10:01:00Z")  # 已结案
    with pytest.raises(PatchError):
        area.adjudicate("missing", "kept", None, "2026-11-15T10:01:00Z")


def test_higher_version_auto_supersedes_pending_item():
    area = StagingArea()
    area.apply(ev(1, "O9", 1, 1, "c", {"amount": 100}))
    area.apply(ev(2, "O9", 1, 2, "u", {"amount": 200}))
    item_id = adjudication_key("orders", "O9", "amount", 1)
    assert area.adjudications[item_id].status == "pending"

    area.apply(ev(3, "O9", 2, 3, "u", {"amount": 300}))
    assert area.adjudications[item_id].status == "auto_superseded"
    assert area.pending_items() == []
    assert field_map(area, "orders", "O9")["amount"].value == 300


# --- 墓碑 / 忽略 / 复活 -----------------------------------------------------


def test_tombstone_ignore_and_resurrect():
    area = StagingArea()
    area.apply(ev(1, "I1", 1, 1, "c", {"sku": "S1", "on_hand": 10}, table="inventory"))
    area.apply(ev(2, "I1", 2, 2, "u", {"on_hand": 8}, table="inventory"))
    area.apply(ev(3, "I1", 3, 3, "d", {}, table="inventory", change_id="CHG-D"))
    state = area.keys[("inventory", "I1")]
    assert state.deleted and state.tombstone_version == 3

    area.apply(ev(4, "I1", 2, 4, "u", {"on_hand": 5}, table="inventory"))  # 墓碑后低版本
    assert area.stats.ignored_after_tombstone == 1
    assert state.deleted is True

    area.apply(ev(5, "I1", 4, 5, "c", {"sku": "S1", "on_hand": 20}, table="inventory"))
    assert area.stats.resurrect == 1
    assert state.deleted is False
    fields = field_map(area, "inventory", "I1")
    assert fields["on_hand"].value == 20 and fields["sku"].value == "S1"


def test_duplicate_tombstone_is_deduped():
    area = StagingArea()
    area.apply(ev(1, "I2", 3, 1, "d", {}, table="inventory"))
    area.apply(ev(2, "I2", 3, 2, "d", {}, table="inventory"))
    assert area.stats.tombstone_apply == 1
    assert area.stats.tombstone_dedupe == 1


# --- 指纹幂等：重试同一补丁不重复计数 ---------------------------------------


def test_retry_same_patch_event_is_idempotent():
    area = StagingArea()
    first = ev(1, "O10", 1, 1, "c", {"amount": 1})
    area.apply(first)
    before = area.stats.as_dict()
    again = ev(1, "O10", 1, 1, "c", {"amount": 1})  # 同来源同行号同内容
    assert event_fingerprint(first) == event_fingerprint(again)
    area.apply(again)
    after = area.stats.as_dict()
    assert after["duplicate_events"] == 1
    assert after["field_apply"] == before["field_apply"]
    assert len(area.records) == 1  # 不重复产生结论


def test_parse_rejects_bad_events():
    with pytest.raises(PatchError):
        parse_patch_event({"version": 1, "ts": "2026-11-15T09:00:00Z",
                           "op": "u", "table": "orders"}, "s.jsonl", 1, 1)  # 缺 pk
    with pytest.raises(PatchError):
        parse_patch_event({"pk": "X", "version": 0, "ts": "2026-11-15T09:00:00Z",
                           "op": "u", "table": "orders"}, "s.jsonl", 1, 1)
    with pytest.raises(PatchError):
        parse_patch_event({"pk": "X", "version": 1, "ts": "bad",
                           "op": "u", "table": "orders"}, "s.jsonl", 1, 1)


def test_records_carry_provenance_and_before_after():
    area = StagingArea()
    area.apply(ev(1, "O11", 1, 1, "c", {"amount": 10}))
    area.apply(ev(2, "O11", 2, 2, "u", {"amount": 20}, change_id="CHG-X"))
    rec = area.records[-1].to_dict()
    assert rec["before_value"] == 10 and rec["after_value"] == 20
    assert rec["source"] == "orders-lib.jsonl" and rec["line_no"] == 2
    assert rec["change_id"] == "CHG-X"
    assert rec["decision_label"] == "字段写入"
