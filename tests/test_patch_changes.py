"""补丁流水线 · 跨表变更闸门（change_id 到齐度 + 待裁决阻塞，纯函数）。"""
from __future__ import annotations

from app.patch_changes import (
    BLOCKED,
    IMPLICIT_PREFIX,
    PUBLISHED,
    READY,
    WAITING,
    ChangeBook,
    ChangeSpec,
    implicit_change_id,
)
from app.patch_engine import Adjudication, parse_patch_event


def ev(line_no, table, pk, minute, change_id, version=1):
    return parse_patch_event(
        {
            "table": table,
            "pk": pk,
            "version": version,
            "ts": f"2026-11-15T09:{minute:02d}:00Z",
            "op": "u",
            "after": {"x": line_no},
            "change_id": change_id,
        },
        f"{table}-lib.jsonl",
        line_no,
        line_no,
    )


def pending_adj(table, pk, field="x", version=1):
    item_id = f"{table}|{pk}|{field}|v{version}"
    return Adjudication(
        item_id=item_id, table=table, pk=pk, field_name=field, version=version,
        change_id=None, kept_value=1, incoming_value=2,
        kept_source="a", kept_line_no=1, incoming_source="b", incoming_line_no=2,
    )


def test_half_arrived_change_stays_waiting():
    book = ChangeBook({"CHG-1": ChangeSpec("CHG-1", ["orders", "customers"], "联动")})
    book.register(ev(1, "orders", "O1", 1, "CHG-1"))
    book.refresh_status({})
    state = book.get("CHG-1")
    assert state.status == WAITING
    assert state.arrived_tables == {"orders"}
    assert state.to_dict()["missing_tables"] == ["customers"]
    assert book.ready_changes() == []


def test_change_becomes_ready_when_all_tables_arrive():
    book = ChangeBook({"CHG-1": ChangeSpec("CHG-1", ["orders", "customers"])})
    book.register(ev(1, "orders", "O1", 1, "CHG-1"))
    book.register(ev(2, "customers", "C1", 2, "CHG-1"))
    book.refresh_status({})
    assert book.get("CHG-1").status == READY
    assert {s.change_id for s in book.ready_changes()} == {"CHG-1"}


def test_pending_adjudication_on_touched_key_blocks_change():
    book = ChangeBook({"CHG-2": ChangeSpec("CHG-2", ["orders"])})
    book.register(ev(1, "orders", "O2", 1, "CHG-2"))
    adj = pending_adj("orders", "O2")
    book.refresh_status({adj.item_id: adj})
    assert book.get("CHG-2").status == BLOCKED


def test_adjudication_on_other_key_does_not_block():
    book = ChangeBook({"CHG-3": ChangeSpec("CHG-3", ["orders"])})
    book.register(ev(1, "orders", "O3", 1, "CHG-3"))
    adj = pending_adj("orders", "OTHER")
    book.refresh_status({adj.item_id: adj})
    assert book.get("CHG-3").status == READY


def test_published_change_stays_published_after_refresh():
    book = ChangeBook({"CHG-4": ChangeSpec("CHG-4", ["orders"])})
    book.register(ev(1, "orders", "O4", 1, "CHG-4"))
    book.refresh_status({}, published_changes={"CHG-4"})
    assert book.get("CHG-4").status == PUBLISHED
    # 再次刷新也不会退回 ready
    book.refresh_status({}, published_changes={"CHG-4"})
    assert book.get("CHG-4").status == PUBLISHED


def test_event_without_change_id_becomes_implicit_single_table_change():
    book = ChangeBook()
    event = ev(1, "inventory", "I9", 1, None)
    state = book.register(event)
    assert state.change_id.startswith(IMPLICIT_PREFIX)
    assert state.implicit is True
    assert state.expected_tables == ["inventory"]
    book.refresh_status({})
    assert state.status == READY  # 单表事件自身即到齐
    assert implicit_change_id("inventory", "I9", 1) == state.change_id


def test_to_dict_serializes_gate_view():
    book = ChangeBook({"CHG-5": ChangeSpec("CHG-5", ["orders", "customers"], "演示")})
    book.register(ev(1, "orders", "O5", 1, "CHG-5"))
    doc = book.get("CHG-5").to_dict()
    assert doc["change_id"] == "CHG-5"
    assert doc["label"] == "演示"
    assert doc["missing_tables"] == ["customers"]
    assert doc["status_label"] == "等待跨表到齐"
    assert doc["touched_keys"] == [{"table": "orders", "pk": "O5"}]
