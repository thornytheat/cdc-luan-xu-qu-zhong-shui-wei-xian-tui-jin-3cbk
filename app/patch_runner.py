"""补丁流水线 · 编排：归档 → 检查点续跑 → 字段级合并 → 跨表闸门 → 原子发布。

事务口径（关键承诺）：
- 每条补丁「合并入暂存 + 推进检查点」在同一事务提交；中途失败则整笔回滚，
  检查点停在最后已确认位置，重试从下一事件续跑，已确认事件不会被重复处理。
- 发布在**一个 SQLite 事务**内把暂存区触碰键整体切换到现网（含墓碑），
  任一步失败全部回滚 → 现网保留上一版；成功才写发布台账并把变更置为 published。
- 发布台账对 (change_id, status) 唯一 + 成功存在性双重校验，
  重试同一变更不会产生第二条成功记录，发布次数不重复增加。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import patch_store as ps, settings
from .patch_changes import (
    ChangeBook,
    ChangeSpec,
    PUBLISHED,
    implicit_change_id,
)
from .patch_engine import (
    Adjudication,
    FieldState,
    PatchError,
    PatchEvent,
    StagingArea,
    adjudication_key,
    parse_patch_event,
)


def new_publish_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"P{stamp}-{uuid.uuid4().hex[:4].upper()}"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- 暂存区装载 / 落库 ------------------------------------------------------


def _load_staging(conn) -> StagingArea:
    """把 SQLite 里的暂存字段、墓碑、待裁决项还原成内存 StagingArea（续跑用）。"""
    area = StagingArea()
    staged = ps.fetch_area_rows(conn, "staging")
    for (table, pk), bucket in staged.items():
        state = area._key(table, pk)
        for item in bucket["fields"]:
            ts = datetime.strptime(item["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            state.fields[item["field"]] = FieldState(
                item["field"], item["value"], item["version"], item["source"],
                item["line_no"], -1, ts,
            )
        if bucket.get("deleted"):
            state.deleted = True
            state.tombstone_version = bucket.get("tombstone_version")
            state.tombstone_line_no = bucket.get("tombstone_line_no")
            state.tombstone_source = bucket.get("tombstone_source")
    for item in ps.list_adjudications(conn):
        adj = Adjudication(
            item_id=item["item_id"],
            table=item["table"],
            pk=item["pk"],
            field_name=item["field"],
            version=item["version"],
            change_id=item["change_id"],
            kept_value=item["kept_value"],
            incoming_value=item["incoming_value"],
            kept_source=item["kept_source"],
            kept_line_no=item["kept_line_no"],
            incoming_source=item["incoming_source"],
            incoming_line_no=item["incoming_line_no"],
            status=item["status"],
            decided_at=item["decided_at"],
            decision_note=item["decision_note"],
            decided_value=item["decided_value"],
        )
        area.adjudications[adj.item_id] = adj
    return area


def _persist_staging_delta(conn, area: StagingArea, touched: set[tuple[str, str]]) -> None:
    """把本轮触碰键的暂存状态整体写回（字段 + 墓碑 + 新增/变更裁决项）。"""
    for table, pk in sorted(touched):
        state = area.keys[(table, pk)]
        for fs in state.fields.values():
            ps.replace_staging_field(conn, table, pk, fs.name, fs.provenance())
        ps.upsert_key_state(
            conn, table, pk, "staging",
            {
                "deleted": state.deleted,
                "tombstone_version": state.tombstone_version,
                "tombstone_line_no": state.tombstone_line_no,
                "tombstone_source": state.tombstone_source,
            },
        )
    for item in area.adjudications.values():
        ps.save_adjudication(conn, item.to_dict())


# --- 变更簿装载 / 落库 ------------------------------------------------------


def _load_change_book(conn) -> ChangeBook:
    book = ChangeBook(ps.load_manifests(conn))
    for row in ps.list_changes(conn):
        state = book.changes.setdefault(
            row["change_id"],
            book.changes.get(row["change_id"])
            or _state_from_row(row, book.specs.get(row["change_id"])),
        )
        state.arrived_tables = set(row["arrived_tables"])
        state.touched_keys = {
            (item["table"], item["pk"]) if isinstance(item, dict) else (item[0], item[1])
            for item in row["touched_keys"]
        }
        state.status = row["status"]
    return book


def _state_from_row(row: dict, spec: ChangeSpec | None):
    from .patch_changes import ChangeState

    return ChangeState(
        change_id=row["change_id"],
        expected_tables=list(row["expected_tables"]),
        label=row.get("label") or (spec.label if spec else ""),
        implicit=row.get("implicit", False),
    )


def _save_change_book(conn, book: ChangeBook, published: set[str]) -> None:
    book.refresh_status(book_changes_adjudications(conn), published)
    for state in book.changes.values():
        ps.save_change(conn, state.to_dict())


def book_changes_adjudications(conn) -> dict[str, Adjudication]:
    """从库里的裁决项构造 Adjudication 映射，供 ChangeBook 判 blocked。"""
    out: dict[str, Adjudication] = {}
    for item in ps.list_adjudications(conn):
        out[item["item_id"]] = Adjudication(
            item_id=item["item_id"], table=item["table"], pk=item["pk"],
            field_name=item["field"], version=item["version"],
            change_id=item["change_id"], kept_value=item["kept_value"],
            incoming_value=item["incoming_value"], kept_source=item["kept_source"],
            kept_line_no=item["kept_line_no"], incoming_source=item["incoming_source"],
            incoming_line_no=item["incoming_line_no"], status=item["status"],
        )
    return out


# --- 摄入 -------------------------------------------------------------------


def ingest_events(
    conn,
    events: list[PatchEvent],
    manifests: dict[str, ChangeSpec] | None = None,
    auto_publish: bool = True,
    fail_after_line: int | None = None,
) -> dict:
    """摄入一批补丁事件（已 parse 校验）。返回处理统计与本轮发布结果。

    fail_after_line：演示用故障注入——处理到该来源行号后抛错（事务回滚），
    用于验证「检查点续跑 + 现网保留上一版」。
    """
    manifests = manifests or {}
    for spec in manifests.values():
        ps.upsert_manifest(conn, spec)

    archive_stats = ps.archive_patch_events(conn, events)
    area = _load_staging(conn)
    book = _load_change_book(conn)
    published = {
        row["change_id"]
        for row in ps.list_publish_log(conn)
        if row["status"] == "success"
    }

    checkpoints = {
        row["source"]: row["last_line_no"]
        for row in conn.execute("SELECT source, last_line_no FROM patch_checkpoints")
    }
    # 只处理「本批已归档且在检查点之后」的事件；按到达顺序（seq）处理
    pending = [
        event
        for event in sorted(events, key=lambda e: (e.seq, e.source, e.line_no))
        if event.line_no > checkpoints.get(event.source, 0)
    ]

    processed: list[dict] = []
    processed_seqs: set[int] = set()
    touched: set[tuple[str, str]] = set()
    injected: PatchError | None = None
    # 故障注入只锚定本批第一个来源，避免跨来源行号巧合误触发
    fail_source = pending[0].source if pending and fail_after_line is not None else None

    for event in pending:
        try:
            conn.execute("BEGIN")
            records = area.apply(event)
            touched.add((event.table, event.pk))
            change_state = book.register(event)
            _persist_staging_delta(conn, area, touched)
            book.refresh_status(
                {item.item_id: item for item in area.adjudications.values()}, published
            )
            ps.save_change(conn, change_state.to_dict())
            ps.advance_checkpoint(conn, event.source, event.line_no, event.seq)
            conn.commit()
        except Exception as exc:  # 整笔回滚：检查点不前进，现网/暂停留在上一版
            conn.rollback()
            raise PatchError(
                f"处理 {event.source} 第 {event.line_no} 行失败已回滚：{exc}；"
                "检查点保留在最后已确认位置，可从下一事件续跑"
            ) from exc
        processed.extend(rec.to_dict() for rec in records)
        processed_seqs.add(event.seq)

        if (
            fail_after_line is not None
            and event.source == fail_source
            and event.line_no >= fail_after_line
        ):
            injected = PatchError(
                f"故障注入：已在 {event.source} 第 {event.line_no} 行后中断，"
                "该位置之前的事件均已确认，重试将从下一事件续跑"
            )
            break

    # 刷新全部变更状态并落库（故障注入后也要提交，避免遗留事务锁库）
    _save_change_book(conn, book, published)
    conn.commit()

    publishes: list[dict] = []
    if auto_publish and injected is None:
        publishes = publish_ready(conn)

    result = {
        "archive": archive_stats,
        "processed_events": len(processed),
        "field_records": processed,
        "stats": area.stats.as_dict(),
        "checkpoints": [
            ps.get_checkpoint(conn, source)
            for source in sorted({e.source for e in events})
            if ps.get_checkpoint(conn, source) is not None
        ],
        "publishes": publishes,
        "remaining_pending": sum(1 for e in pending if e.seq not in processed_seqs),
    }
    if injected is not None:
        result["injected_failure"] = str(injected)
        raise injected
    return result


# --- 原子发布 ---------------------------------------------------------------


def _live_snapshot_for_keys(conn, keys: list[tuple[str, str]]) -> dict:
    """发布前抓取触碰键的现网值（before 对照与回滚依据）。"""
    live = ps.fetch_area_rows(conn, "live")
    return {
        f"{table}|{pk}": live.get((table, pk), {"table": table, "pk": pk, "fields": [],
                                                 "deleted": False})
        for table, pk in keys
    }


def publish_change(conn, change_id: str) -> dict:
    """把单个已就绪变更原子切换到现网；返回发布台账条目。"""
    rows = {row["change_id"]: row for row in ps.list_changes(conn)}
    change = rows.get(change_id)
    if change is None:
        raise PatchError(f"业务变更不存在：{change_id}")

    # 幂等：已成功发布过 → 不重复发布、不增加发布次数
    if ps.successful_publish_exists(conn, change_id):
        entry = _duplicate_entry(conn, change_id)
        return entry

    if change["status"] == "waiting":
        raise PatchError(
            f"变更 {change_id} 跨表结果未到齐（缺：{', '.join(change['missing_tables'])}），"
            "只到达一半的结果不能发布"
        )
    pending = {
        item["item_id"]
        for item in ps.list_adjudications(conn, status="pending")
        if (item["table"], item["pk"])
        in {
            (k["table"], k["pk"]) if isinstance(k, dict) else (k[0], k[1])
            for k in change["touched_keys"]
        }
    }
    if change["status"] == "blocked" or pending:
        raise PatchError(
            f"变更 {change_id} 还有 {len(pending)} 个待裁决字段，裁决完成后才能发布"
        )

    keys = sorted(
        (k["table"], k["pk"]) if isinstance(k, dict) else (k[0], k[1])
        for k in change["touched_keys"]
    )
    staged = ps.fetch_area_rows(conn, "staging")
    before = _live_snapshot_for_keys(conn, keys)

    conn.execute("BEGIN")
    try:
        field_changes = _apply_switch(conn, staged, keys, before)
        entry = {
            "publish_id": new_publish_id(),
            "change_id": change_id,
            "published_at": _now(),
            "status": "success",
            "tables": sorted({table for table, _ in keys}),
            "key_refs": [{"table": t, "pk": pk} for t, pk in keys],
            "before": before,
            "after": {f"{t}|{pk}": staged[(t, pk)] for t, pk in keys},
            "field_changes": field_changes,
            "reason": "暂存区触碰键整体原子切换到现网（单事务）",
        }
        ps.record_publish(conn, entry)
        # 变更置 published（同事务）
        current = rows[change_id]
        current["status"] = PUBLISHED
        ps.save_change(conn, current)
        conn.commit()
    except Exception as exc:
        conn.rollback()  # 现网完整保留上一版
        failed = {
            "publish_id": new_publish_id(),
            "change_id": change_id,
            "published_at": _now(),
            "status": "failed",
            "tables": sorted({table for table, _ in keys}),
            "key_refs": [{"table": t, "pk": pk} for t, pk in keys],
            "before": before,
            "after": {},
            "field_changes": [],
            "reason": f"发布失败已回滚，现网保留上一版：{exc}",
        }
        ps.record_publish(conn, failed)
        conn.commit()
        raise PatchError(failed["reason"]) from exc
    return entry


def _apply_switch(conn, staged: dict, keys: list[tuple[str, str]], before: dict) -> list[dict]:
    """在当前事务内把暂存值覆盖到现网（只动触碰键；其他现网键不受影响）。"""
    changes: list[dict] = []
    for table, pk in keys:
        bucket = staged.get((table, pk))
        if bucket is None:
            continue
        before_bucket = before[f"{table}|{pk}"]
        before_fields = {f["field"]: f for f in before_bucket.get("fields", [])}
        # 现网该键旧字段：先删除再按暂存重建，保证墓碑/字段移除与暂存一致
        conn.execute(
            "DELETE FROM patch_live WHERE table_name=? AND pk=?", (table, pk)
        )
        for item in bucket["fields"]:
            conn.execute(
                """INSERT INTO patch_live(table_name, pk, field_name, value_json, version,
                       source, line_no, ts)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    table, pk, item["field"],
                    json.dumps(item["value"], ensure_ascii=False),
                    item["version"], item["source"], item["line_no"], item["ts"],
                ),
            )
            old = before_fields.get(item["field"])
            changes.append(
                {
                    "table": table,
                    "pk": pk,
                    "field": item["field"],
                    "before_value": old["value"] if old else None,
                    "after_value": item["value"],
                    "before_source": old["source"] if old else None,
                    "after_source": item["source"],
                    "after_line_no": item["line_no"],
                    "version": item["version"],
                    "kind": "updated" if old else "inserted",
                }
            )
        # 暂存里已不存在于现网的字段（被整键重建覆盖）记录为 removed 对照
        for name, old in before_fields.items():
            if name not in {f["field"] for f in bucket["fields"]}:
                changes.append(
                    {
                        "table": table,
                        "pk": pk,
                        "field": name,
                        "before_value": old["value"],
                        "after_value": None,
                        "before_source": old["source"],
                        "after_source": None,
                        "after_line_no": None,
                        "version": None,
                        "kind": "removed",
                    }
                )
        ps.upsert_key_state(
            conn, table, pk, "live",
            {
                "deleted": bucket.get("deleted", False),
                "tombstone_version": bucket.get("tombstone_version"),
                "tombstone_line_no": bucket.get("tombstone_line_no"),
                "tombstone_source": bucket.get("tombstone_source"),
            },
        )
    return changes


def _duplicate_entry(conn, change_id: str) -> dict:
    logs = [
        row for row in ps.list_publish_log(conn)
        if row["change_id"] == change_id and row["status"] == "success"
    ]
    original = logs[0]
    return {
        "publish_id": original["publish_id"],
        "change_id": change_id,
        "published_at": original["published_at"],
        "status": "skipped_duplicate",
        "tables": original["tables"],
        "key_refs": original["key_refs"],
        "before": original["before"],
        "after": original["after"],
        "field_changes": original["field_changes"],
        "reason": "该变更已发布过，跳过重试且不增加发布次数",
    }


def publish_ready(conn) -> list[dict]:
    """发布所有 ready 变更；waiting/blocked 原样保留。返回本轮成功/跳过条目。"""
    out: list[dict] = []
    for row in ps.list_changes(conn):
        if row["status"] == "ready":
            out.append(publish_change(conn, row["change_id"]))
    return out


# --- 裁决 -------------------------------------------------------------------


def adjudicate(conn, item_id: str, choose: str, note: str | None = None) -> dict:
    """人工裁决后重算变更闸门；若相关变更因此到齐且无阻塞，立即补发布。"""
    area = _load_staging(conn)
    item = area.adjudications.get(item_id)
    if item is None:
        raise PatchError(f"待裁决项不存在：{item_id}")
    area.adjudicate(item_id, choose, note, _now())
    state = area.keys[(item.table, item.pk)]
    _persist_staging_delta(conn, area, {(item.table, item.pk)})
    ps.save_adjudication(conn, area.adjudications[item_id].to_dict())
    conn.commit()

    published = {
        row["change_id"]
        for row in ps.list_publish_log(conn)
        if row["status"] == "success"
    }
    book = _load_change_book(conn)
    _save_change_book(conn, book, published)
    conn.commit()

    publishes = publish_ready(conn)
    return {
        "adjudication": ps.get_adjudication(conn, item_id),
        "publishes": publishes,
    }


# --- 页面聚合视图 ------------------------------------------------------------


def pipeline_view(conn) -> dict:
    """结果页一次性聚合：现网/暂存对照、待裁决、变更闸门、发布台账、检查点。"""
    staging = ps.fetch_area_rows(conn, "staging")
    live = ps.fetch_area_rows(conn, "live")
    changes = ps.list_changes(conn)
    adjudications = ps.list_adjudications(conn)
    publishes = ps.list_publish_log(conn)
    checkpoints = [
        dict(row) for row in conn.execute(
            "SELECT * FROM patch_checkpoints ORDER BY source"
        ).fetchall()
    ]
    events = ps.fetch_all_patch_events(conn)

    # 字段级现网/暂存对照（来源、版本、前后值）
    all_keys = sorted(set(staging) | set(live))
    field_compare = []
    for table, pk in all_keys:
        s_bucket = staging.get((table, pk), {"fields": [], "deleted": False})
        l_bucket = live.get((table, pk), {"fields": [], "deleted": False})
        s_fields = {f["field"]: f for f in s_bucket.get("fields", [])}
        l_fields = {f["field"]: f for f in l_bucket.get("fields", [])}
        for name in sorted(set(s_fields) | set(l_fields)):
            s, l = s_fields.get(name), l_fields.get(name)
            field_compare.append(
                {
                    "table": table,
                    "pk": pk,
                    "field": name,
                    "staging_value": s["value"] if s else None,
                    "staging_version": s["version"] if s else None,
                    "staging_source": s["source"] if s else None,
                    "staging_line_no": s["line_no"] if s else None,
                    "live_value": l["value"] if l else None,
                    "live_version": l["version"] if l else None,
                    "live_source": l["source"] if l else None,
                    "live_line_no": l["line_no"] if l else None,
                    "drift": (l is None)
                    or (s is not None and _json(s["value"]) != _json(l["value"])),
                }
            )
    return {
        "counts": ps.patch_counts(conn),
        "events": [
            {
                "source": e.source,
                "line_no": e.line_no,
                "seq": e.seq,
                "table": e.table,
                "pk": e.pk,
                "version": e.version,
                "ts": e.ts_iso,
                "op": e.op,
                "change_id": e.change_id,
                "after": e.after,
            }
            for e in events
        ],
        "staging": [staging[k] for k in sorted(staging)],
        "live": [live[k] for k in sorted(live)],
        "field_compare": field_compare,
        "changes": changes,
        "adjudications": adjudications,
        "publishes": publishes,
        "checkpoints": checkpoints,
    }


def _json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# --- 样例补丁装载 ------------------------------------------------------------


def load_manifest(path: Path | None = None) -> dict[str, ChangeSpec]:
    """读取业务变更清单；文件缺失时返回空（隐式变更仍可工作）。"""
    path = path or settings.PATCH_MANIFEST
    if not path.exists():
        return {}
    doc = json.loads(path.read_text(encoding="utf-8"))
    return {
        item["change_id"]: ChangeSpec(
            change_id=item["change_id"],
            expected_tables=list(item["expected_tables"]),
            label=item.get("label", ""),
        )
        for item in doc.get("changes", [])
    }


def load_sample_patches(patch_dir: Path | None = None) -> list[PatchEvent]:
    """按 PATCH_SOURCES 顺序读取样例补丁，seq 全局递增表达跨来源到达顺序。"""
    patch_dir = patch_dir or settings.PATCH_DIR
    events: list[PatchEvent] = []
    seq = 0
    for item in settings.PATCH_SOURCES:
        path = patch_dir / item["source"]
        if not path.exists():
            raise PatchError(f"缺少样例补丁文件：{path}")
        with path.open("r", encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, start=1):
                text = raw.strip()
                if not text:
                    continue
                seq += 1
                events.append(
                    parse_patch_event(json.loads(text), item["source"], line_no, seq)
                )
    return events


def seed_sample_patches(
    conn, auto_publish: bool = True, fail_after_line: int | None = None
) -> dict:
    """幂等导入样例补丁并跑完整流水线；已归档事件不会重复处理。"""
    manifests = load_manifest()
    events = load_sample_patches()
    return ingest_events(
        conn,
        events,
        manifests=manifests,
        auto_publish=auto_publish,
        fail_after_line=fail_after_line,
    )
