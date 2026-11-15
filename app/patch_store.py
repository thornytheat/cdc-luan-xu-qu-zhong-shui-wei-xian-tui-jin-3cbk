"""补丁流水线 · SQLite 持久化。

表设计（与既有 events/runs 并存，互不影响旧流水线）：

- patch_events      归档补丁事件，(source, line_no) 唯一 → 重复导入幂等
- patch_checkpoints 每来源「已确认位置」：失败续跑只处理之后的事件
- patch_manifests   业务变更清单（change_id → 预期表）
- patch_changes     变更登记：到表情况 / 触碰键 / 状态
- patch_staging     暂存字段值（字段级合并结果，待发布）
- patch_live        现网字段值（只有原子发布成功才会变；失败保留上一版）
- patch_adjudications 待裁决项（pending/已决）
- patch_publish_log 发布台账：每次原子切换一条；同 change_id 只允许一条成功记录
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from .patch_changes import ChangeSpec
from .patch_engine import PatchEvent

PATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS patch_events (
  source      TEXT    NOT NULL,
  line_no     INTEGER NOT NULL,
  seq         INTEGER NOT NULL,
  table_name  TEXT    NOT NULL,
  pk          TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  ts          TEXT    NOT NULL,
  op          TEXT    NOT NULL,
  after_json  TEXT    NOT NULL,
  change_id   TEXT,
  PRIMARY KEY (source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_patch_seq ON patch_events(seq);

CREATE TABLE IF NOT EXISTS patch_checkpoints (
  source       TEXT PRIMARY KEY,
  last_line_no INTEGER NOT NULL,
  last_seq     INTEGER NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS patch_manifests (
  change_id       TEXT PRIMARY KEY,
  expected_json   TEXT NOT NULL,
  label           TEXT NOT NULL,
  implicit        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS patch_changes (
  change_id       TEXT PRIMARY KEY,
  expected_json   TEXT NOT NULL,
  arrived_json    TEXT NOT NULL,
  keys_json       TEXT NOT NULL,
  status          TEXT NOT NULL,
  label           TEXT NOT NULL,
  implicit        INTEGER NOT NULL DEFAULT 0,
  updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS patch_staging (
  table_name  TEXT    NOT NULL,
  pk          TEXT    NOT NULL,
  field_name  TEXT    NOT NULL,
  value_json  TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  source      TEXT    NOT NULL,
  line_no     INTEGER NOT NULL,
  ts          TEXT    NOT NULL,
  PRIMARY KEY (table_name, pk, field_name)
);

CREATE TABLE IF NOT EXISTS patch_live (
  table_name  TEXT    NOT NULL,
  pk          TEXT    NOT NULL,
  field_name  TEXT    NOT NULL,
  value_json  TEXT    NOT NULL,
  version     INTEGER NOT NULL,
  source      TEXT    NOT NULL,
  line_no     INTEGER NOT NULL,
  ts          TEXT    NOT NULL,
  PRIMARY KEY (table_name, pk, field_name)
);

-- 删除态单独标记，字段行保留以便复活与审计
CREATE TABLE IF NOT EXISTS patch_key_state (
  table_name         TEXT NOT NULL,
  pk                 TEXT NOT NULL,
  area               TEXT NOT NULL,           -- staging / live
  deleted            INTEGER NOT NULL DEFAULT 0,
  tombstone_version  INTEGER,
  tombstone_line_no  INTEGER,
  tombstone_source   TEXT,
  PRIMARY KEY (table_name, pk, area)
);

CREATE TABLE IF NOT EXISTS patch_adjudications (
  item_id           TEXT PRIMARY KEY,
  table_name        TEXT NOT NULL,
  pk                TEXT NOT NULL,
  field_name        TEXT NOT NULL,
  version           INTEGER NOT NULL,
  change_id         TEXT,
  kept_json         TEXT NOT NULL,
  incoming_json     TEXT NOT NULL,
  kept_source       TEXT NOT NULL,
  kept_line_no      INTEGER NOT NULL,
  incoming_source   TEXT NOT NULL,
  incoming_line_no  INTEGER NOT NULL,
  status            TEXT NOT NULL,
  decided_at        TEXT,
  decision_note     TEXT,
  decided_json      TEXT
);

CREATE TABLE IF NOT EXISTS patch_publish_log (
  publish_id      TEXT PRIMARY KEY,
  change_id       TEXT NOT NULL,
  published_at    TEXT NOT NULL,
  status          TEXT NOT NULL,              -- success / failed / skipped_duplicate
  tables_json     TEXT NOT NULL,
  keys_json       TEXT NOT NULL,
  before_json     TEXT NOT NULL,              -- 切换前快照（回滚与前后值对照）
  after_json      TEXT NOT NULL,
  field_changes_json TEXT NOT NULL,
  reason          TEXT NOT NULL,
  UNIQUE(change_id, status)
);
CREATE INDEX IF NOT EXISTS idx_publish_change ON patch_publish_log(change_id);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def init_patch_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(PATCH_SCHEMA)
    conn.commit()


# --- 归档与检查点 -----------------------------------------------------------


def archive_patch_events(conn: sqlite3.Connection, events: list[PatchEvent]) -> dict:
    """幂等归档：(source, line_no) 冲突忽略；返回插入/跳过统计。"""
    inserted = skipped = 0
    for event in events:
        cursor = conn.execute(
            """INSERT OR IGNORE INTO patch_events
               (source, line_no, seq, table_name, pk, version, ts, op, after_json, change_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                event.source, event.line_no, event.seq, event.table, event.pk,
                event.version, event.ts_iso, event.op,
                json.dumps(event.after, ensure_ascii=False), event.change_id,
            ),
        )
        if cursor.rowcount:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    return {"attempted": len(events), "inserted": inserted, "skipped": skipped}


def get_checkpoint(conn: sqlite3.Connection, source: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM patch_checkpoints WHERE source=?", (source,)
    ).fetchone()
    if row is None:
        return None
    return {"source": row["source"], "last_line_no": row["last_line_no"], "last_seq": row["last_seq"]}


def advance_checkpoint(conn: sqlite3.Connection, source: str, line_no: int, seq: int) -> None:
    conn.execute(
        """INSERT INTO patch_checkpoints(source, last_line_no, last_seq, updated_at)
           VALUES (?,?,?,?)
           ON CONFLICT(source) DO UPDATE SET
             last_line_no=excluded.last_line_no,
             last_seq=excluded.last_seq,
             updated_at=excluded.updated_at""",
        (source, line_no, seq, now_iso()),
    )


def fetch_pending_events(
    conn: sqlite3.Connection, source: str | None = None
) -> list[PatchEvent]:
    """按检查点取「未确认」事件；无检查点的来源从头开始（续跑口径）。"""
    rows = conn.execute(
        "SELECT * FROM patch_events ORDER BY seq, source, line_no"
    ).fetchall()
    out: list[PatchEvent] = []
    checkpoints = {
        row["source"]: row["last_line_no"]
        for row in conn.execute("SELECT source, last_line_no FROM patch_checkpoints")
    }
    for row in rows:
        if source is not None and row["source"] != source:
            continue
        if row["line_no"] <= checkpoints.get(row["source"], 0):
            continue  # 已确认位置之前：续跑跳过，绝不重复处理
        out.append(_row_to_patch(row))
    return out


def fetch_all_patch_events(conn: sqlite3.Connection) -> list[PatchEvent]:
    rows = conn.execute("SELECT * FROM patch_events ORDER BY seq").fetchall()
    return [_row_to_patch(row) for row in rows]


def _row_to_patch(row: sqlite3.Row) -> PatchEvent:
    return PatchEvent(
        source=row["source"],
        line_no=int(row["line_no"]),
        seq=int(row["seq"]),
        table=row["table_name"],
        pk=row["pk"],
        version=int(row["version"]),
        ts=datetime.strptime(row["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc),
        op=row["op"],
        after=json.loads(row["after_json"]),
        change_id=row["change_id"],
    )


# --- 清单与变更登记 ---------------------------------------------------------


def upsert_manifest(conn: sqlite3.Connection, spec: ChangeSpec) -> None:
    conn.execute(
        """INSERT INTO patch_manifests(change_id, expected_json, label, implicit)
           VALUES (?,?,?,?)
           ON CONFLICT(change_id) DO UPDATE SET
             expected_json=excluded.expected_json,
             label=excluded.label,
             implicit=excluded.implicit""",
        (
            spec.change_id,
            json.dumps(spec.expected_tables, ensure_ascii=False),
            spec.label or "",
            1 if spec.implicit else 0,
        ),
    )
    conn.commit()


def load_manifests(conn: sqlite3.Connection) -> dict[str, ChangeSpec]:
    rows = conn.execute("SELECT * FROM patch_manifests").fetchall()
    return {
        row["change_id"]: ChangeSpec(
            change_id=row["change_id"],
            expected_tables=json.loads(row["expected_json"]),
            label=row["label"],
            implicit=bool(row["implicit"]),
        )
        for row in rows
    }


def save_change(
    conn: sqlite3.Connection, state: dict,
) -> None:
    conn.execute(
        """INSERT INTO patch_changes(change_id, expected_json, arrived_json, keys_json,
               status, label, implicit, updated_at)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(change_id) DO UPDATE SET
             expected_json=excluded.expected_json,
             arrived_json=excluded.arrived_json,
             keys_json=excluded.keys_json,
             status=excluded.status,
             label=excluded.label,
             implicit=excluded.implicit,
             updated_at=excluded.updated_at""",
        (
            state["change_id"],
            json.dumps(state["expected_tables"], ensure_ascii=False),
            json.dumps(state["arrived_tables"], ensure_ascii=False),
            json.dumps(state["touched_keys"], ensure_ascii=False),
            state["status"],
            state.get("label", ""),
            1 if state.get("implicit") else 0,
            now_iso(),
        ),
    )


def list_changes(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM patch_changes ORDER BY change_id"
    ).fetchall()
    return [_change_row(row) for row in rows]


def _change_row(row: sqlite3.Row) -> dict:
    expected = json.loads(row["expected_json"])
    arrived = json.loads(row["arrived_json"])
    return {
        "change_id": row["change_id"],
        "expected_tables": expected,
        "arrived_tables": arrived,
        "missing_tables": [t for t in expected if t not in arrived],
        "touched_keys": json.loads(row["keys_json"]),
        "status": row["status"],
        "label": row["label"],
        "implicit": bool(row["implicit"]),
        "updated_at": row["updated_at"],
    }


# --- 暂存 / 现网字段状态 -----------------------------------------------------


def replace_staging_field(
    conn: sqlite3.Connection, table: str, pk: str, field_name: str, provenance: dict
) -> None:
    conn.execute(
        """INSERT INTO patch_staging(table_name, pk, field_name, value_json, version,
               source, line_no, ts)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(table_name, pk, field_name) DO UPDATE SET
             value_json=excluded.value_json,
             version=excluded.version,
             source=excluded.source,
             line_no=excluded.line_no,
             ts=excluded.ts""",
        (
            table, pk, field_name,
            json.dumps(provenance["value"], ensure_ascii=False),
            provenance["version"], provenance["source"], provenance["line_no"],
            provenance["ts"],
        ),
    )


def upsert_key_state(
    conn: sqlite3.Connection, table: str, pk: str, area: str, state: dict
) -> None:
    conn.execute(
        """INSERT INTO patch_key_state(table_name, pk, area, deleted, tombstone_version,
               tombstone_line_no, tombstone_source)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(table_name, pk, area) DO UPDATE SET
             deleted=excluded.deleted,
             tombstone_version=excluded.tombstone_version,
             tombstone_line_no=excluded.tombstone_line_no,
             tombstone_source=excluded.tombstone_source""",
        (
            table, pk, area, 1 if state.get("deleted") else 0,
            state.get("tombstone_version"), state.get("tombstone_line_no"),
            state.get("tombstone_source"),
        ),
    )


def fetch_area_rows(conn: sqlite3.Connection, area: str) -> dict[tuple[str, str], dict]:
    """聚合某区域（staging/live）每个键的字段与墓碑，供发布/页面/回滚使用。"""
    table = "patch_staging" if area == "staging" else "patch_live"
    rows: dict[tuple[str, str], dict] = {}
    for row in conn.execute(f"SELECT * FROM {table} ORDER BY table_name, pk, field_name"):
        bucket = rows.setdefault(
            (row["table_name"], row["pk"]),
            {"table": row["table_name"], "pk": row["pk"], "fields": []},
        )
        bucket["fields"].append(
            {
                "field": row["field_name"],
                "value": json.loads(row["value_json"]),
                "version": row["version"],
                "source": row["source"],
                "line_no": row["line_no"],
                "ts": row["ts"],
            }
        )
    for row in conn.execute(
        "SELECT * FROM patch_key_state WHERE area=?", (area,)
    ):
        bucket = rows.setdefault(
            (row["table_name"], row["pk"]),
            {"table": row["table_name"], "pk": row["pk"], "fields": []},
        )
        bucket.update(
            {
                "deleted": bool(row["deleted"]),
                "tombstone_version": row["tombstone_version"],
                "tombstone_line_no": row["tombstone_line_no"],
                "tombstone_source": row["tombstone_source"],
            }
        )
    for bucket in rows.values():
        bucket.setdefault("deleted", False)
    return rows


# --- 待裁决项 ---------------------------------------------------------------


def save_adjudication(conn: sqlite3.Connection, item: dict) -> None:
    conn.execute(
        """INSERT INTO patch_adjudications(item_id, table_name, pk, field_name, version,
               change_id, kept_json, incoming_json, kept_source, kept_line_no,
               incoming_source, incoming_line_no, status, decided_at, decision_note, decided_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(item_id) DO UPDATE SET
             status=excluded.status,
             decided_at=excluded.decided_at,
             decision_note=excluded.decision_note,
             decided_json=excluded.decided_json""",
        (
            item["item_id"], item["table"], item["pk"], item["field"], item["version"],
            item["change_id"],
            json.dumps(item["kept_value"], ensure_ascii=False),
            json.dumps(item["incoming_value"], ensure_ascii=False),
            item["kept_source"], item["kept_line_no"],
            item["incoming_source"], item["incoming_line_no"],
            item["status"], item.get("decided_at"), item.get("decision_note"),
            json.dumps(item.get("decided_value"), ensure_ascii=False)
            if item.get("decided_value") is not None else None,
        ),
    )


def list_adjudications(conn: sqlite3.Connection, status: str | None = None) -> list[dict]:
    if status:
        rows = conn.execute(
            "SELECT * FROM patch_adjudications WHERE status=? ORDER BY item_id", (status,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM patch_adjudications ORDER BY status, item_id"
        ).fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "item_id": row["item_id"],
                "table": row["table_name"],
                "pk": row["pk"],
                "field": row["field_name"],
                "version": row["version"],
                "change_id": row["change_id"],
                "kept_value": json.loads(row["kept_json"]),
                "incoming_value": json.loads(row["incoming_json"]),
                "kept_source": row["kept_source"],
                "kept_line_no": row["kept_line_no"],
                "incoming_source": row["incoming_source"],
                "incoming_line_no": row["incoming_line_no"],
                "status": row["status"],
                "decided_at": row["decided_at"],
                "decision_note": row["decision_note"],
                "decided_value": json.loads(row["decided_json"])
                if row["decided_json"] is not None else None,
            }
        )
    return out


def get_adjudication(conn: sqlite3.Connection, item_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM patch_adjudications WHERE item_id=?", (item_id,)
    ).fetchone()
    if row is None:
        return None
    return _adj_row(row)


def _adj_row(row: sqlite3.Row) -> dict:
    return {
        "item_id": row["item_id"],
        "table": row["table_name"],
        "pk": row["pk"],
        "field": row["field_name"],
        "version": row["version"],
        "change_id": row["change_id"],
        "kept_value": json.loads(row["kept_json"]),
        "incoming_value": json.loads(row["incoming_json"]),
        "kept_source": row["kept_source"],
        "kept_line_no": row["kept_line_no"],
        "incoming_source": row["incoming_source"],
        "incoming_line_no": row["incoming_line_no"],
        "status": row["status"],
        "decided_at": row["decided_at"],
        "decision_note": row["decision_note"],
        "decided_value": json.loads(row["decided_json"])
        if row["decided_json"] is not None else None,
    }


# --- 发布台账 ---------------------------------------------------------------


def successful_publish_exists(conn: sqlite3.Connection, change_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM patch_publish_log WHERE change_id=? AND status='success' LIMIT 1",
        (change_id,),
    ).fetchone()
    return row is not None


def record_publish(conn: sqlite3.Connection, entry: dict) -> None:
    # UNIQUE(change_id, status) 兜底：同一变更同一状态只能落一条，重试不增加发布次数
    conn.execute(
        """INSERT OR IGNORE INTO patch_publish_log(publish_id, change_id, published_at,
               status, tables_json, keys_json, before_json, after_json,
               field_changes_json, reason)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            entry["publish_id"], entry["change_id"], entry["published_at"], entry["status"],
            json.dumps(entry["tables"], ensure_ascii=False),
            json.dumps(entry["key_refs"], ensure_ascii=False),
            json.dumps(entry["before"], ensure_ascii=False),
            json.dumps(entry["after"], ensure_ascii=False),
            json.dumps(entry["field_changes"], ensure_ascii=False),
            entry["reason"],
        ),
    )


def list_publish_log(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM patch_publish_log ORDER BY published_at DESC, publish_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "publish_id": row["publish_id"],
            "change_id": row["change_id"],
            "published_at": row["published_at"],
            "status": row["status"],
            "tables": json.loads(row["tables_json"]),
            "key_refs": json.loads(row["keys_json"]),
            "before": json.loads(row["before_json"]),
            "after": json.loads(row["after_json"]),
            "field_changes": json.loads(row["field_changes_json"]),
            "reason": row["reason"],
        }
        for row in rows
    ]


def patch_counts(conn: sqlite3.Connection) -> dict:
    def scalar(sql: str) -> int:
        return int(conn.execute(sql).fetchone()[0])

    return {
        "events": scalar("SELECT COUNT(*) FROM patch_events"),
        "staging_keys": scalar("SELECT COUNT(DISTINCT table_name || pk) FROM patch_staging"),
        "live_keys": scalar("SELECT COUNT(DISTINCT table_name || pk) FROM patch_live"),
        "pending_adjudications": scalar(
            "SELECT COUNT(*) FROM patch_adjudications WHERE status='pending'"
        ),
        "published_changes": scalar(
            "SELECT COUNT(*) FROM patch_publish_log WHERE status='success'"
        ),
    }


def reset_pipeline(conn: sqlite3.Connection) -> None:
    """清空补丁流水线全部表（演示重放用；旧 events/runs 不受影响）。"""
    for name in (
        "patch_events", "patch_checkpoints", "patch_manifests", "patch_changes",
        "patch_staging", "patch_live", "patch_key_state", "patch_adjudications",
        "patch_publish_log",
    ):
        conn.execute(f"DELETE FROM {name}")
    conn.commit()
