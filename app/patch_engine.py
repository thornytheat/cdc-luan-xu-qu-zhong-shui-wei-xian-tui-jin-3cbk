"""补丁流水线 · 领域引擎（纯函数，不依赖数据库与 Web）。

解决的问题：订单 / 客户 / 库存补丁来自不同库，整行覆盖会把后续补丁**没有携带**的
字段清空（误删）。本引擎把合并粒度下沉到**业务键 + 字段版本**：

- 每个 (table, pk, field) 独立保留「获胜版本」；补丁只覆盖它带来的字段，未携带字段不动。
- 同字段新版本 > 已获胜版本 → 覆盖（完整的新补丁尽快反映）。
- 同字段同版本且内容相同 → 静默去重（重试同一补丁不产生任何新结论）。
- 同字段同版本但内容不同 → 隔离为待裁决项，**先到值保留在现网/暂存，不被覆盖**。
- 同字段更低版本 → 丢弃，不改任何状态。
- op=d 的高版本墓碑让整个键进入删除态；墓碑后低版本事件一律忽略；严格更高版本复活。

跨表原子性由 change_id（业务变更号）承担：同一 change_id 的全部表都核对完成、
且没有未裁决项时才允许发布；只到达一半的跨表结果一直停在暂存区，不进现网。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Literal

# --- 口径常量 ---------------------------------------------------------------

# 单条字段结论（对外契约，页面/测试/快照都依赖这些字面量）
FieldDecision = Literal[
    "field_apply",          # 新字段或更高版本：写入暂存
    "field_dedupe",         # 同版本同值：静默去重
    "field_conflict",       # 同版本不同值：隔离待裁决，保留先到
    "field_superseded",     # 更低版本：丢弃
    "tombstone_apply",      # 墓碑生效，键转删除态
    "tombstone_dedupe",     # 同版本重复墓碑：去重
    "resurrect",            # 严格更高版本让删除态键复活
    "ignored_after_tombstone",  # 墓碑后低版本：忽略
]

FIELD_DECISION_LABELS = {
    "field_apply": "字段写入",
    "field_dedupe": "同值去重",
    "field_conflict": "字段冲突隔离",
    "field_superseded": "低版本丢弃",
    "tombstone_apply": "墓碑生效",
    "tombstone_dedupe": "墓碑去重",
    "resurrect": "墓碑后复活",
    "ignored_after_tombstone": "墓碑后忽略",
}


class PatchError(ValueError):
    """补丁事件非法（行列位置明确，路由转 4xx）。"""


# --- 补丁事件 ---------------------------------------------------------------


@dataclass(frozen=True)
class PatchEvent:
    source: str           # 来源库文件，如 orders-lib.jsonl
    line_no: int          # 文件行号（从 1 开始），所有结论的定位坐标
    seq: int              # 全局到达顺序（同一来源内等于行号，跨来源由编排序保证唯一）
    table: str
    pk: str
    version: int
    ts: datetime
    op: str               # c / u / d
    after: dict
    change_id: str | None = None

    @property
    def ts_iso(self) -> str:
        return self.ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    @property
    def is_delete(self) -> bool:
        return self.op == "d"


def _parse_ts(raw: object) -> datetime:
    from datetime import timezone

    if not isinstance(raw, str) or not raw.strip():
        raise PatchError("ts 必须是非空 ISO 时间字符串")
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise PatchError(f"ts 无法解析为 ISO 时间：{raw}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_patch_event(obj: object, source: str, line_no: int, seq: int) -> PatchEvent:
    """解析并校验一条补丁事件；`where` 口径与 archive 保持一致，错误信息带来源+行号。"""
    where = f"{source} 第 {line_no} 行"
    if not isinstance(obj, dict):
        raise PatchError(f"{where}：补丁事件必须是 JSON 对象")
    for name in ("table", "pk", "version", "ts", "op"):
        if name not in obj:
            raise PatchError(f"{where}：缺少字段 {name}")

    table = obj["table"]
    if not isinstance(table, str) or not table.strip():
        raise PatchError(f"{where}：table 必须是非空字符串")
    pk = obj["pk"]
    if not isinstance(pk, str) or not pk.strip():
        raise PatchError(f"{where}：pk 必须是非空字符串")
    version = obj["version"]
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise PatchError(f"{where}：version 必须是 >=1 的整数")
    op = obj["op"]
    if op not in ("c", "u", "d"):
        raise PatchError(f"{where}：op 必须是 c/u/d 之一，收到 {op!r}")
    after = obj.get("after", {})
    if after is None:
        after = {}
    if not isinstance(after, dict):
        raise PatchError(f"{where}：after 必须是对象")
    change_id = obj.get("change_id")
    if change_id is not None and (not isinstance(change_id, str) or not change_id.strip()):
        raise PatchError(f"{where}：change_id 必须是非空字符串或省略")
    return PatchEvent(
        source=source,
        line_no=line_no,
        seq=seq,
        table=table.strip(),
        pk=pk.strip(),
        version=version,
        ts=_parse_ts(obj["ts"]),
        op=op,
        after=after,
        change_id=change_id.strip() if isinstance(change_id, str) else None,
    )


# --- 暂存状态 ---------------------------------------------------------------


def canonical(value: object) -> str:
    """值归一化：同值判定只看 JSON 语义（键序不影响）。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass
class FieldState:
    """单个字段在暂存区的获胜版本与来源坐标。"""

    name: str
    value: object
    version: int
    source: str
    line_no: int
    seq: int
    ts: datetime

    def provenance(self) -> dict:
        return {
            "field": self.name,
            "value": self.value,
            "version": self.version,
            "source": self.source,
            "line_no": self.line_no,
            "ts": self.ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


@dataclass
class KeyState:
    """一个业务键的暂存状态：字段级获胜值 + 墓碑 + 已见事件指纹。"""

    table: str
    pk: str
    fields: dict[str, FieldState] = field(default_factory=dict)
    deleted: bool = False
    tombstone_version: int | None = None
    tombstone_line_no: int | None = None
    tombstone_source: str | None = None
    # 已处理事件指纹 → 字段级结论；重试同一补丁时直接复用，结论与计数不重复
    seen: dict[str, "FieldRecord"] = field(default_factory=dict)


@dataclass
class FieldRecord:
    """一条字段级结论（变更日志的最小单元，全部带定位坐标与前后值）。"""

    seq: int
    source: str
    line_no: int
    table: str
    pk: str
    field_name: str
    version: int
    decision: str
    change_id: str | None
    ts: str
    before_value: object = None
    after_value: object = None
    kept_value: object = None          # 冲突时先到（保留）值
    incoming_value: object = None      # 冲突时后到（隔离）值
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "seq": self.seq,
            "source": self.source,
            "line_no": self.line_no,
            "table": self.table,
            "pk": self.pk,
            "field": self.field_name,
            "version": self.version,
            "decision": self.decision,
            "decision_label": FIELD_DECISION_LABELS.get(self.decision, self.decision),
            "change_id": self.change_id,
            "ts": self.ts,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "kept_value": self.kept_value,
            "incoming_value": self.incoming_value,
            "reason": self.reason,
        }


@dataclass
class Adjudication:
    """同字段同版本不同值 → 隔离待裁决；先到值保留，后到值挂起。"""

    item_id: str
    table: str
    pk: str
    field_name: str
    version: int
    change_id: str | None
    kept_value: object
    incoming_value: object
    kept_source: str
    kept_line_no: int
    incoming_source: str
    incoming_line_no: int
    status: str = "pending"            # pending / resolved_kept / resolved_incoming / auto_superseded
    decided_at: str | None = None
    decision_note: str | None = None
    decided_value: object = None

    def to_dict(self) -> dict:
        return {
            "item_id": self.item_id,
            "table": self.table,
            "pk": self.pk,
            "field": self.field_name,
            "version": self.version,
            "change_id": self.change_id,
            "status": self.status,
            "kept_value": self.kept_value,
            "incoming_value": self.incoming_value,
            "kept_source": self.kept_source,
            "kept_line_no": self.kept_line_no,
            "incoming_source": self.incoming_source,
            "incoming_line_no": self.incoming_line_no,
            "decided_at": self.decided_at,
            "decision_note": self.decision_note,
            "decided_value": self.decided_value,
        }


def adjudication_key(table: str, pk: str, field_name: str, version: int) -> str:
    return f"{table}|{pk}|{field_name}|v{version}"


def event_fingerprint(event: PatchEvent) -> str:
    """补丁幂等键：来源 + 行号 + 版本 + op + 归一化内容。

    同一来源同一行号重试 → 同一指纹 → 直接复用旧结论，不重复计数/发布。
    """
    return "|".join(
        [event.source, str(event.line_no), str(event.version), event.op, canonical(event.after)]
    )


# --- 核心合并 ---------------------------------------------------------------


@dataclass
class MergeStats:
    events: int = 0
    duplicate_events: int = 0          # 整条补丁重试（指纹命中）
    field_apply: int = 0
    field_dedupe: int = 0
    field_conflict: int = 0
    field_superseded: int = 0
    tombstone_apply: int = 0
    tombstone_dedupe: int = 0
    resurrect: int = 0
    ignored_after_tombstone: int = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


class StagingArea:
    """内存态暂存区：合并只发生在这里；现网切换由 publish 模块在一个事务里完成。"""

    def __init__(self) -> None:
        self.keys: dict[tuple[str, str], KeyState] = {}
        self.records: list[FieldRecord] = []
        self.adjudications: dict[str, Adjudication] = {}
        self.stats = MergeStats()

    def _key(self, table: str, pk: str) -> KeyState:
        state = self.keys.get((table, pk))
        if state is None:
            state = KeyState(table=table, pk=pk)
            self.keys[(table, pk)] = state
        return state

    def _add_record(self, record: FieldRecord) -> None:
        self.records.append(record)

    def _conflict(
        self, event: PatchEvent, field_name: str, current: FieldState, incoming: object
    ) -> FieldRecord:
        """同版本不同值：挂待裁决项（已存在则复用，不重复制造冲突）。"""
        item_id = adjudication_key(event.table, event.pk, field_name, event.version)
        item = self.adjudications.get(item_id)
        if item is None:
            item = Adjudication(
                item_id=item_id,
                table=event.table,
                pk=event.pk,
                field_name=field_name,
                version=event.version,
                change_id=event.change_id,
                kept_value=current.value,
                incoming_value=incoming,
                kept_source=current.source,
                kept_line_no=current.line_no,
                incoming_source=event.source,
                incoming_line_no=event.line_no,
            )
            self.adjudications[item_id] = item
            self.stats.field_conflict += 1
        record = FieldRecord(
            seq=event.seq,
            source=event.source,
            line_no=event.line_no,
            table=event.table,
            pk=event.pk,
            field_name=field_name,
            version=event.version,
            decision="field_conflict",
            change_id=event.change_id,
            ts=event.ts_iso,
            before_value=current.value,
            kept_value=current.value,
            incoming_value=incoming,
            reason=(
                f"字段 {field_name} 在 v{event.version} 出现两个不同值："
                f"{current.source} 第 {current.line_no} 行={canonical(current.value)}，"
                f"{event.source} 第 {event.line_no} 行={canonical(incoming)}；隔离待裁决，先到值保留"
            ),
        )
        return record

    def _merge_content_event(self, state: KeyState, event: PatchEvent) -> list[FieldRecord]:
        """c/u 事件：逐字段按版本合并，未携带字段一律不动（这是不被整行清空的关键）。"""
        out: list[FieldRecord] = []
        for field_name in sorted(event.after):
            incoming = event.after[field_name]
            current = state.fields.get(field_name)

            if current is None:
                state.fields[field_name] = FieldState(
                    field_name, incoming, event.version, event.source, event.line_no,
                    event.seq, event.ts,
                )
                self.stats.field_apply += 1
                out.append(FieldRecord(
                    seq=event.seq, source=event.source, line_no=event.line_no,
                    table=event.table, pk=event.pk, field_name=field_name,
                    version=event.version, decision="field_apply",
                    change_id=event.change_id, ts=event.ts_iso,
                    before_value=None, after_value=incoming,
                    reason=f"字段 {field_name} 首见，写入 v{event.version}（来源 {event.source}）",
                ))
                continue

            if event.version > current.version:
                before = current.value
                state.fields[field_name] = FieldState(
                    field_name, incoming, event.version, event.source, event.line_no,
                    event.seq, event.ts,
                )
                self.stats.field_apply += 1
                # 更高版本自然取代旧版本上的待裁决项：标记自动失效，不再阻塞发布
                item_id = adjudication_key(event.table, event.pk, field_name, current.version)
                item = self.adjudications.get(item_id)
                if item is not None and item.status == "pending":
                    item.status = "auto_superseded"
                    item.decision_note = f"更高版本 v{event.version} 到达，v{current.version} 裁决项自动失效"
                    item.decided_at = event.ts_iso
                out.append(FieldRecord(
                    seq=event.seq, source=event.source, line_no=event.line_no,
                    table=event.table, pk=event.pk, field_name=field_name,
                    version=event.version, decision="field_apply",
                    change_id=event.change_id, ts=event.ts_iso,
                    before_value=before, after_value=incoming,
                    reason=f"v{event.version} > 已获胜 v{current.version}，覆盖字段 {field_name}（其他字段不动）",
                ))
            elif event.version == current.version:
                if canonical(incoming) == canonical(current.value):
                    self.stats.field_dedupe += 1
                    out.append(FieldRecord(
                        seq=event.seq, source=event.source, line_no=event.line_no,
                        table=event.table, pk=event.pk, field_name=field_name,
                        version=event.version, decision="field_dedupe",
                        change_id=event.change_id, ts=event.ts_iso,
                        before_value=current.value, after_value=incoming,
                        reason=f"字段 {field_name} v{event.version} 同值重复，静默去重",
                    ))
                else:
                    out.append(self._conflict(event, field_name, current, incoming))
            else:
                self.stats.field_superseded += 1
                out.append(FieldRecord(
                    seq=event.seq, source=event.source, line_no=event.line_no,
                    table=event.table, pk=event.pk, field_name=field_name,
                    version=event.version, decision="field_superseded",
                    change_id=event.change_id, ts=event.ts_iso,
                    before_value=current.value, incoming_value=incoming,
                    reason=(
                        f"字段 {field_name} 携带 v{event.version} 低于已获胜 v{current.version}"
                        f"（{current.source} 第 {current.line_no} 行），丢弃且不改状态"
                    ),
                ))
        return out

    def apply(self, event: PatchEvent) -> list[FieldRecord]:
        """处理一条补丁事件，返回字段级结论；重复事件直接复用历史结论。"""
        self.stats.events += 1
        state = self._key(event.table, event.pk)
        fingerprint = event_fingerprint(event)
        cached = state.seen.get(fingerprint)
        if cached is not None:
            self.stats.duplicate_events += 1
            # 复用同一结论（同 seq 坐标），不新增任何计数
            return [cached]

        records: list[FieldRecord]
        if event.is_delete:
            records = self._apply_delete(state, event)
        else:
            if state.deleted:
                if event.version > (state.tombstone_version or 0):
                    records = self._resurrect(state, event)
                else:
                    self.stats.ignored_after_tombstone += 1
                    records = [FieldRecord(
                        seq=event.seq, source=event.source, line_no=event.line_no,
                        table=event.table, pk=event.pk, field_name="*",
                        version=event.version, decision="ignored_after_tombstone",
                        change_id=event.change_id, ts=event.ts_iso,
                        reason=(
                            f"键处于墓碑 v{state.tombstone_version} 之后，本条 v{event.version}"
                            " 不高于墓碑版本，整条忽略且不改状态"
                        ),
                    )]
            else:
                records = self._merge_content_event(state, event)

        state.seen[fingerprint] = records[0]
        self.records.extend(records)
        return records

    def _apply_delete(self, state: KeyState, event: PatchEvent) -> list[FieldRecord]:
        if state.deleted:
            if event.version == state.tombstone_version:
                self.stats.tombstone_dedupe += 1
                return [FieldRecord(
                    seq=event.seq, source=event.source, line_no=event.line_no,
                    table=event.table, pk=event.pk, field_name="*",
                    version=event.version, decision="tombstone_dedupe",
                    change_id=event.change_id, ts=event.ts_iso,
                    reason=f"墓碑 v{event.version} 重复到达，静默去重",
                )]
            if event.version < state.tombstone_version:  # type: ignore[operator]
                self.stats.ignored_after_tombstone += 1
                return [FieldRecord(
                    seq=event.seq, source=event.source, line_no=event.line_no,
                    table=event.table, pk=event.pk, field_name="*",
                    version=event.version, decision="ignored_after_tombstone",
                    change_id=event.change_id, ts=event.ts_iso,
                    reason=f"墓碑 v{state.tombstone_version} 已存在，更低 v{event.version} 删除事件忽略",
                )]
        # 更高版本（或首见）墓碑：转删除态，暂存字段保留用于审计与复活，不物理清空
        state.deleted = True
        state.tombstone_version = event.version
        state.tombstone_line_no = event.line_no
        state.tombstone_source = event.source
        self.stats.tombstone_apply += 1
        return [FieldRecord(
            seq=event.seq, source=event.source, line_no=event.line_no,
            table=event.table, pk=event.pk, field_name="*",
            version=event.version, decision="tombstone_apply",
            change_id=event.change_id, ts=event.ts_iso,
            reason=f"删除事件 v{event.version} 生效，键转墓碑态（暂存字段保留待更高版本复活）",
        )]

    def _resurrect(self, state: KeyState, event: PatchEvent) -> list[FieldRecord]:
        """严格更高版本内容事件到达墓碑键：复活，并按字段合并新内容。"""
        state.deleted = False
        state.tombstone_version = None
        state.tombstone_line_no = None
        state.tombstone_source = None
        self.stats.resurrect += 1
        records = [FieldRecord(
            seq=event.seq, source=event.source, line_no=event.line_no,
            table=event.table, pk=event.pk, field_name="*",
            version=event.version, decision="resurrect",
            change_id=event.change_id, ts=event.ts_iso,
            reason=f"v{event.version} 严格高于墓碑版本，键复活并按字段合并本次补丁",
        )]
        records.extend(self._merge_content_event(state, event))
        return records

    # --- 裁决 ---------------------------------------------------------------

    def adjudicate(self, item_id: str, choose: str, note: str | None, now_iso: str) -> Adjudication:
        """人工裁决：choose=kept 保留先到值；choose=incoming 采用后到值并改写暂存。"""
        item = self.adjudications.get(item_id)
        if item is None:
            raise PatchError(f"待裁决项不存在：{item_id}")
        if item.status != "pending":
            raise PatchError(f"待裁决项 {item_id} 已结案（{item.status}），不能重复裁决")
        if choose not in ("kept", "incoming"):
            raise PatchError("choose 必须是 kept 或 incoming")
        state = self.keys[(item.table, item.pk)]
        if choose == "incoming":
            current = state.fields.get(item.field_name)
            item.kept_value = current.value if current else item.kept_value
            state.fields[item.field_name] = FieldState(
                item.field_name, item.incoming_value, item.version,
                item.incoming_source, item.incoming_line_no, -1,
                datetime.strptime(now_iso, "%Y-%m-%dT%H:%M:%SZ"),
            )
            item.status = "resolved_incoming"
            item.decided_value = item.incoming_value
        else:
            item.status = "resolved_kept"
            item.decided_value = item.kept_value
        item.decided_at = now_iso
        item.decision_note = note
        return item

    # --- 快照视图 ------------------------------------------------------------

    def staged_row(self, table: str, pk: str) -> dict:
        state = self.keys[(table, pk)]
        return {
            "table": table,
            "pk": pk,
            "deleted": state.deleted,
            "tombstone_version": state.tombstone_version,
            "tombstone_line_no": state.tombstone_line_no,
            "fields": [fs.provenance() for fs in sorted(state.fields.values(), key=lambda f: f.name)],
        }

    def pending_items(self) -> list[Adjudication]:
        return [item for item in self.adjudications.values() if item.status == "pending"]
