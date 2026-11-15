"""补丁流水线 · 业务变更（change_id）跨表编排（纯函数侧）。

一个业务变更可能同时改订单、客户、库存多张表。口径：

- 清单（manifest）声明每个 change_id 预期覆盖哪些表；没有显式 change_id 的补丁
  归为「单表隐式变更」，预期表就是它自己所在的表。
- 每张表只到达一部分时，变更停在 `waiting`（半到达结果不发布）。
- 预期表全部到达后进入 `ready`；若该变更涉及的键上还有 pending 待裁决项，则 `blocked`。
- ready 变更由 publish 模块在**一个 SQLite 事务**里原子切换到现网；
  发布成功 → `published`，失败保持上一版与原状态，可从检查点续跑。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .patch_engine import Adjudication, PatchEvent

# 变更状态（对外契约）
WAITING = "waiting"        # 预期表未到齐
READY = "ready"            # 已到齐且无阻塞，可原子发布
BLOCKED = "blocked"        # 到齐但存在未裁决字段冲突
PUBLISHED = "published"    # 已原子切换到现网
SUPERSEDED = "superseded"  # 变更内键被更高版本整体推进（保留审计，不再单独发布）

CHANGE_STATUS_LABELS = {
    WAITING: "等待跨表到齐",
    READY: "可发布",
    BLOCKED: "待裁决阻塞",
    PUBLISHED: "已发布现网",
    SUPERSEDED: "已被更高版本取代",
}

IMPLICIT_PREFIX = "IMPLICIT-"   # 无 change_id 补丁的隐式变更号前缀


def implicit_change_id(table: str, pk: str, version: int) -> str:
    """无业务变更号的补丁：按 表+键+版本 生成隐式变更，天然是单表变更。"""
    return f"{IMPLICIT_PREFIX}{table}:{pk}:v{version}"


@dataclass
class ChangeSpec:
    change_id: str
    expected_tables: list[str]
    label: str = ""
    implicit: bool = False


@dataclass
class ChangeState:
    change_id: str
    expected_tables: list[str]
    arrived_tables: set[str] = field(default_factory=set)
    touched_keys: set[tuple[str, str]] = field(default_factory=set)
    label: str = ""
    implicit: bool = False
    status: str = WAITING

    def to_dict(self) -> dict:
        return {
            "change_id": self.change_id,
            "label": self.label,
            "implicit": self.implicit,
            "expected_tables": list(self.expected_tables),
            "arrived_tables": sorted(self.arrived_tables),
            "missing_tables": [t for t in self.expected_tables if t not in self.arrived_tables],
            "touched_keys": [{"table": t, "pk": pk} for t, pk in sorted(self.touched_keys)],
            "status": self.status,
            "status_label": CHANGE_STATUS_LABELS.get(self.status, self.status),
        }


class ChangeBook:
    """登记每个业务变更的到表情况与触碰键，并据待裁决项刷新状态。"""

    def __init__(self, specs: dict[str, ChangeSpec] | None = None) -> None:
        self.specs: dict[str, ChangeSpec] = dict(specs or {})
        self.changes: dict[str, ChangeState] = {}

    def register(self, event: PatchEvent) -> ChangeState:
        change_id = event.change_id or implicit_change_id(event.table, event.pk, event.version)
        state = self.changes.get(change_id)
        if state is None:
            spec = self.specs.get(change_id)
            if spec is not None:
                state = ChangeState(
                    change_id=change_id,
                    expected_tables=list(spec.expected_tables),
                    label=spec.label,
                    implicit=spec.implicit,
                )
            else:
                # 清单未声明：显式 change_id 默认只登记它实际出现的表；
                # 隐式变更预期表就是自身表（单表即可到齐）
                state = ChangeState(
                    change_id=change_id,
                    expected_tables=[event.table],
                    implicit=change_id.startswith(IMPLICIT_PREFIX),
                )
            self.changes[change_id] = state
        state.arrived_tables.add(event.table)
        state.touched_keys.add((event.table, event.pk))
        return state

    def refresh_status(
        self,
        adjudications: dict[str, Adjudication],
        published_changes: set[str] | None = None,
    ) -> None:
        """按「到齐度 + 待裁决项」重算每个变更状态；已发布的保持 published。"""
        published_changes = published_changes or set()
        pending = {
            (item.table, item.pk)
            for item in adjudications.values()
            if item.status == "pending"
        }
        for state in self.changes.values():
            if state.change_id in published_changes or state.status == PUBLISHED:
                state.status = PUBLISHED
                continue
            all_arrived = all(t in state.arrived_tables for t in state.expected_tables)
            if not all_arrived:
                state.status = WAITING
            elif any(key in pending for key in state.touched_keys):
                state.status = BLOCKED
            else:
                state.status = READY

    def ready_changes(self) -> list[ChangeState]:
        return [s for s in self.changes.values() if s.status == READY]

    def get(self, change_id: str) -> ChangeState | None:
        return self.changes.get(change_id)
