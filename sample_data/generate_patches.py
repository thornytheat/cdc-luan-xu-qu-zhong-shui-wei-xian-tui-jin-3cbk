#!/usr/bin/env python3
"""确定性生成「字段级合并 + 跨表原子发布」演示补丁。

产物（固定内容，不进随机数）：
- sample_data/patches/orders-lib.jsonl      订单库补丁（含整行覆盖会误删的半行补丁）
- sample_data/patches/customers-lib.jsonl   客户库补丁
- sample_data/patches/inventory-lib.jsonl   库存库补丁
- sample_data/patches/manifest.json         业务变更清单（change_id → 预期表）

埋点（脚本末尾自检）：
1. 半行补丁只带 status：amount 等未携带字段必须原样保留（整行覆盖会被清空）。
2. CHG-20261115-01 跨 orders/customers 两表：orders 先到时变更保持 waiting，不进现网；
   customers 到达后同一事务原子切换。
3. 同字段同版本不同值（amount 9900 vs 9955）→ 待裁决，相关变更 blocked，先到值保留；
   裁决 incoming 后才允许发布。
4. 同版本同值重试补丁 → 静默去重；整条重试不增加发布次数。
5. 墓碑 + 墓碑后低版本忽略 + 更高版本复活（库存键 I7007）。
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PATCH_DIR = ROOT / "patches"

# 到达顺序由 seq 决定（跨来源全局排序），来源内行号从 1 开始
orders_patches = [
    # seq, line, pk, version, ts(分钟偏移), op, after, change_id
    (1, 1, "O7001", 1, 10, "c", {
        "order_id": "O7001", "customer_id": "C7001", "amount": 8800,
        "qty": 2, "status": "pending", "updated_at": "2026-11-15T09:10:00Z"}, None),
    (2, 2, "O7001", 2, 12, "u", {
        "order_id": "O7001", "customer_id": "C7001", "amount": 8800,
        "qty": 2, "status": "pending", "updated_at": "2026-11-15T09:10:00Z"}, None),  # 同版本同值去重
    (3, 3, "O7001", 3, 15, "u", {
        "status": "paid", "updated_at": "2026-11-15T09:15:00Z"},
        "CHG-20261115-01"),  # 半行补丁：只带 status，其他字段必须保留
    # 同字段同版本不同值 → 隔离待裁决（先到保留 9900）
    (4, 4, "O7002", 1, 16, "c", {
        "order_id": "O7002", "customer_id": "C7002", "amount": 9900,
        "qty": 1, "status": "pending", "updated_at": "2026-11-15T09:16:00Z"},
        "CHG-20261115-02"),
    (5, 5, "O7002", 1, 18, "u", {
        "order_id": "O7002", "customer_id": "C7002", "amount": 9955,
        "qty": 1, "status": "pending", "updated_at": "2026-11-15T09:18:00Z"},
        "CHG-20261115-02"),
    (6, 6, "O7003", 2, 20, "u", {
        "order_id": "O7003", "customer_id": "C7003", "amount": 7600,
        "qty": 4, "status": "shipped", "updated_at": "2026-11-15T09:20:00Z"},
        "CHG-20261115-03"),  # 无对应高版本基线，作为新键首见处理
]

customers_patches = [
    (7, 1, "C7001", 1, 11, "c", {
        "customer_id": "C7001", "name": "周婷", "email": "c7001@example.com",
        "tier": "gold", "updated_at": "2026-11-15T09:11:00Z"}, None),
    (8, 2, "C7001", 2, 15, "u", {
        "tier": "platinum", "updated_at": "2026-11-15T09:15:00Z"},
        "CHG-20261115-01"),  # 跨表变更的第二只脚：到达后 CHG-01 才到齐
    (9, 3, "C7009", 1, 22, "c", {
        "customer_id": "C7009", "name": "吴迪", "email": "c7009@example.com",
        "tier": "silver", "updated_at": "2026-11-15T09:22:00Z"}, None),
]

inventory_patches = [
    (10, 1, "I7007", 1, 12, "c", {
        "sku": "SKU-7007", "warehouse": "WH-SH-01", "on_hand": 300,
        "reserved": 10, "updated_at": "2026-11-15T09:12:00Z"}, None),
    (11, 2, "I7007", 2, 14, "u", {
        "on_hand": 280, "updated_at": "2026-11-15T09:14:00Z"}, None),  # 半行：warehouse 保留
    (12, 3, "I7007", 3, 17, "d", {}, "CHG-20261115-04"),               # 墓碑
    (13, 4, "I7007", 2, 13, "u", {
        "on_hand": 260, "updated_at": "2026-11-15T09:13:00Z"}, None),  # 墓碑后低版本 → 忽略
    (14, 5, "I7007", 4, 25, "c", {
        "sku": "SKU-7007", "warehouse": "WH-BJ-02", "on_hand": 120,
        "reserved": 0, "updated_at": "2026-11-15T09:25:00Z"},
        "CHG-20261115-04"),  # 严格更高版本复活
    (15, 6, "I7008", 1, 26, "c", {
        "sku": "SKU-7008", "warehouse": "WH-GZ-03", "on_hand": 540,
        "reserved": 3, "updated_at": "2026-11-15T09:26:00Z"}, None),
]

manifest = {
    "changes": [
        {"change_id": "CHG-20261115-01", "label": "大促下单状态与客户等级联动",
         "expected_tables": ["orders", "customers"]},
        {"change_id": "CHG-20261115-02", "label": "订单金额重算（两库口径不同，需裁决）",
         "expected_tables": ["orders"]},
        {"change_id": "CHG-20261115-03", "label": "订单履约状态更新",
         "expected_tables": ["orders"]},
        {"change_id": "CHG-20261115-04", "label": "库存键删除后重建",
         "expected_tables": ["inventory"]},
    ]
}

FILES = {
    "orders-lib.jsonl": orders_patches,
    "customers-lib.jsonl": customers_patches,
    "inventory-lib.jsonl": inventory_patches,
}


def main() -> int:
    PATCH_DIR.mkdir(parents=True, exist_ok=True)
    total = 0
    for filename, rows in FILES.items():
        with (PATCH_DIR / filename).open("w", encoding="utf-8") as fh:
            for _seq, line_no, pk, version, minute, op, after, change_id in rows:
                event = {
                    "table": filename.split("-")[0],
                    "pk": pk,
                    "version": version,
                    "ts": f"2026-11-15T09:{minute:02d}:00Z",
                    "op": op,
                    "after": after,
                    "change_id": change_id,
                }
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
                total += 1
        print(f"{filename:22s} 补丁 {len(rows):2d} 条")
    (PATCH_DIR / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"manifest 声明 {len(manifest['changes'])} 个业务变更；补丁合计 {total} 条")

    # --- 自检（与引擎口径对齐） ---
    assert total == 15
    # 半行补丁存在且只带两个字段
    half = next(r for r in orders_patches if r[6].keys() and set(r[6]) == {"status", "updated_at"})
    assert half[2] == "O7001" and half[7] == "CHG-20261115-01"
    # 跨表变更预期两张表
    chg1 = next(c for c in manifest["changes"] if c["change_id"] == "CHG-20261115-01")
    assert chg1["expected_tables"] == ["orders", "customers"]
    # 同版本冲突埋点
    conflicts = [r for r in orders_patches if r[2] == "O7002"]
    assert len(conflicts) == 2 and conflicts[0][3] == conflicts[1][3] == 1
    assert conflicts[0][6]["amount"] != conflicts[1][6]["amount"]
    # 墓碑与复活
    tomb = [r for r in inventory_patches if r[5] == "d"]
    assert tomb and tomb[0][3] == 3
    revive = [r for r in inventory_patches if r[2] == "I7007" and r[5] == "c" and r[3] == 4]
    assert len(revive) == 1
    print("自检通过：半行保留 / 跨表闸门 / 字段冲突 / 墓碑复活 埋点齐备")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
