"""FastAPI 路由（薄）：页面渲染 + JSON 端点，业务全部在领域模块里。

页面：/ 首页 · /workbench 工作台 · /result/{run_id} 结果 · /review 复核与导出
接口：POST /api/merge · GET /api/runs · GET /api/runs/{id} · GET /api/runs/{id}/snapshot
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import archive, patch_runner, patch_store, runner, schema_gate, settings, snapshot, store
from .archive import EventError
from .patch_engine import PatchError, parse_patch_event

BASE_DIR = settings.BASE_DIR


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = store.connect()
    store.init_db(conn)
    patch_store.init_patch_tables(conn)
    if store.total_events(conn) == 0:
        store.seed_sample(conn)
    conn.close()
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


class MergeRequest(BaseModel):
    tables: list[str] = Field(default_factory=list, description="要合并的表名")
    schema_version: int = Field(2, ge=1, le=2, description="要评估的目标 schema 版本")


def render(request: Request, template: str, **ctx) -> HTMLResponse:
    context = {
        "app_name": settings.APP_NAME,
        "app_short": settings.APP_SHORT,
        "tables_meta": settings.TABLES,
        "kind_labels": settings.KIND_LABELS,
        "verdict_labels": settings.VERDICT_LABELS,
        "change_labels": settings.CHANGE_LABELS,
        "op_labels": settings.OP_LABELS,
        "field_decision_labels": patch_runner_label_map(),
        "change_status_labels": patch_status_label_map(),
        "patch_sources": settings.PATCH_SOURCES,
        "nav": ctx.pop("nav", ""),
    }
    context.update(ctx)
    return templates.TemplateResponse(request, template, context)


def patch_runner_label_map() -> dict:
    from .patch_engine import FIELD_DECISION_LABELS

    return FIELD_DECISION_LABELS


def patch_status_label_map() -> dict:
    from .patch_changes import CHANGE_STATUS_LABELS

    return CHANGE_STATUS_LABELS


def with_conn():
    conn = store.connect()
    store.init_db(conn)
    patch_store.init_patch_tables(conn)
    return conn


# --- 页面 ------------------------------------------------------------------


@app.get("/health", include_in_schema=False)
def health():
    conn = with_conn()
    try:
        return {
            "status": "ok",
            "app": settings.APP_SHORT,
            "events": store.total_events(conn),
            "runs": store.run_count(conn),
            "seed_version": store.get_meta(conn, "seed_version"),
            "pipeline": patch_store.patch_counts(conn),
        }
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        meta = {
            "seed_version": store.get_meta(conn, "seed_version"),
            "seeded_at": store.get_meta(conn, "seeded_at"),
            "total_events": store.total_events(conn),
            "runs": store.run_count(conn),
        }
        recent = store.list_runs(conn, limit=3)
    finally:
        conn.close()
    stats = {
        "total_events": meta["total_events"],
        "table_count": len(settings.TABLES),
        "runs": meta["runs"],
        "last_run_id": recent[0]["run_id"] if recent else None,
        "last_run_at": recent[0]["created_at"] if recent else None,
        "last_conflicts": recent[0]["summary"]["conflicts"] if recent else 0,
    }
    return render(
        request,
        "index.html",
        page="index",
        nav="index",
        overview=overview,
        meta=meta,
        recent=recent,
        stats=stats,
        embedded=settings.EMBEDDED,
        steps=settings.DEMO_STEPS,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
        changes=schema_gate.structural_changes(
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
            settings.TABLE_NAMES,
        ),
    )


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        recent = store.list_runs(conn, limit=5)
    finally:
        conn.close()
    return render(
        request,
        "workbench.html",
        page="workbench",
        nav="workbench",
        overview=overview,
        recent=recent,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
    )


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result_page(request: Request, run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        return render(
            request,
            "not_found.html",
            page="not_found",
            nav="",
            run_id=run_id,
            error=f"没有找到运行 {run_id}，可能已被清理；请到工作台重新执行一次合并。",
        )
    snap = snapshot.build_snapshot(run)
    preview = snapshot.preview(snap)
    result = run["result"]
    ignored = {
        table: set(payload.get("ignored_line_nos", []))
        for table, payload in result["tables"].items()
    }
    change_by_line = {(entry["table"], entry["line_no"]): entry for entry in result["changelog"]}
    late_rows = []
    for item in result["late_events"]:
        row = dict(item)
        entry = change_by_line.get((item["table"], item["line_no"]), {})
        row["after_tombstone"] = item["line_no"] in ignored.get(item["table"], set())
        row["kind"] = entry.get("kind", "")
        row["kind_label"] = entry.get("kind_label", "已处理")
        row["reason"] = entry.get("reason", "")
        late_rows.append(row)
    return render(
        request,
        "result.html",
        page="result",
        nav="",
        run=run,
        summary=run["summary"],
        result=result,
        snapshot=snap,
        preview=preview,
        late_rows=late_rows,
        snapshot_bytes=snapshot.snapshot_size(snap),
        base_schema=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        target_schema=schema_gate.load_schema(
            settings.SCHEMAS_DIR / f"v{run['schema_version']}.json"
        ),
    )


@app.get("/review", response_class=HTMLResponse)
def review(request: Request, run_id: str | None = None, kind: str = Query(default="")):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=50)
        selected = store.get_run(conn, run_id) if run_id else (
            store.get_run(conn, runs[0]["run_id"]) if runs else None
        )
    finally:
        conn.close()
    entries: list[dict] = []
    kinds: list[str] = []
    if selected is not None:
        entries = selected["result"]["changelog"]
        kinds = sorted({entry["kind"] for entry in entries})
        if kind:
            entries = [entry for entry in entries if entry["kind"] == kind]
        entries = entries[:200]
    return render(
        request,
        "review.html",
        page="review",
        nav="review",
        runs=runs,
        selected=selected,
        entries=entries,
        kinds=kinds,
        active_kind=kind,
        run_count=len(runs),
    )


# --- API -------------------------------------------------------------------


@app.post("/api/merge")
def api_merge(payload: MergeRequest):
    conn = with_conn()
    try:
        run = runner.execute(conn, payload.tables, payload.schema_version)
    except runner.RunError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except schema_gate.SchemaError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EventError as exc:
        raise HTTPException(status_code=422, detail=f"样例事件解析失败：{exc}") from exc
    finally:
        conn.close()
    return JSONResponse(
        {
            "run_id": run["run_id"],
            "summary": run["summary"],
            "tables": run["tables"],
            "schema_version": run["schema_version"],
            "result_url": f"/result/{run['run_id']}",
            "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
        }
    )


@app.get("/api/runs")
def api_runs(limit: int = Query(default=20, ge=1, le=100)):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=limit)
    finally:
        conn.close()
    return {"runs": runs, "count": len(runs)}


@app.get("/api/runs/{run_id}")
def api_run(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
    result = run["result"]
    return {
        "run_id": run["run_id"],
        "created_at": run["created_at"],
        "tables": run["tables"],
        "schema_version": run["schema_version"],
        "summary": run["summary"],
        "watermark": {
            "watermark": result["watermark"]["watermark"],
            "open_keys": result["watermark"]["open_keys"],
            "closed_keys": result["watermark"]["closed_keys"],
            "per_table": {
                table: {
                    "watermark": payload["watermark"]["watermark"],
                    "open_keys": payload["watermark"]["open_keys"],
                    "closed_keys": payload["watermark"]["closed_keys"],
                    "laggard_keys": payload["watermark"]["laggard_keys"],
                }
                for table, payload in result["tables"].items()
            },
        },
        "tables": {
            table: {
                "row_count": payload["row_count"],
                "tombstone_count": payload["tombstone_count"],
                "key_count": payload["key_count"],
                "counters": payload["counters"],
            }
            for table, payload in result["tables"].items()
        },
        "gate": result["gate"],
        "conflicts": [
            {
                "table": item["table"],
                "pk": item["pk"],
                "version": item["version"],
                "diff": item["diff"],
                "first_line": item["first"]["line_no"],
                "second_line": item["second"]["line_no"],
            }
            for item in result["conflicts"]
        ],
        "late_events": result["late_events"],
        "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
    }


@app.get("/api/runs/{run_id}/snapshot")
def api_snapshot(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
    payload = snapshot.dumps(snapshot.build_snapshot(run))
    filename = f"cdcwater-snapshot-{run_id}.json"
    return Response(
        content=payload,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/seed")
def api_seed():
    """幂等重放样例：已归档的事件不会重复插入。"""
    conn = with_conn()
    try:
        stats = store.seed_sample(conn)
        stats["total_events"] = store.total_events(conn)
        stats["counts"] = store.event_counts(conn)
    except EventError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        conn.close()
    return stats


@app.get("/api/samples")
def api_samples():
    conn = with_conn()
    try:
        return {"overview": store.event_overview(conn), "total": store.total_events(conn)}
    finally:
        conn.close()


@app.get("/api/schemas/{version}")
def api_schema(version: int):
    if version not in (1, 2):
        raise HTTPException(status_code=400, detail="schema 版本只支持 1 或 2")
    return schema_gate.load_schema(settings.SCHEMAS_DIR / f"v{version}.json")


# ===========================================================================
# 补丁流水线：字段级合并 + 跨表原子发布
# ===========================================================================


class IngestItem(BaseModel):
    table: str
    pk: str
    version: int = Field(ge=1)
    ts: str
    op: str
    after: dict = Field(default_factory=dict)
    change_id: str | None = None
    source: str = "api-inline.jsonl"
    line_no: int | None = None


class IngestRequest(BaseModel):
    events: list[IngestItem] = Field(default_factory=list)
    manifests: list[dict] = Field(default_factory=list)
    auto_publish: bool = True
    fail_after_line: int | None = Field(default=None, ge=1)


class AdjudicateRequest(BaseModel):
    item_id: str
    choose: str = Field(description="kept 保留先到值 / incoming 采用后到值")
    note: str | None = None


class PublishRequest(BaseModel):
    change_id: str


class SeedPatchRequest(BaseModel):
    auto_publish: bool = True
    fail_after_line: int | None = Field(default=None, ge=1)


def _patch_error(exc: PatchError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@app.get("/pipeline", response_class=HTMLResponse)
def pipeline_page(request: Request):
    conn = with_conn()
    try:
        view = patch_runner.pipeline_view(conn)
        manifest_doc = None
        if settings.PATCH_MANIFEST.exists():
            import json as _json

            manifest_doc = _json.loads(settings.PATCH_MANIFEST.read_text(encoding="utf-8"))
    finally:
        conn.close()
    return render(
        request,
        "pipeline.html",
        page="pipeline",
        nav="pipeline",
        view=view,
        manifest_doc=manifest_doc,
    )


@app.post("/api/pipeline/seed")
def api_pipeline_seed(payload: SeedPatchRequest | None = None):
    """幂等导入样例补丁（15 条 / 3 个来源库 / 4 个业务变更）。"""
    payload = payload or SeedPatchRequest()
    conn = with_conn()
    try:
        try:
            result = patch_runner.seed_sample_patches(
                conn,
                auto_publish=payload.auto_publish,
                fail_after_line=payload.fail_after_line,
            )
        except PatchError as exc:
            # fail_after_line 故障注入：已回滚且检查点可续跑
            view = patch_runner.pipeline_view(conn)
            return JSONResponse(
                status_code=409,
                content={"detail": str(exc), "retryable": True, "view": view},
            )
        result["counts"] = patch_store.patch_counts(conn)
        result["view"] = patch_runner.pipeline_view(conn)
        return result
    finally:
        conn.close()


@app.post("/api/pipeline/reset")
def api_pipeline_reset(payload: dict | None = None):
    """清空补丁流水线全部状态（演示重放）；旧 CDC events/runs 不受影响。"""
    conn = with_conn()
    try:
        patch_store.reset_pipeline(conn)
        return {"reset": True, "counts": patch_store.patch_counts(conn)}
    finally:
        conn.close()


@app.post("/api/pipeline/ingest")
def api_pipeline_ingest(payload: IngestRequest):
    """摄入一组补丁事件；自动按检查点续跑，ready 变更在单事务内原子发布。"""
    from .patch_changes import ChangeSpec

    conn = with_conn()
    try:
        try:
            events = []
            used_lines: dict[str, int] = {}
            for index, item in enumerate(payload.events, start=1):
                line_no = item.line_no or used_lines.get(item.source, 0) + 1
                used_lines[item.source] = max(used_lines.get(item.source, 0), line_no)
                obj = item.model_dump(exclude={"source", "line_no"})
                events.append(parse_patch_event(obj, item.source, line_no, index))
        except PatchError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        manifests = {
            spec["change_id"]: ChangeSpec(
                change_id=spec["change_id"],
                expected_tables=list(spec["expected_tables"]),
                label=spec.get("label", ""),
            )
            for spec in payload.manifests
        }
        try:
            result = patch_runner.ingest_events(
                conn,
                events,
                manifests=manifests,
                auto_publish=payload.auto_publish,
                fail_after_line=payload.fail_after_line,
            )
        except PatchError as exc:
            # 故障注入/处理失败：409 表示已回滚但检查点可续跑，附带当前视图便于页面刷新
            view = patch_runner.pipeline_view(conn)
            return JSONResponse(
                status_code=409,
                content={"detail": str(exc), "retryable": True, "view": view},
            )
        result["counts"] = patch_store.patch_counts(conn)
        result["view"] = patch_runner.pipeline_view(conn)
        return result
    finally:
        conn.close()


@app.post("/api/pipeline/adjudicate")
def api_pipeline_adjudicate(payload: AdjudicateRequest):
    conn = with_conn()
    try:
        result = patch_runner.adjudicate(conn, payload.item_id, payload.choose, payload.note)
        result["view"] = patch_runner.pipeline_view(conn)
        return result
    except PatchError as exc:
        raise _patch_error(exc) from exc
    finally:
        conn.close()


@app.post("/api/pipeline/publish")
def api_pipeline_publish(payload: PublishRequest):
    conn = with_conn()
    try:
        entry = patch_runner.publish_change(conn, payload.change_id)
        return {"entry": entry, "view": patch_runner.pipeline_view(conn)}
    except PatchError as exc:
        raise _patch_error(exc) from exc
    finally:
        conn.close()


@app.get("/api/pipeline/view")
def api_pipeline_view():
    conn = with_conn()
    try:
        return patch_runner.pipeline_view(conn)
    finally:
        conn.close()


@app.get("/api/pipeline/publishes")
def api_pipeline_publishes(limit: int = Query(default=50, ge=1, le=200)):
    conn = with_conn()
    try:
        return {"publishes": patch_store.list_publish_log(conn, limit=limit)}
    finally:
        conn.close()
