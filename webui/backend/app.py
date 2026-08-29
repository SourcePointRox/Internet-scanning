"""WebUI 后端：FastAPI 管控面。

- 启动前在配置端口区间内探测空闲端口；
- REST：状态 / 带宽 / 模块启停 / 扫描调速·暂停·续扫 / 错误事件 / 存储路径 / 只读查询 / 分类统计；
- WebSocket：1s 间隔推送实时仪表盘数据（含扫描进度与速率历史）；
- 前端静态文件直接由本服务托管（无构建链）。
"""
from __future__ import annotations

import asyncio
import logging
import socket
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.webui")

FRONTEND = Path(__file__).resolve().parent.parent / "frontend"


def find_free_port(host: str, lo: int, hi: int) -> int:
    for port in range(lo, hi + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"端口区间 {lo}-{hi} 无可用端口")


class BandwidthUpdate(BaseModel):
    upload_mbps: float | None = None
    cap_pct: float | None = None
    quotas: dict[str, float] | None = None


class RateUpdate(BaseModel):
    rate_pps: int = Field(ge=100, le=1_000_000)


def _dashboard(orch) -> dict:
    """仪表盘聚合数据（REST /status 与 WS 推送共用）。"""
    return {
        "modules": REGISTRY.snapshot(),
        "bandwidth": orch.bandwidth.snapshot(),
        "nic": orch.nic.rates(),
        "queues": {
            "l4_q": orch.l4_q.qsize(), "l7_q": orch.l7_q.qsize(),
            "enrich_q": orch.enrich_q.qsize(), "class_q": orch.class_q.qsize(),
        },
        "scan": orch.scan_progress(),
        "errors_recent": ErrorReporter.get().counts(),
        "dry_run": orch.dry_run,
        "engine_l7": orch.l7.engine,
        "node": {"id": orch.node_id},
    }


def create_app(orch) -> FastAPI:
    app = FastAPI(title="NetAtlas WebUI", docs_url=None, redoc_url=None)

    # ---------------- 状态 ----------------
    @app.get("/api/status")
    def status():
        d = _dashboard(orch)
        d["storage"] = orch.catalog.storage_overview()
        return d

    # ---------------- 实时信息流 ----------------
    @app.get("/api/feed")
    def feed(n: int = 50):
        from orchestrator import livefeed
        return {"items": livefeed.latest(min(n, 200))}

    # ---------------- 带宽控制 ----------------
    @app.get("/api/bandwidth")
    def get_bw():
        return orch.bandwidth.snapshot()

    @app.post("/api/bandwidth")
    def set_bw(update: BandwidthUpdate):
        if update.upload_mbps is not None:
            orch.bandwidth.set_upload_mbps(update.upload_mbps)
        if update.cap_pct is not None:
            orch.bandwidth.set_cap_pct(update.cap_pct)
        if update.quotas:
            for k, v in update.quotas.items():
                orch.bandwidth.set_quota(k, v)
        return orch.bandwidth.snapshot()

    # ---------------- 扫描控制：调速 / 暂停 / 续扫 ----------------
    @app.get("/api/scan/progress")
    def scan_progress():
        return orch.scan_progress()

    @app.post("/api/scan/rate")
    def scan_rate(update: RateUpdate):
        orch.l4.set_rate(update.rate_pps)
        return {"rate_pps": orch.l4.rate_pps,
                "note": "masscan 无热调速：新速率在当前/下一分片边界生效；scapy 后端立即生效"}

    @app.post("/api/scan/pause")
    def scan_pause():
        orch.l4.pause()
        return {"paused": True, "state": orch.scan_progress()["scan_state"]}

    @app.post("/api/scan/resume")
    def scan_resume():
        orch.l4.resume()
        return {"resumed": True}

    # ---------------- 模块启停 ----------------
    @app.post("/api/modules/{name}/start")
    def module_start(name: str):
        ok = REGISTRY.start(name)
        return JSONResponse({"name": name, "started": ok},
                            status_code=200 if ok else 409)

    @app.post("/api/modules/{name}/stop")
    def module_stop(name: str):
        ok = REGISTRY.stop(name)
        return JSONResponse({"name": name, "stopped": ok},
                            status_code=200 if ok else 409)

    @app.post("/api/pipeline/start")
    def pipeline_start():
        orch.start_pipeline()
        return {"ok": True}

    @app.post("/api/pipeline/stop")
    def pipeline_stop():
        orch.stop_pipeline()
        return {"ok": True}

    # ---------------- 错误事件 ----------------
    @app.get("/api/errors")
    def errors(n: int = 50, module: str | None = None):
        reporter = ErrorReporter.get()
        return {"in_memory": reporter.recent(min(n, 200), module=module),
                "persisted": orch.catalog.recent_errors(min(n, 100)),
                "counts": reporter.counts()}

    # ---------------- 数据查询 ----------------
    @app.get("/api/classification/stats")
    def class_stats():
        return {"top_categories": orch.catalog.classification_stats()}

    @app.get("/api/query")
    def query(sql: str):
        """只读 DuckDB 查询代理（防御：仅 SELECT，限 200 行）。"""
        normalized = sql.strip().rstrip(";")
        if not normalized.lower().startswith("select"):
            return JSONResponse({"error": "仅允许 SELECT 查询"}, status_code=400)
        try:
            con = orch.catalog.duck()
            sql_final = normalized if " limit " in normalized.lower() else normalized + " LIMIT 200"
            rows = con.execute(sql_final).fetchall()
            cols = [d[0] for d in con.description]
            return {"columns": cols, "rows": rows}
        except Exception as e:  # noqa: BLE001 —— 查询错误透传给前端展示
            return JSONResponse({"error": str(e)}, status_code=400)

    # ---------------- WebSocket 实时推送 ----------------
    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        from orchestrator import livefeed
        await websocket.accept()
        interval = float(orch.cfg.get("webui", "ws_push_interval_s", default=1.0))
        last_seq = 0  # 信息流增量游标：只推新条目，前端追加渲染
        try:
            while True:
                new_items = livefeed.since(last_seq)
                if new_items:
                    last_seq = new_items[-1]["seq"]
                payload = _dashboard(orch)
                payload["feed"] = new_items
                await websocket.send_json(payload)
                await asyncio.sleep(interval)
        except (WebSocketDisconnect, RuntimeError):
            return

    # ---------------- 前端 ----------------
    @app.get("/")
    def index():
        return FileResponse(FRONTEND / "index.html")

    return app


def run_webui(orch) -> None:
    """探测空闲端口并在前台运行 uvicorn（阻塞）。"""
    import uvicorn
    host = orch.cfg.get("webui", "host", default="127.0.0.1")
    lo, hi = orch.cfg.get("webui", "port_range", default=[8000, 9000])
    port = find_free_port(host, int(lo), int(hi))
    url = f"http://{host}:{port}"
    log.info("WebUI 已开放: %s", url)
    REGISTRY.set_extra("webui", url=url, port=port)
    REGISTRY.set_running("webui", True)  # 前端控制面板需正确显示自身状态
    print(f"\n  NetAtlas WebUI → {url}\n")

    app = create_app(orch)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    server.run()
