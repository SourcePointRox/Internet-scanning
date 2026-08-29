"""分布式协同接口（预留）：任务分片与节点协调。

设计目标：当前单机运行零依赖（LocalShardCoordinator），未来多机扩展时
仅需替换协调器实现（HTTP/etcd/Redis 后端），编排器与扫描模块无需改动。

分片策略：把目标 CIDR 空间按 /24 块做一致性哈希，节点 N 仅扫描
hash(block) % shard_total == shard_index 的块 —— 静态、无中心、可复算，
与断点续扫状态机天然兼容（每节点状态文件独立）。
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Any, Iterator

log = logging.getLogger("netatlas.sharding")


def stable_node_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class ShardCoordinator(ABC):
    """多机协调器抽象：节点注册、心跳、分片声明、全局进度汇总。"""

    @abstractmethod
    def register_node(self, node_id: str, meta: dict[str, Any]) -> None: ...

    @abstractmethod
    def heartbeat(self, node_id: str, progress: dict[str, Any]) -> None: ...

    @abstractmethod
    def active_nodes(self) -> list[dict[str, Any]]: ...

    @abstractmethod
    def my_targets(self, targets: list[str], shard_total: int, shard_index: int
                   ) -> list[str]: ...

    @abstractmethod
    def global_progress(self) -> dict[str, Any]: ...

    def close(self) -> None:  # noqa: B027 —— 默认无资源
        pass


class LocalShardCoordinator(ShardCoordinator):
    """单机默认实现：全部分片归本节点，进度即本机状态。"""

    def __init__(self):
        self._nodes: dict[str, dict[str, Any]] = {}

    def register_node(self, node_id: str, meta: dict[str, Any]) -> None:
        self._nodes[node_id] = {"node_id": node_id, "meta": meta,
                                "registered_at": time.time(), "last_seen": time.time(),
                                "progress": {}}

    def heartbeat(self, node_id: str, progress: dict[str, Any]) -> None:
        node = self._nodes.get(node_id)
        if node:
            node["last_seen"] = time.time()
            node["progress"] = progress

    def active_nodes(self) -> list[dict[str, Any]]:
        cutoff = time.time() - 90
        return [n for n in self._nodes.values() if n["last_seen"] >= cutoff]

    def my_targets(self, targets: list[str], shard_total: int, shard_index: int) -> list[str]:
        return list(targets)  # 单机：全量

    def global_progress(self) -> dict[str, Any]:
        nodes = self.active_nodes()
        return {"coordinator": "local", "nodes": len(nodes),
                "probes_sent": sum(int(n["progress"].get("probes_sent", 0)) for n in nodes),
                "open_found": sum(int(n["progress"].get("open_found", 0)) for n in nodes)}


class HashedSharder:
    """CIDR 空间一致性哈希分片器（无状态，任何节点可独立复算同一划分）。

    多机部署时所有节点使用相同 (shard_total, seed)，各自仅扫描
    ``assign(block) == shard_index`` 的 /24 块；IPv6 按 /48 块同理。
    """

    def __init__(self, shard_total: int = 1, shard_index: int = 0, seed: str = "netatlas"):
        if not 0 <= shard_index < max(1, shard_total):
            raise ValueError(f"shard_index 必须位于 [0, {shard_total})")
        self.total, self.index, self.seed = max(1, shard_total), shard_index, seed

    def assign(self, block: str) -> int:
        h = hashlib.blake2s(f"{self.seed}:{block}".encode(), digest_size=4).digest()
        return int.from_bytes(h, "little") % self.total

    def owns(self, block: str) -> bool:
        return self.assign(block) == self.index

    def filter_cidr(self, cidr: str) -> Iterator[str]:
        """把 CIDR 展开为本节点拥有的 /24（IPv4）或 /48（IPv6）块。

        单机（total=1）时原样返回，不做无谓展开。
        """
        if self.total == 1:
            yield cidr
            return
        net = ipaddress.ip_network(cidr, strict=False)
        block_prefix = 24 if net.version == 4 else 48
        if net.prefixlen >= block_prefix:
            if self.owns(str(net)):
                yield str(net)
            return
        for block in net.subnets(new_prefix=block_prefix):
            if self.owns(str(block)):
                yield str(block)


# ========================================================================
# HTTP 协调器：多机部署的参考实现（零外部依赖，stdlib only）
#
# 协议（JSON over HTTP）：
#   POST /api/nodes/register  {node_id, meta}                       -> {ok}
#   POST /api/nodes/heartbeat {node_id, progress}                   -> {ok}（同时续租分片）
#   GET  /api/nodes                                               -> {nodes: [...]}
#   POST /api/shards/claim    {node_id, shard_total, preferred_index?} -> {shard_index}
#   GET  /api/progress                                            -> 全局聚合进度
#
# 服务端（CoordinatorServer）持有节点注册表与**带租约的分片认领表**：
# 节点心跳即续租；租约（默认 45s）到期未续，分片自动释放，可被其他节点接管
# —— 节点宕机不会造成任务分片永久丢失。
# ========================================================================

DEFAULT_LEASE_TTL_S = 45.0


class _CoordinatorState:
    """协调器服务端内存状态（线程安全）。"""

    def __init__(self, lease_ttl_s: float = DEFAULT_LEASE_TTL_S):
        self.lease_ttl = lease_ttl_s
        self._lock = threading.Lock()
        self.nodes: dict[str, dict[str, Any]] = {}
        # key = f"{shard_total}:{index}" —— 认领表按任务（shard_total）命名空间隔离，
        # 不同分片规模的同名序号互不冲突
        self.claims: dict[str, dict[str, Any]] = {}

    # ---- 节点 ----
    def register(self, node_id: str, meta: dict[str, Any]) -> None:
        with self._lock:
            self.nodes[node_id] = {"node_id": node_id, "meta": meta,
                                   "registered_at": time.time(),
                                   "last_seen": time.time(), "progress": {}}

    def heartbeat(self, node_id: str, progress: dict[str, Any]) -> None:
        with self._lock:
            node = self.nodes.get(node_id)
            if node:
                node["last_seen"] = time.time()
                node["progress"] = progress
            for claim in self.claims.values():          # 心跳即续租
                if claim["node_id"] == node_id:
                    claim["expires_at"] = time.time() + self.lease_ttl

    def active_nodes(self) -> list[dict[str, Any]]:
        cutoff = time.time() - max(90.0, self.lease_ttl * 2)
        with self._lock:
            return [dict(n) for n in self.nodes.values() if n["last_seen"] >= cutoff]

    # ---- 分片认领 ----
    def _purge_expired(self) -> None:
        now = time.time()
        for key in [k for k, c in self.claims.items() if c["expires_at"] < now]:
            log.info("分片 %s 租约到期（节点 %s），释放", key, self.claims[key]["node_id"])
            del self.claims[key]

    def claim(self, node_id: str, shard_total: int, preferred: int | None) -> int:
        """认领一个分片：优先 preferred，否则取最小空闲序号；无空闲返回 -1。"""
        with self._lock:
            self._purge_expired()
            for key, claim in self.claims.items():      # 已持有：续租返回
                if claim["node_id"] == node_id and claim["shard_total"] == shard_total:
                    claim["expires_at"] = time.time() + self.lease_ttl
                    return int(key.split(":", 1)[1])
            if shard_total <= 0:
                return -1
            candidates = ([preferred] if preferred is not None else []) + \
                [i for i in range(shard_total) if i != preferred]
            for idx in candidates:
                if idx is None or not 0 <= idx < shard_total:
                    continue
                key = f"{shard_total}:{idx}"
                if key in self.claims:
                    continue
                self.claims[key] = {"node_id": node_id, "shard_total": shard_total,
                                    "expires_at": time.time() + self.lease_ttl}
                log.info("节点 %s 认领分片 %d/%d", node_id, idx, shard_total)
                return idx
            return -1

    def progress(self) -> dict[str, Any]:
        nodes = self.active_nodes()
        with self._lock:
            self._purge_expired()
            claims = {k: c["node_id"] for k, c in self.claims.items()}
        return {"coordinator": "http", "nodes": len(nodes),
                "probes_sent": sum(int(n["progress"].get("probes_sent", 0)) for n in nodes),
                "open_found": sum(int(n["progress"].get("open_found", 0)) for n in nodes),
                "claims": claims}


def _make_handler(state: _CoordinatorState):
    from http.server import BaseHTTPRequestHandler

    class Handler(BaseHTTPRequestHandler):
        server_version = "NetAtlasCoordinator/1.0"

        def _json_body(self) -> dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length <= 0 or length > 1_000_000:
                    return {}
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return {}

        def _reply(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):               # 静默访问日志
            log.debug("coordinator-http: " + fmt, *args)

        def do_POST(self):
            body = self._json_body()
            node_id = str(body.get("node_id", "") or "")
            if self.path == "/api/nodes/register" and node_id:
                state.register(node_id, dict(body.get("meta") or {}))
                return self._reply(200, {"ok": True})
            if self.path == "/api/nodes/heartbeat" and node_id:
                state.heartbeat(node_id, dict(body.get("progress") or {}))
                return self._reply(200, {"ok": True})
            if self.path == "/api/shards/claim" and node_id:
                try:
                    total = int(body.get("shard_total", 1))
                except (TypeError, ValueError):
                    total = 1
                preferred = body.get("preferred_index")
                try:
                    preferred = int(preferred) if preferred is not None else None
                except (TypeError, ValueError):
                    preferred = None
                idx = state.claim(node_id, total, preferred)
                return self._reply(200, {"shard_index": idx})
            self._reply(404, {"error": f"未知路径或参数缺失: {self.path}"})

        def do_GET(self):
            if self.path == "/api/nodes":
                return self._reply(200, {"nodes": state.active_nodes()})
            if self.path == "/api/progress":
                return self._reply(200, state.progress())
            if self.path == "/api/health":
                return self._reply(200, {"ok": True})
            self._reply(404, {"error": f"未知路径: {self.path}"})

    return Handler


class CoordinatorServer:
    """HTTP 协调器服务端（参考实现，可独立运行）。

    用法：``python -m orchestrator.sharding --serve --port 8765``
    生产环境可替换为 etcd/Redis 后端，客户端协议保持不变。
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8765,
                 lease_ttl_s: float = DEFAULT_LEASE_TTL_S):
        from http.server import ThreadingHTTPServer
        self.state = _CoordinatorState(lease_ttl_s)
        self.httpd = ThreadingHTTPServer((host, port), _make_handler(self.state))
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> None:
        self._thread = threading.Thread(target=self.httpd.serve_forever,
                                        daemon=True, name="shard-coordinator")
        self._thread.start()
        log.info("分片协调器已启动: %s（租约 %.0fs）", self.url, self.state.lease_ttl)

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class HttpShardCoordinator(ShardCoordinator):
    """HTTP 协调器客户端：注册 + 分片认领 + 周期心跳（后台线程续租）。

    容错策略：协调器不可达时按项目错误约定上报 ErrorReporter 并降级——
    分片回落到配置中的 shard_index（绝不扩量扫描，避免多机重复探测）。
    """

    def __init__(self, url: str, shard_total: int = 1, shard_index: int = 0,
                 heartbeat_interval_s: float = 15.0, timeout_s: float = 5.0):
        self.url = url.rstrip("/")
        self.shard_total = max(1, shard_total)
        self._claimed = shard_index                     # 兜底：配置值
        self.heartbeat_interval = heartbeat_interval_s
        self.timeout = timeout_s
        self.node_id: str | None = None
        self._last_progress: dict[str, Any] = {}
        self._stop = threading.Event()
        self._hb_thread: threading.Thread | None = None

    # ---- HTTP 原语 ----
    def _request(self, method: str, path: str, payload: dict | None = None,
                 attempts: int = 2) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        last_exc: Exception | None = None
        for i in range(max(1, attempts)):
            try:
                req = urllib.request.Request(
                    self.url + path, data=body, method=method,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, OSError, ValueError) as e:
                last_exc = e
                if i + 1 < attempts:
                    time.sleep(0.3 * (i + 1))
        from orchestrator.errors import ErrorReporter
        ErrorReporter.get().report("sharding", f"协调器请求失败 {method} {path}",
                                   exc=last_exc)
        raise last_exc  # type: ignore[misc]

    def _post(self, path: str, payload: dict, attempts: int = 2) -> dict[str, Any]:
        return self._request("POST", path, payload, attempts)

    def _get(self, path: str) -> dict[str, Any]:
        return self._request("GET", path)

    # ---- ShardCoordinator 接口 ----
    def register_node(self, node_id: str, meta: dict[str, Any]) -> None:
        self.node_id = node_id
        try:
            self._post("/api/nodes/register", {"node_id": node_id, "meta": meta})
            resp = self._post("/api/shards/claim", {
                "node_id": node_id, "shard_total": self.shard_total,
                "preferred_index": self._claimed}, attempts=3)
            idx = int(resp.get("shard_index", -1))
            if idx >= 0:
                self._claimed = idx
            else:
                from orchestrator.errors import ErrorReporter
                ErrorReporter.get().degrade("sharding", "shard-claim",
                                            "无空闲分片可认领，本节点空转待命")
                self._claimed = -1                      # 显式空转：不扫任何目标
        except Exception:  # noqa: BLE001 —— _request 已上报，这里只降级
            log.warning("协调器 %s 不可达，分片回落配置值 %d", self.url, self._claimed)
        self._start_heartbeat_loop()

    def heartbeat(self, node_id: str, progress: dict[str, Any]) -> None:
        self._last_progress = dict(progress)
        try:
            self._post("/api/nodes/heartbeat",
                       {"node_id": node_id, "progress": progress}, attempts=1)
        except Exception:  # noqa: BLE001 —— 已上报；心跳失败不打断扫描
            pass

    def active_nodes(self) -> list[dict[str, Any]]:
        try:
            return list(self._get("/api/nodes").get("nodes", []))
        except Exception:  # noqa: BLE001 —— 降级：仅本节点
            return [{"node_id": self.node_id or "unknown",
                     "last_seen": time.time(), "progress": self._last_progress}]

    def my_targets(self, targets: list[str], shard_total: int, shard_index: int) -> list[str]:
        total = max(1, self.shard_total or shard_total)
        if total == 1:
            return list(targets)
        if self._claimed < 0:                           # 未认领到分片：不扫
            return []
        sharder = HashedSharder(total, self._claimed)
        out: list[str] = []
        for cidr in targets:
            out.extend(sharder.filter_cidr(cidr))
        return out

    def global_progress(self) -> dict[str, Any]:
        try:
            return self._get("/api/progress")
        except Exception:  # noqa: BLE001 —— 降级：本节点视图
            return {"coordinator": "http(unreachable)", "nodes": 1,
                    "probes_sent": int(self._last_progress.get("probes_sent", 0)),
                    "open_found": int(self._last_progress.get("open_found", 0))}

    def close(self) -> None:
        self._stop.set()
        if self._hb_thread:
            self._hb_thread.join(timeout=2)

    # ---- 周期心跳（保活 + 续租） ----
    def _start_heartbeat_loop(self) -> None:
        self._stop.clear()

        def _loop():
            while not self._stop.wait(self.heartbeat_interval):
                if self.node_id:
                    self.heartbeat(self.node_id, self._last_progress)

        self._hb_thread = threading.Thread(target=_loop, daemon=True,
                                           name="shard-heartbeat")
        self._hb_thread.start()


def build_coordinator(cfg) -> ShardCoordinator:
    """按配置构建协调器。distributed.enabled=false 时一律本地实现。"""
    dist = cfg.get("distributed", default={}) or {}
    if not dist.get("enabled", False):
        return LocalShardCoordinator()
    kind = str(dist.get("coordinator", "local"))
    if kind == "local":
        return LocalShardCoordinator()
    if kind == "http":
        url = str(dist.get("coordinator_url", "") or "")
        if not url:
            log.warning("distributed.coordinator=http 但未配置 coordinator_url，回退 local")
            return LocalShardCoordinator()
        return HttpShardCoordinator(
            url,
            shard_total=int(dist.get("shard_total", 1)),
            shard_index=int(dist.get("shard_index", 0)),
            heartbeat_interval_s=float(dist.get("heartbeat_interval_s", 15)))
    # 预留：etcd/redis 协调器在此接入
    log.warning("未知协调器类型 %r，回退 local", kind)
    return LocalShardCoordinator()


if __name__ == "__main__":  # pragma: no cover —— 独立运行协调器服务
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="NetAtlas 分片协调器服务")
    parser.add_argument("--serve", action="store_true", help="启动协调器 HTTP 服务")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--lease-ttl", type=float, default=DEFAULT_LEASE_TTL_S)
    cli = parser.parse_args()
    if cli.serve:
        server = CoordinatorServer(cli.host, cli.port, cli.lease_ttl)
        server.start()
        print(f"协调器运行中: {server.url}（Ctrl+C 退出）")
        try:
            threading.Event().wait()
        except KeyboardInterrupt:
            server.stop()
