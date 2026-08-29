"""L7 应用层抓取模块。

双引擎：
1. **zgrab2**（首选，外部 Go 进程）：CSV 行(ip,domain,tag,port) 经 stdin 喂入，
   NDJSON 从 stdout 回收 —— 33+ 协议完整握手 transcript。
2. **python 内置异步引擎**（兜底）：asyncio 实现的 HTTP/HTTPS/TLS/banner 抓取，
   支持并发信号量、每连接读上限、RTT 测量、UA 标识（合规要求）。

输入：L4 模块产出的存活队列 {"ip","port","proto",...}
输出：统一 NDJSON schema 写入 l7 输出队列。
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import shutil
import ssl
import subprocess
import threading
import time
from pathlib import Path

from orchestrator.config import Config
from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.l7")
MODULE = "l7_grabber"

# 瞬时网络错误：值得按 l7.retry 配置重试一次
_TRANSIENT = (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError)

# 端口 -> 服务名（Python 引擎据此选择握手方式）
# 第二段为常见 HTTP 协议端口（7547 TR-069 / 9200 ES / 2375 Docker / 50070 Hadoop 等）
PORT_PROTOCOL = {80: "http", 8080: "http", 8000: "http", 8888: "http",
                 3000: "http", 9000: "http", 9200: "http",
                 7547: "http", 2375: "http", 50070: "http",
                 443: "https", 8443: "https",
                 22: "ssh", 23: "telnet", 21: "ftp", 25: "smtp", 465: "smtp", 587: "smtp",
                 110: "pop3", 143: "imap", 993: "tls", 995: "tls",
                 3306: "mysql", 6379: "redis", 27017: "mongodb",
                 5432: "postgres", 1433: "mssql", 123: "ntp"}

# 服务名 -> zgrab2 模块名（v0.1.8 无 https 模块，HTTPS 改走 tls 模块抓取证书链）
ZGRAB2_MODULE = {
    "https": "tls", "pop3": "pop3", "imap": "imap", "mysql": "mysql",
    "redis": "redis", "mongodb": "mongodb", "postgres": "postgres",
    "mssql": "mssql", "ntp": "ntp", "telnet": "telnet",
}


class L7Grabber:
    def __init__(self, cfg: Config, in_queue: "queue.Queue[dict]", out_queue: "queue.Queue[dict]",
                 bandwidth=None):
        self.cfg = cfg
        self.in_q = in_queue
        self.out_q = out_queue
        self.bw = bandwidth
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.engine = self._select_engine()
        self.body_limit = int(cfg.get("l7", "body_limit_bytes", default=65536))
        self.ua = cfg.get("project", "user_agent", default="NetAtlas/1.0")

    def _select_engine(self) -> str:
        pref = self.cfg.get("l7", "engine", default="auto")
        zgrab = shutil.which("zgrab2") or (self.cfg.abs_path("paths", "bin") / "zgrab2.exe")
        if pref == "zgrab2" or (pref == "auto" and zgrab and Path(str(zgrab)).exists()):
            self.zgrab2 = str(zgrab)
            return "zgrab2"
        return "python"

    # ---------- 生命周期 ----------
    def start(self) -> None:
        self._stop.clear()
        t = threading.Thread(target=self._run_python if self.engine == "python" else self._run_zgrab2,
                             daemon=True, name="l7-grabber")
        t.start()
        self._threads.append(t)
        REGISTRY.set_running(MODULE, True)
        REGISTRY.set_extra(MODULE, engine=self.engine)

    def stop(self) -> None:
        self._stop.set()
        REGISTRY.set_running(MODULE, False)

    # ---------- Python 异步引擎 ----------
    def _run_python(self) -> None:
        asyncio.run(self._python_loop())

    async def _python_loop(self) -> None:
        conc = int(self.cfg.get("l7", "concurrency", default=512))
        sem = asyncio.Semaphore(conc)
        pending: set[asyncio.Task] = set()
        loop = asyncio.get_running_loop()

        while not self._stop.is_set():
            try:
                hit = await loop.run_in_executor(None, self.in_q.get, True, 0.5)
            except queue.Empty:
                if pending:
                    done, pending = await asyncio.wait(pending, timeout=0.1,
                                                       return_when=asyncio.FIRST_COMPLETED)
                    for d in done:
                        if not d.cancelled() and d.exception() is None and d.result():
                            self.out_q.put(d.result())
                            REGISTRY.incr(MODULE, "grabbed")
                continue
            task = asyncio.create_task(self._grab_one(sem, hit))
            pending.add(task)

    async def _grab_one(self, sem: asyncio.Semaphore, hit: dict) -> dict | None:
        ip, port = hit["ip"], hit["port"]
        proto = PORT_PROTOCOL.get(port, "banner")
        retry_cfg = self.cfg.get("l7", "retry", default={}) or {}
        attempts = int(retry_cfg.get("max_attempts", 1))
        backoff = float(retry_cfg.get("backoff_ms", 300)) / 1000
        last_err: Exception | None = None
        async with sem:
            for attempt in range(1, attempts + 1):
                try:
                    if proto in ("http", "https"):
                        return await self._grab_http(ip, port, tls=(proto == "https"), base=hit)
                    return await self._grab_banner(ip, port, proto, base=hit)
                except _TRANSIENT as e:
                    last_err = e
                    if attempt < attempts:
                        await asyncio.sleep(backoff * attempt)
                    continue
                except Exception as e:  # noqa: BLE001 —— 抓取错误即数据（记录落盘），但计数上报
                    REGISTRY.incr(MODULE, "errors")
                    return {**hit, "protocol": proto, "error": f"{type(e).__name__}: {e}"[:200],
                            "ts": int(time.time())}
        REGISTRY.incr(MODULE, "errors")
        REGISTRY.incr(MODULE, "retries_exhausted")
        return {**hit, "protocol": proto,
                "error": f"{type(last_err).__name__}: {last_err}"[:200],
                "ts": int(time.time())}

    async def _grab_http(self, ip: str, port: int, tls: bool, base: dict) -> dict:
        t0 = time.perf_counter()
        ssl_ctx = None
        if tls:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port, ssl=ssl_ctx),
            timeout=self.cfg.get("l7", "connect_timeout_ms", default=3000) / 1000)
        rtt_ms = round((time.perf_counter() - t0) * 1000, 1)
        req = (f"GET / HTTP/1.1\r\nHost: {ip}\r\nUser-Agent: {self.ua}\r\n"
               f"Accept: */*\r\nConnection: close\r\n\r\n")
        writer.write(req.encode())
        await writer.drain()
        if self.bw:
            self.bw.acquire("l7_grab", len(req))
        # read(n) 在首个 TCP 段到达即返回 —— 必须循环读到 EOF/限量，
        # 否则 headers 与 body 分段到达时正文丢失
        raw = await self._read_to_eof(reader, self.body_limit + 8192,
                                      self.cfg.get("l7", "read_timeout_ms", default=5000) / 1000)
        writer.close()
        if self.bw:
            self.bw.acquire("l7_grab", len(raw))
        head, _, body = raw.partition(b"\r\n\r\n")
        lines = head.decode("latin-1", "replace").split("\r\n")
        status = lines[0] if lines else ""
        headers = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        # TLS 证书提取
        cert = None
        if tls:
            try:
                sslobj = writer.get_extra_info("ssl_object")
                if sslobj:
                    cert = sslobj.getpeercert(binary_form=False) or None
            except (ssl.SSLError, ValueError, AttributeError) as e:
                log.debug("证书提取失败 %s:%d: %s", ip, port, e)
                REGISTRY.incr(MODULE, "cert_parse_errors")
        import re
        m = re.search(rb"<title[^>]*>(.*?)</title>", body[:16384], re.I | re.S)
        title = m.group(1).decode("utf-8", "replace").strip()[:200] if m else None
        return {**base, "protocol": "https" if tls else "http", "rtt_ms": rtt_ms,
                "http": {"status": status, "headers": headers, "title": title,
                         "body_sha1_len": len(body)},
                "tls": {"cert": cert} if cert else {},
                "body_sample": body[: self.body_limit].decode("utf-8", "replace") if body else "",
                "ts": int(time.time())}

    @staticmethod
    async def _read_to_eof(reader: asyncio.StreamReader, limit: int,
                           timeout_s: float) -> bytes:
        """循环读取直到 EOF 或达到 limit；整体超时返回已收部分（不丢数据）。"""
        chunks: list[bytes] = []
        remaining = limit
        try:
            while remaining > 0:
                chunk = await asyncio.wait_for(reader.read(min(16384, remaining)),
                                               timeout=timeout_s)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
        except asyncio.TimeoutError:
            pass  # 对端不关闭连接：用已收到的部分继续解析
        return b"".join(chunks)

    async def _grab_banner(self, ip: str, port: int, proto: str, base: dict) -> dict:
        t0 = time.perf_counter()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port),
            timeout=self.cfg.get("l7", "connect_timeout_ms", default=3000) / 1000)
        rtt_ms = round((time.perf_counter() - t0) * 1000, 1)
        banner = await self._read_to_eof(reader, 2048, 4.0)
        writer.close()
        return {**base, "protocol": proto, "rtt_ms": rtt_ms,
                "banner": banner.decode("utf-8", "replace")[:1024],
                "ts": int(time.time())}

    # ---------- ZGrab2 引擎 ----------
    def _run_zgrab2(self) -> None:
        """按协议启动独立 zgrab2 进程（multiple 模式在部分版本不可用）。

        每个 (端口 -> 协议模块) 组合对应一个常驻 zgrab2 进程：
            zgrab2 <module> -p <port>       # 从 stdin 读 IP，向 stdout 输出 NDJSON
        本线程负责分发目标并为每个进程起一个读取线程。
        """
        workers: dict[int, dict] = {}

        def spawn(port: int, service: str) -> dict | None:
            module = ZGRAB2_MODULE.get(service, service)
            # --flush：每行结果立即刷新（管道模式下默认块缓冲，否则拿不到实时输出）
            # --senders：限制每进程并发。默认 1000/进程 × 数十个进程会耗尽内存与端口，
            #           32/进程 × ~40 进程 = ~1300 并发，对本项目吞吐（<50 目标/s）足够
            senders = str(self.cfg.get("l7", "zgrab2_senders", default=32))
            cmd = [self.zgrab2, "--senders", senders, module, "-p", str(port), "--flush"]
            try:
                proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, text=True, bufsize=1,
                                        errors="replace")
            except OSError as e:
                ErrorReporter.get().report(MODULE, f"zgrab2 进程启动失败（{module}:{port}）", exc=e)
                REGISTRY.incr(MODULE, "spawn_errors")
                return None
            w = {"proc": proc, "module": module, "service": service}
            workers[port] = w
            threading.Thread(target=self._zgrab_reader, args=(proc, port, service),
                             daemon=True, name=f"zgrab-{module}-{port}").start()
            log.info("启动 zgrab2 进程: %s（端口 %d，pid=%s）", " ".join(cmd), port, proc.pid)
            REGISTRY.set_extra(MODULE, **{f"proc_{module}_{port}": proc.pid})
            return w

        while not self._stop.is_set():
            try:
                hit = self.in_q.get(timeout=0.5)
            except queue.Empty:
                continue
            port = int(hit["port"])
            service = PORT_PROTOCOL.get(port)
            if service is None:
                # 无对应协议模块的端口（53/111/135/139/445/3389/5900/11211/1883 等）：
                # 不发起 L7 握手，但保留 L4 存活事实（开放端口本身就是核心数据）
                self.out_q.put({**hit, "protocol": "unknown", "l7_probed": False,
                                "ts": int(time.time())})
                REGISTRY.incr(MODULE, "skipped_no_module")
                continue
            w = workers.get(port) or spawn(port, service)
            if w is None:
                # spawn 失败：记录存活事实但标记未探测，目标不丢失
                self.out_q.put({**hit, "protocol": service, "l7_probed": False,
                                "error": "zgrab2 spawn failed", "ts": int(time.time())})
                continue
            try:
                w["proc"].stdin.write(f"{hit['ip']}\n")
                w["proc"].stdin.flush()
            except (BrokenPipeError, ValueError, OSError) as e:
                ErrorReporter.get().report(MODULE, f"zgrab2 管道断裂（端口 {port}），重启进程",
                                           level="warning", exc=e)
                w = spawn(port, service)  # 进程异常退出则重启
                if w is None:
                    self.out_q.put({**hit, "protocol": service, "l7_probed": False,
                                    "error": "zgrab2 respawn failed", "ts": int(time.time())})
                    continue
                try:
                    w["proc"].stdin.write(f"{hit['ip']}\n")
                    w["proc"].stdin.flush()
                except (BrokenPipeError, ValueError, OSError):
                    REGISTRY.incr(MODULE, "stdin_errors")
                    continue

        for w in workers.values():
            try:
                w["proc"].terminate()
            except OSError as e:
                log.debug("zgrab2 terminate: %s", e)

    def _zgrab_reader(self, proc: subprocess.Popen, port: int, service: str) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            if self._stop.is_set():
                break
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                self.out_q.put(self._normalize_zgrab(rec, port, service))
                REGISTRY.incr(MODULE, "grabbed")
            except (KeyError, TypeError, ValueError) as e:
                REGISTRY.incr(MODULE, "normalize_errors")
                ErrorReporter.get().report(MODULE, "zgrab2 输出归一化失败", level="warning",
                                           exc=e)

    @staticmethod
    def _normalize_zgrab(rec: dict, port: int, service: str) -> dict:
        """把 zgrab2 输出归一化为本项目 schema（供富化/分类模块消费）。

        zgrab2 输出形如：{"ip": "...", "data": {"tls": {"status":..., "result": {...}}}}
        归一化为：{"ip","port","protocol","ts","http":{...},"tls":{...},"zgrab_data":{...}}
        """
        import time
        data = rec.get("data", {}) or {}
        out = {"ip": rec.get("ip"), "port": port, "protocol": service,
               "ts": int(time.time()), "engine": "zgrab2"}
        mod = next(iter(data), None) if isinstance(data, dict) else None
        if mod:
            blk = data[mod] or {}
            out["zgrab_status"] = blk.get("status")
            result = blk.get("result", {}) or {}
            if mod == "http":
                resp = result.get("response", {}) or {}
                out["http"] = {
                    "status": resp.get("status_line", ""),
                    "headers": resp.get("headers", {}) or {},
                    "title": None,
                    "body_len": len((resp.get("body") or "") if isinstance(resp.get("body"), str)
                                    else str(resp.get("body") or "")),
                }
                out["body_sample"] = (resp.get("body") or "")[:4096] \
                    if isinstance(resp.get("body"), str) else ""
            elif mod == "tls":
                hl = result.get("handshake_log", {}) or {}
                certs = hl.get("server_certificates", {}) or {}
                chain = certs.get("chain", []) or []
                # zgrab2 v0.1.8 证书结构：chain[i].parsed.{subject_dn,issuer_dn,validity,...}
                summary = []
                for c in chain:
                    parsed = (c or {}).get("parsed", {}) or {}
                    subject = parsed.get("subject", {}) or {}
                    issuer = parsed.get("issuer", {}) or {}
                    validity = parsed.get("validity", {}) or {}
                    exts = parsed.get("extensions", {}) or {}
                    summary.append({
                        "subject_dn": parsed.get("subject_dn"),
                        "common_name": subject.get("common_name"),
                        "issuer_dn": parsed.get("issuer_dn"),
                        "issuer_cn": issuer.get("common_name"),
                        "not_before": validity.get("start"),
                        "not_after": validity.get("end"),
                        "alt_names": exts.get("subject_alt_name", {}).get("dns_names")
                        if isinstance(exts.get("subject_alt_name"), dict) else None,
                        "key_algorithm": (parsed.get("subject_key_info", {}) or {}).get(
                            "key_algorithm", {}).get("name")
                        if isinstance(parsed.get("subject_key_info"), dict) else None,
                    })
                out["tls"] = {
                    "server_hello": hl.get("server_hello", {}),
                    "server_certificates": certs,
                    "cert_summary": summary,
                }
            else:
                out["banner"] = str(result)[:1024]
            if blk.get("status") != "success":
                out["error"] = str(blk.get("error"))[:200] if blk.get("error") else \
                    f"zgrab status={blk.get('status')}"
        return out
