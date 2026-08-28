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
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.l7")
MODULE = "l7_grabber"

PORT_PROTOCOL = {80: "http", 8080: "http", 8000: "http", 8888: "http",
                 443: "https", 8443: "https",
                 22: "ssh", 21: "ftp", 25: "smtp", 587: "smtp",
                 53: "dns", 110: "pop3", 143: "imap", 993: "tls", 995: "tls"}


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
        async with sem:
            try:
                if proto in ("http", "https"):
                    return await self._grab_http(ip, port, tls=(proto == "https"), base=hit)
                return await self._grab_banner(ip, port, proto, base=hit)
            except Exception as e:  # noqa: BLE001
                REGISTRY.incr(MODULE, "errors")
                return {**hit, "protocol": proto, "error": str(e)[:200],
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
        raw = await asyncio.wait_for(reader.read(self.body_limit + 8192),
                                     timeout=self.cfg.get("l7", "read_timeout_ms", default=5000) / 1000)
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
            except Exception:  # noqa: BLE001
                cert = None
        import re
        m = re.search(rb"<title[^>]*>(.*?)</title>", body[:16384], re.I | re.S)
        title = m.group(1).decode("utf-8", "replace").strip()[:200] if m else None
        return {**base, "protocol": "https" if tls else "http", "rtt_ms": rtt_ms,
                "http": {"status": status, "headers": headers, "title": title,
                         "body_sha1_len": len(body)},
                "tls": {"cert": cert} if cert else {},
                "body_sample": body[: self.body_limit].decode("utf-8", "replace") if body else "",
                "ts": int(time.time())}

    async def _grab_banner(self, ip: str, port: int, proto: str, base: dict) -> dict:
        t0 = time.perf_counter()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port),
            timeout=self.cfg.get("l7", "connect_timeout_ms", default=3000) / 1000)
        rtt_ms = round((time.perf_counter() - t0) * 1000, 1)
        try:
            banner = await asyncio.wait_for(reader.read(2048), timeout=4.0)
        except asyncio.TimeoutError:
            banner = b""
        writer.close()
        return {**base, "protocol": proto, "rtt_ms": rtt_ms,
                "banner": banner.decode("utf-8", "replace")[:1024],
                "ts": int(time.time())}

    # ---------- ZGrab2 引擎 ----------
    def _run_zgrab2(self) -> None:
        """流式驱动 zgrab2 multiple 模式。"""
        proto_ports: dict[str, list[int]] = {}
        for p, name in PORT_PROTOCOL.items():
            proto_ports.setdefault(name, []).append(p)
        ini = self.cfg.root / "config" / "zgrab2-multi.ini"
        if not ini.exists():
            log.warning("缺少 %s，zgrab2 引擎回退 python", ini)
            self._run_python()
            return
        proc = subprocess.Popen([self.zgrab2, "multiple", "-c", str(ini)],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                text=True, bufsize=1, errors="replace")
        REGISTRY.set_extra(MODULE, engine="zgrab2", pid=proc.pid)

        def feeder():
            while not self._stop.is_set():
                try:
                    hit = self.in_q.get(timeout=0.5)
                except queue.Empty:
                    continue
                tag = PORT_PROTOCOL.get(hit["port"], "http")
                proc.stdin.write(f"{hit['ip']},,{tag},{hit['port']}\n")
                proc.stdin.flush()

        ft = threading.Thread(target=feeder, daemon=True)
        ft.start()
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
            self.out_q.put(rec)
            REGISTRY.incr(MODULE, "grabbed")
        proc.terminate()
