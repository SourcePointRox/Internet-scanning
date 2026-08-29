"""L3/L4 无状态扫描模块：masscan 子进程封装（两阶段发现）。

阶段一：top 端口全网发现；阶段二：存活主机全端口。

可靠性设计：
- **断点续扫状态机**（orchestrator.persistence.ScanStateMachine）：
  目标/端口/分片/速率/masscan resume 文件全部落盘，进程崩溃重启后可续扫；
- **准实时调速**：masscan 不支持热调速 —— 按 ``l4.resume.segment_minutes``
  把扫描切成时间段分片，片间以新速率 + ``--resume`` 重启 masscan；
  AIMD 反馈（orchestrator.ratecontrol）可按丢包/网卡利用率自动升降速；
- **进程看门狗**：masscan 异常退出自动重启（指数退避，上限 5 次）；
- 无 masscan 二进制时自动进入 dry-run 模式（不发包，仅记录意图）。
"""
from __future__ import annotations

import ipaddress
import json
import logging
import queue
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from orchestrator.config import Config
from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.l4")
MODULE = "l4_scanner"


class ExcludeList:
    """加载并校验排除网段（config/exclude.txt）。"""

    def __init__(self, path: Path):
        self.networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.split("#", 1)[0].strip()
                if line:
                    try:
                        self.networks.append(ipaddress.ip_network(line, strict=False))
                    except ValueError:
                        ErrorReporter.get().report(MODULE, f"排除列表忽略非法条目: {line}",
                                                   level="warning")

    def contains(self, ip: str) -> bool:
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return True  # 非法 IP 一律视为排除
        return any(addr in n for n in self.networks)


class L4Scanner:
    def __init__(self, cfg: Config, out_queue: "queue.Queue[dict]", dry_run: bool = False,
                 bandwidth=None, scan_state=None, rate_controller=None,
                 sharder=None):
        self.cfg = cfg
        self.out = out_queue
        self.dry_run = dry_run
        self.bandwidth = bandwidth
        self.scan_state = scan_state          # ScanStateMachine | None
        self.rate_controller = rate_controller  # RateController | None
        self.sharder = sharder                # HashedSharder | None（分布式分片）
        self._proc: subprocess.Popen | None = None
        self._stop = threading.Event()
        self._pause = threading.Event()       # 暂停（保留状态可续扫）
        self._thread: threading.Thread | None = None
        self._restart_lock = threading.Lock()
        self.rate_pps = int(cfg.get("l4", "default_rate_pps", default=8000))
        bin_dir = cfg.abs_path("paths", "bin")
        self.masscan = self._find_masscan(bin_dir)
        self.exclude = ExcludeList(cfg.abs_path("l4", "exclude_file"))
        self.backend = self._select_backend()
        self._scapy = None
        self._probes_sent = 0
        self._opens = 0
        self._terminated_by_us = False  # 分片/调速主动终止标记（区别于真实崩溃）

    def _select_backend(self) -> str:
        if self.dry_run:
            return "dry-run"
        pref = str(self.cfg.get("l4", "scanner", default="auto")).lower()
        if pref == "masscan" or (pref == "auto" and self.masscan):
            return "masscan"
        if pref in ("scapy", "auto"):
            from modules.l4_scapy import ScapyL4Scanner
            ok, reason = ScapyL4Scanner.available()
            if ok:
                return "scapy"
            ErrorReporter.get().degrade(MODULE, "scapy 后端", reason)
            REGISTRY.set_extra(MODULE, scapy_error=reason)
        return "dry-run"

    @staticmethod
    def _find_masscan(bin_dir: Path) -> str | None:
        for cand in (bin_dir / "masscan.exe", bin_dir / "masscan"):
            if cand.exists():
                return str(cand)
        return shutil.which("masscan")

    # ---------- 生命周期 ----------
    def start(self, targets: list[str] | None = None, ports: str | None = None,
              resume: str | None = None) -> None:
        # 断点续扫：未显式指定 targets 且存在未完成扫描时，恢复上次状态
        prev = self.scan_state.snapshot() if self.scan_state else {}
        if (not targets and self.scan_state and self.scan_state.resumable()):
            targets = prev.get("targets") or None
            ports = ports or prev.get("ports") or None
            resume = resume or prev.get("resume_file")
            log.info("恢复未完成扫描（分片 %s，已探针 %s）",
                     prev.get("segment"), prev.get("probes_sent"))
            REGISTRY.incr(MODULE, "resumed_runs")
        targets = targets or ["0.0.0.0/0"]
        # 分布式分片：本节点仅扫描属于自己的块
        if self.sharder is not None:
            sharded: list[str] = []
            for cidr in targets:
                sharded.extend(self.sharder.filter_cidr(cidr))
            targets = sharded or targets
        ports = ports or self.cfg.get("l4", "phase1_ports")
        self._stop.clear()
        self._pause.clear()
        # 每次启动重新探测后端，便于用户中途放入 masscan 后热切换
        self.masscan = self._find_masscan(self.cfg.abs_path("paths", "bin"))
        self.backend = self._select_backend()
        REGISTRY.set_extra(MODULE, backend=self.backend)
        if self.scan_state and prev.get("state") != "RUNNING":
            self.scan_state.begin_run(list(targets), ports, self.rate_pps, resume,
                                      run_id=prev.get("run_id") or uuid.uuid4().hex[:12])
        if self.backend == "scapy":
            from modules.l4_scapy import ScapyL4Scanner
            self._scapy = ScapyL4Scanner(self.cfg, self.out, exclude=self.exclude,
                                         bandwidth=self.bandwidth,
                                         scan_state=self.scan_state)
            self._scapy.start(targets=targets, ports=ports, rate_pps=self.rate_pps)
            REGISTRY.set_running(MODULE, True)
            return
        self._thread = threading.Thread(
            target=self._run, args=(targets, ports, resume),
            daemon=True, name="l4-scanner",
        )
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        if self._scapy is not None:
            self._scapy.stop()
        self._terminate_proc()
        if self.scan_state and self.scan_state.snapshot().get("state") == "RUNNING":
            self.scan_state.transition("PAUSED")  # 主动停止 = 可续扫的暂停
        REGISTRY.set_running(MODULE, False)

    def pause(self) -> None:
        """暂停扫描（保留进度，可 resume）。"""
        self._pause.set()
        if self._scapy is not None:
            self._scapy.stop()               # scapy 后端也要真正停发，否则暂停形同虚设
        self._terminate_proc()
        if self.scan_state:
            self.scan_state.transition("PAUSED")

    def resume(self) -> None:
        """从暂停处继续（用 masscan --resume + 状态机恢复）。"""
        self._pause.clear()
        prev = self.scan_state.snapshot() if self.scan_state else {}
        self.start(targets=prev.get("targets") or None,
                   ports=prev.get("ports") or None,
                   resume=prev.get("resume_file"))

    def set_rate(self, pps: int) -> None:
        """调速：记录新速率，在下一个分片边界生效（masscan 无热调速）。"""
        max_pps = int(self.cfg.get("l4", "max_rate_pps", default=18000))
        self.rate_pps = max(100, min(pps, max_pps))
        if self.rate_controller is not None:
            self.rate_pps = self.rate_controller.set_rate(self.rate_pps)
        if self._scapy is not None:
            self._scapy.set_rate(self.rate_pps)  # scapy 后端支持真正的热调速
        REGISTRY.set_extra(MODULE, rate_pps=self.rate_pps)
        if self.scan_state:
            self.scan_state.progress(rate_pps=self.rate_pps)

    def _terminate_proc(self, grace_s: float = 2.0) -> None:
        """优雅终止 masscan（先 Ctrl-Break/SIGINT 让其写 paused.conf，再强杀）。

        注意：Windows 无控制台环境下 CTRL_BREAK 无法投递，grace_s 后必然
        走 terminate —— 此时退出码非 0，调用方须以 _terminated_by_us 区分。
        """
        proc = self._proc
        if proc and proc.poll() is None:
            try:
                if sys.platform == "win32":
                    proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                else:
                    proc.send_signal(signal.SIGINT)
                proc.wait(timeout=grace_s)
            except (subprocess.TimeoutExpired, OSError):
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

    # ---------- masscan 命令组装 ----------
    def _build_cmd(self, targets: list[str], ports: str, resume: str | None) -> list[str]:
        cmd = [
            self.masscan, *targets, f"-p{ports}",
            "--rate", str(self.rate_pps),
            "--excludefile", str(self.cfg.abs_path("l4", "exclude_file")),
            "--retries", str(self.cfg.get("l4", "retries", default=1)),
            "-oJ", "-",                      # JSON 流输出到 stdout
            "--output-status", "open",
        ]
        src_ip = str(self.cfg.get("l4", "source_ip") or "").strip()
        router_mac = str(self.cfg.get("l4", "router_mac") or "").strip()
        if src_ip and router_mac:            # 二层直连：两者必须同时配置
            cmd += ["--source-ip", src_ip, "--router-mac", router_mac]
        if resume and Path(resume).exists():
            cmd += ["--resume", resume]
        return [str(c) for c in cmd if c]

    def _resume_file(self) -> str:
        """masscan 中断时写 paused.conf 的位置（cwd = data/meta）。"""
        return str(self.cfg.abs_path("paths", "data_meta") / "paused.conf")

    # ---------- 主循环（分片式） ----------
    def _run(self, targets: list[str], ports: str, resume: str | None) -> None:
        if not self.masscan or self.dry_run or self.backend == "dry-run":
            log.warning("masscan 不可用或 dry-run：进入模拟模式（不发包）")
            self._simulate(targets)
            return
        segment_min = float(self.cfg.get("l4", "resume", "segment_minutes", default=10))
        adapt = bool(self.cfg.get("l4", "rate_adapt", "enabled", default=False))
        adapt_interval = float(self.cfg.get("l4", "rate_adapt", "interval_s", default=10))
        resume_file = self._resume_file()
        restarts = 0
        segment = (self.scan_state.snapshot().get("segment") or 0) if self.scan_state else 0
        meta_dir = self.cfg.abs_path("paths", "data_meta")
        meta_dir.mkdir(parents=True, exist_ok=True)

        try:
            while not self._stop.is_set() and not self._pause.is_set():
                cmd = self._build_cmd(targets, ports, resume)
                log.info("启动 masscan 分片 %d（rate=%d pps）: %s ...",
                         segment, self.rate_pps, " ".join(cmd[:6]))
                if self.scan_state:
                    self.scan_state.progress(segment=segment, rate_pps=self.rate_pps,
                                             resume_file=resume_file)
                creationflags = (subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
                                 if sys.platform == "win32" else 0)
                try:
                    self._proc = subprocess.Popen(
                        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, bufsize=1, errors="replace",
                        cwd=str(meta_dir), creationflags=creationflags)
                except OSError as e:
                    ErrorReporter.get().report(MODULE, "masscan 启动失败", exc=e)
                    if self.scan_state:
                        self.scan_state.transition("FAILED", error=str(e))
                    return
                rc, natural_end = self._consume(segment_min * 60, adapt, adapt_interval)
                by_us = self._terminated_by_us
                self._terminated_by_us = False
                if self._stop.is_set() or self._pause.is_set():
                    break
                if natural_end:                # masscan 自行跑完全量目标
                    if self.scan_state:
                        self.scan_state.transition("COMPLETED")
                    log.info("masscan 扫描完成（分片 %d）", segment)
                    return
                if not by_us and rc not in (0, None):
                    # 真实崩溃（非分片/调速主动终止）：退避重启
                    if restarts < 5:
                        restarts += 1
                        ErrorReporter.get().report(
                            MODULE, f"masscan 退出码 {rc}，{2 ** restarts}s 后第 {restarts} 次重启",
                            level="warning")
                        time.sleep(min(30, 2 ** restarts))
                    else:
                        ErrorReporter.get().report(MODULE, f"masscan 反复崩溃（退出码 {rc}），放弃")
                        if self.scan_state:
                            self.scan_state.transition("FAILED", error=f"exit code {rc}")
                        return
                resume = resume_file if Path(resume_file).exists() else resume
                segment += 1
        finally:
            self._terminate_proc()
            REGISTRY.set_running(MODULE, False)

    def _consume(self, segment_s: float, adapt: bool, adapt_interval: float) -> tuple[int | None, bool]:
        """消费 masscan 输出直到：分片时间到 / 进程退出 / 停止信号。

        返回 (退出码, 是否自然结束)。
        """
        assert self._proc is not None and self._proc.stdout is not None
        deadline = time.monotonic() + segment_s
        last_adapt = time.monotonic()
        proc = self._proc
        # 读取线程：readline 是阻塞调用，主循环必须能按时检查分片边界/调速/停止
        lines: "queue.Queue[str | None]" = queue.Queue()

        def _reader() -> None:
            try:
                for ln in proc.stdout:  # type: ignore[union-attr]
                    lines.put(ln)
            except (OSError, ValueError) as e:
                if not self._stop.is_set():
                    ErrorReporter.get().report(MODULE, "masscan 输出流读取失败", exc=e)
            finally:
                lines.put(None)

        threading.Thread(target=_reader, daemon=True, name="masscan-reader").start()
        while not self._stop.is_set() and not self._pause.is_set():
            if proc.poll() is not None:
                # 进程已退出：等待读取线程把管道尾部记录推完（sentinel None），防丢数据
                drain_deadline = time.monotonic() + 5.0
                while time.monotonic() < drain_deadline:
                    try:
                        ln = lines.get(timeout=0.2)
                    except queue.Empty:
                        continue
                    if ln is None:
                        break
                    self._handle_line(ln)
                return proc.returncode, proc.returncode == 0
            if time.monotonic() >= deadline:
                self._terminated_by_us = True   # 分片边界：主动终止，非崩溃
                self._terminate_proc()
                return proc.returncode, False
            if adapt and self.rate_controller and time.monotonic() - last_adapt >= adapt_interval:
                last_adapt = time.monotonic()
                new_rate, changed = self.rate_controller.decide(
                    probes_sent=self._probes_sent, open_found=self._opens)
                if changed:
                    self.rate_pps = new_rate
                    REGISTRY.set_extra(MODULE, rate_pps=new_rate,
                                       adapt=self.rate_controller.last_decision)
                    self._terminated_by_us = True  # 调速重启：主动终止，非崩溃
                    self._terminate_proc()
                    return proc.returncode, False
            try:
                ln = lines.get(timeout=0.2)
            except queue.Empty:
                continue
            if ln is None:
                # 输出流 EOF：进程即将/已经退出，先回收退出码再判断
                try:
                    rc = proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    rc = None
                return rc, rc == 0
            self._handle_line(ln)
        return proc.poll(), False

    def _handle_line(self, line: str) -> None:
        line = line.strip()
        if not (line.startswith("{") and line.endswith(("}", "},"))):
            if "rate:" in line or "found:" in line:
                # 解析 masscan 状态行（"rate: 8.00-kpps, 12.3% done, found: 42"）
                # 估算已发探针数，供 AIMD 调速与进度持久化
                import re
                m_rate = re.search(r"rate:\s*([\d.]+)-kpps", line)
                m_found = re.search(r"found:\s*(\d+)", line)
                m_done = re.search(r"([\d.]+)%\s*done", line)
                if m_rate:
                    kpps = float(m_rate.group(1))
                    now = time.monotonic()
                    last = getattr(self, "_last_status_ts", None)
                    if last is not None:        # 累计两状态行之间的估算探针数
                        self._probes_sent += int(kpps * 1000 * max(0.0, now - last))
                    self._last_status_ts = now
                if m_found:
                    REGISTRY.set_extra(MODULE, masscan_status=line[:200],
                                       masscan_found=int(m_found.group(1)),
                                       progress_pct=float(m_done.group(1)) if m_done else None)
                else:
                    REGISTRY.set_extra(MODULE, masscan_status=line[:200])
            return
        try:
            rec = json.loads(line.rstrip(","))
        except json.JSONDecodeError:
            REGISTRY.incr(MODULE, "parse_errors")
            return
        ip = rec.get("ip")
        if not ip or self.exclude.contains(ip):
            REGISTRY.incr(MODULE, "excluded")
            return
        self._opens += 1
        for port in rec.get("ports", []):
            self.out.put({
                "ip": ip, "port": port.get("port"),
                "proto": port.get("proto", "tcp"),
                "status": port.get("status", "open"),
                "ttl": rec.get("ttl"),
                "ts": rec.get("timestamp", int(time.time())),
                "family": 6 if ":" in ip else 4,
                "engine": "masscan",
            })
            REGISTRY.incr(MODULE, "open_ports")
        if self.scan_state and self._opens % 100 == 0:
            self.scan_state.progress(open_found=REGISTRY.snapshot()[MODULE]["counters"]
                                     .get("open_ports", 0))

    def _simulate(self, targets: list[str]) -> None:
        """dry-run：用回环地址制造少量伪发现，供流水线联调。"""
        REGISTRY.set_extra(MODULE, mode="dry-run")
        if self.scan_state:
            self.scan_state.begin_run(list(targets),
                                      self.cfg.get("l4", "phase1_ports", default="80"),
                                      self.rate_pps, None, run_id=uuid.uuid4().hex[:12])
        sim_ports = [80, 443, 22, 8080, 21]
        while not self._stop.is_set() and not self._pause.is_set():
            for p in sim_ports:
                if self._stop.is_set() or self._pause.is_set():
                    break
                self.out.put({"ip": "127.0.0.1", "port": p, "proto": "tcp",
                              "status": "open", "ttl": 128, "ts": int(time.time()), "family": 4,
                              "engine": "dry-run", "simulated": True})
                REGISTRY.incr(MODULE, "open_ports")
            time.sleep(1.0)
        if self.scan_state and self.scan_state.snapshot().get("state") == "RUNNING":
            self.scan_state.transition("PAUSED")
        REGISTRY.set_running(MODULE, False)
