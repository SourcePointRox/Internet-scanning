"""统一并发抽象：消除 threading / asyncio / subprocess 混杂的编程模型。

项目内所有"从队列取任务 -> 并发执行 -> 写结果队列"的消费者统一实现
``QueueConsumer`` 协议：

- ``ThreadPoolConsumer``：阻塞型工作（兼容旧代码、C 扩展释放 GIL 的场景）；
- ``AsyncConsumer``：IO 密集型工作（asyncio，单线程事件循环 + 信号量并发），
  是网络类消费者（L7 抓取、DNS 富化）的默认选择。

外部子进程（masscan / zgrab2）统一由 ``SupervisedProcess`` 管理：
看门狗、崩溃自动重启（带退避）、结构化错误上报。
"""
from __future__ import annotations

import asyncio
import logging
import queue
import subprocess
import threading
import time
from typing import Any, Callable

from orchestrator.errors import ErrorReporter

log = logging.getLogger("netatlas.concurrency")


class QueueConsumer:
    """消费者协议：start/stop 生命周期 + 背压安全。"""

    def start(self) -> None:  # pragma: no cover - 接口定义
        raise NotImplementedError

    def stop(self) -> None:  # pragma: no cover - 接口定义
        raise NotImplementedError


class ThreadPoolConsumer(QueueConsumer):
    """N 个消费线程从 in_q 取任务，handler 处理后可选写入 out_q。"""

    def __init__(self, name: str, in_q: "queue.Queue[dict]",
                 handler: Callable[[dict], Any], *, workers: int = 4,
                 out_q: "queue.Queue[dict] | None" = None, module: str = "core"):
        self.name, self.in_q, self.handler = name, in_q, handler
        self.workers, self.out_q, self.module = workers, out_q, module
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._stop.clear()
        self._threads = [threading.Thread(target=self._run, daemon=True,
                                          name=f"{self.name}-{i}") for i in range(self.workers)]
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rec = self.in_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                result = self.handler(rec)
                if result is not None and self.out_q is not None:
                    self.out_q.put(result)
            except Exception as e:  # noqa: BLE001 —— 单条失败不拖垮消费者，但必须上报
                ErrorReporter.get().report(self.module, "消费记录处理失败", exc=e)


class AsyncConsumer(QueueConsumer):
    """asyncio 单事件循环消费者：信号量限并发，handler 为协程。

    网络 IO 场景（L7 握手、异步 DNS）用协程替代线程：
    无 GIL 争用、无每线程栈内存开销、并发可上千。
    """

    def __init__(self, name: str, in_q: "queue.Queue[dict]",
                 coro_handler: Callable[[dict], Any], *, concurrency: int = 512,
                 out_q: "queue.Queue[dict] | None" = None, module: str = "core",
                 max_pending: int | None = None):
        self.name, self.in_q, self.coro_handler = name, in_q, coro_handler
        self.concurrency, self.out_q, self.module = concurrency, out_q, module
        self.max_pending = max_pending or concurrency * 4
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.processed = 0
        self.errors = 0

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=lambda: asyncio.run(self._loop()),
                                        daemon=True, name=self.name)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    async def _loop(self) -> None:
        sem = asyncio.Semaphore(self.concurrency)
        pending: set[asyncio.Task] = set()
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                rec = await loop.run_in_executor(None, self.in_q.get, True, 0.5)
            except queue.Empty:
                rec = None
            if rec is not None:
                pending.add(asyncio.create_task(self._run_one(sem, rec)))
            if rec is None or len(pending) >= self.max_pending:
                if pending:
                    done, pending = await asyncio.wait(
                        pending, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
                    for d in done:
                        if d.cancelled():
                            continue
                        if d.exception() is not None:
                            self.errors += 1
                            ErrorReporter.get().report(
                                self.module, "异步任务失败", exc=d.exception())
                            continue
                        result = d.result()
                        self.processed += 1
                        if result is not None and self.out_q is not None:
                            self.out_q.put(result)

    async def _run_one(self, sem: asyncio.Semaphore, rec: dict) -> Any:
        async with sem:
            return await self.coro_handler(rec)


class SupervisedProcess:
    """外部进程看门狗：崩溃自动重启（指数退避），错误结构化上报。

    用于 masscan / zgrab2 等子进程的统一管理，替代散落的 try/except 重启逻辑。
    """

    def __init__(self, name: str, cmd_factory: Callable[[], list[str]],
                 *, module: str = "core", max_restarts: int = 5,
                 stdin: bool = False, on_stdout_line: Callable[[str], None] | None = None):
        self.name, self.cmd_factory = name, cmd_factory
        self.module, self.max_restarts = module, max_restarts
        self.on_stdout_line = on_stdout_line
        self._use_stdin = stdin
        self.proc: subprocess.Popen | None = None
        self.restarts = 0
        self._stop = threading.Event()
        self._reader: threading.Thread | None = None

    def launch(self) -> subprocess.Popen:
        cmd = self.cmd_factory()
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE if self._use_stdin else None,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, errors="replace")
        if self.on_stdout_line is not None and self.proc.stdout is not None:
            self._reader = threading.Thread(target=self._read_loop, daemon=True,
                                            name=f"{self.name}-reader")
            self._reader.start()
        return self.proc

    def _read_loop(self) -> None:
        assert self.proc is not None and self.proc.stdout is not None
        try:
            for line in self.proc.stdout:
                if self._stop.is_set():
                    break
                try:
                    self.on_stdout_line(line)  # type: ignore[misc]
                except Exception as e:  # noqa: BLE001
                    ErrorReporter.get().report(self.module, "子进程输出行处理失败", exc=e)
        except (OSError, ValueError) as e:
            if not self._stop.is_set():
                ErrorReporter.get().report(self.module, "子进程输出流中断", exc=e)

    def ensure_alive(self) -> bool:
        """检查并在需要时重启进程。返回当前是否存活。"""
        if self._stop.is_set():
            return False
        if self.proc is not None and self.proc.poll() is None:
            return True
        if self.restarts >= self.max_restarts:
            ErrorReporter.get().report(
                self.module, f"子进程 {self.name} 重启次数耗尽（{self.max_restarts}），放弃")
            return False
        backoff = min(30.0, 2.0 ** self.restarts)
        self.restarts += 1
        ErrorReporter.get().report(
            self.module, f"子进程 {self.name} 退出，{backoff:.0f}s 后第 {self.restarts} 次重启",
            level="warning")
        time.sleep(backoff)
        try:
            self.launch()
            return True
        except OSError as e:
            ErrorReporter.get().report(self.module, f"子进程 {self.name} 重启失败", exc=e)
            return False

    def write_stdin(self, text: str) -> bool:
        if self.proc is None or self.proc.stdin is None or self.proc.poll() is not None:
            return False
        try:
            self.proc.stdin.write(text)
            self.proc.stdin.flush()
            return True
        except (BrokenPipeError, ValueError, OSError):
            return False

    def terminate(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
