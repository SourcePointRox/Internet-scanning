"""统一错误处理基础设施：重试 / 结构化错误上报 / 降级策略。

取代散落的 ``except Exception: pass`` 模式：
- ``retry``：指数退避重试装饰器/函数，仅重试瞬时错误（超时、连接重置、临时 IO）；
- ``ErrorReporter``：进程内结构化错误总线，各模块上报而非吞掉异常，
  WebUI ``/api/errors`` 可查，可选 webhook 外发告警；
- ``degrade``：记录降级事件（如 GeoIP 缺失、zgrab2 不可用），状态可查。
"""
from __future__ import annotations

import functools
import logging
import threading
import time
from collections import deque
from typing import Any, Callable, TypeVar

log = logging.getLogger("netatlas.errors")

F = TypeVar("F", bound=Callable[..., Any])

# 默认视为"瞬时、值得重试"的异常类型
TRANSIENT = (TimeoutError, ConnectionError, InterruptedError)


class ErrorReporter:
    """线程安全的结构化错误环形缓冲 + 告警钩子。"""

    _instance: "ErrorReporter | None" = None
    _instance_lock = threading.Lock()

    def __init__(self, maxlen: int = 500):
        self._lock = threading.Lock()
        self._buf: deque[dict] = deque(maxlen=maxlen)
        self._seq = 0
        self._hooks: list[Callable[[dict], None]] = []
        self._counts: dict[str, int] = {}

    @classmethod
    def get(cls) -> "ErrorReporter":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def add_hook(self, hook: Callable[[dict], None]) -> None:
        """注册告警钩子（如 webhook 外发）。钩子异常不影响主流程。"""
        self._hooks.append(hook)

    def report(self, module: str, message: str, *, level: str = "error",
               exc: BaseException | None = None, **ctx: Any) -> dict:
        """上报一条结构化错误/告警事件。"""
        with self._lock:
            self._seq += 1
            event = {
                "seq": self._seq, "ts": int(time.time()), "module": module,
                "level": level, "message": str(message)[:500],
                "exception": f"{type(exc).__name__}: {exc}" if exc else None,
                "context": {k: str(v)[:200] for k, v in ctx.items()},
            }
            self._buf.append(event)
            self._counts[module] = self._counts.get(module, 0) + 1
        logger = logging.getLogger(f"netatlas.{module}")
        (logger.warning if level == "warning" else logger.error)(
            "%s%s", message, f" ({type(exc).__name__}: {exc})" if exc else "")
        for hook in self._hooks:
            try:
                hook(event)
            except Exception:  # noqa: BLE001 —— 告警钩子自身绝不能影响主流程
                log.debug("alert hook failed", exc_info=True)
        return event

    def degrade(self, module: str, feature: str, reason: str) -> dict:
        """记录功能降级事件（warning 级）。"""
        return self.report(module, f"功能降级: {feature} —— {reason}",
                           level="warning", feature=feature)

    def recent(self, n: int = 50, module: str | None = None) -> list[dict]:
        with self._lock:
            items = list(self._buf)
        if module:
            items = [e for e in items if e["module"] == module]
        return items[-n:]

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


def retry(max_attempts: int = 3, backoff_s: float = 0.3, backoff_factor: float = 2.0,
          retry_on: tuple[type[BaseException], ...] = TRANSIENT,
          module: str = "core", on_giveup: str = "raise") -> Callable[[F], F]:
    """指数退避重试装饰器。

    on_giveup: "raise" 重新抛出最后一次异常；"none" 返回 None（由调用方降级处理）。
    重试耗尽时向 ErrorReporter 上报，杜绝静默失败。
    """
    def deco(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = backoff_s
            last: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except retry_on as e:
                    last = e
                    if attempt < max_attempts:
                        time.sleep(delay)
                        delay *= backoff_factor
                except Exception as e:  # 非瞬时错误：不重试，直接上报抛出
                    ErrorReporter.get().report(module, f"{fn.__name__} 非瞬时错误，不重试", exc=e)
                    raise
            reporter = ErrorReporter.get()
            if on_giveup == "none":
                reporter.report(module, f"{fn.__name__} 重试 {max_attempts} 次后放弃（降级返回 None）",
                                level="warning", exc=last)
                return None
            reporter.report(module, f"{fn.__name__} 重试 {max_attempts} 次后失败", exc=last)
            raise last  # type: ignore[misc]
        return wrapper  # type: ignore[return-value]
    return deco
