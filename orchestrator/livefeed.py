"""实时扫描信息流：线程安全环形缓冲。

分类器每处理一条记录即 push 一条摘要，WebUI 通过 WebSocket 增量拉取，
前端滚动展示（时间 / IP / 端口 / 协议 / 域名 / 分类层级 / 标题）。
"""
from __future__ import annotations

import threading
import time
from collections import deque

_lock = threading.Lock()
_buf: deque[dict] = deque(maxlen=500)
_seq = 0


def push(item: dict) -> int:
    global _seq
    with _lock:
        _seq += 1
        entry = {"seq": _seq, "ts": int(time.time()), **item}
        _buf.append(entry)
        return _seq


def latest(n: int = 50) -> list[dict]:
    with _lock:
        return list(_buf)[-n:]


def since(seq: int, limit: int = 100) -> list[dict]:
    """增量拉取：返回 seq 之后的新条目（WebSocket 推送用）。"""
    with _lock:
        return [e for e in _buf if e["seq"] > seq][-limit:]
