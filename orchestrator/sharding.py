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
import logging
import os
import socket
import time
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


def build_coordinator(cfg) -> ShardCoordinator:
    """按配置构建协调器。distributed.enabled=false 时一律本地实现。"""
    dist = cfg.get("distributed", default={}) or {}
    if not dist.get("enabled", False):
        return LocalShardCoordinator()
    kind = str(dist.get("coordinator", "local"))
    if kind == "local":
        return LocalShardCoordinator()
    # 预留：http/etcd/redis 协调器在此接入
    log.warning("未知协调器类型 %r，回退 local（分布式接口预留中）", kind)
    return LocalShardCoordinator()
