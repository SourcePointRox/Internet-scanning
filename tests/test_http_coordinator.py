"""HTTP 分布式协调器测试：真实起本地 CoordinatorServer，验证完整协议。

覆盖：节点注册 / 心跳聚合 / 分片认领互补 / 首选序号 / 租约到期重认领 /
my_targets 过滤一致性 / 协调器不可达时的降级行为 / build_coordinator 接线。
"""
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.sharding import (CoordinatorServer, HashedSharder,
                                   HttpShardCoordinator, LocalShardCoordinator,
                                   build_coordinator)


class _Cfg:
    """最小 Config 替身（build_coordinator 只读 distributed 段）。"""

    def __init__(self, dist):
        self._dist = dist

    def get(self, *keys, default=None):
        if keys == ("distributed",):
            return self._dist
        return default


class ServerFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = CoordinatorServer("127.0.0.1", 0, lease_ttl_s=0.6)
        cls.server.start()
        cls.url = cls.server.url

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()


class TestHttpCoordinatorProtocol(ServerFixture):
    def test_register_heartbeat_and_global_progress(self):
        c = HttpShardCoordinator(self.url, shard_total=1, heartbeat_interval_s=60)
        c.register_node("node-a", {"shard_total": 1, "shard_index": 0})
        c.heartbeat("node-a", {"probes_sent": 1234, "open_found": 7})
        gp = c.global_progress()
        self.assertEqual(gp["coordinator"], "http")
        self.assertEqual(gp["probes_sent"], 1234)
        self.assertEqual(gp["open_found"], 7)
        nodes = c.active_nodes()
        self.assertTrue(any(n["node_id"] == "node-a" for n in nodes))
        c.close()

    def test_shard_claims_are_complementary(self):
        """两节点认领同一 shard_total=2：序号必须互补且不冲突。"""
        a = HttpShardCoordinator(self.url, shard_total=2, shard_index=0,
                                 heartbeat_interval_s=60)
        b = HttpShardCoordinator(self.url, shard_total=2, shard_index=0,
                                 heartbeat_interval_s=60)
        a.register_node("claim-a", {})
        b.register_node("claim-b", {})   # 首选 0 已被占，应退让到 1
        self.assertEqual({a._claimed, b._claimed}, {0, 1})
        self.assertEqual(a._claimed, 0)  # 先到先得，首选生效
        a.close()
        b.close()

    def test_lease_expiry_allows_reclaim(self):
        """节点宕机（不再续租）后，分片租约到期可被其他节点接管。"""
        a = HttpShardCoordinator(self.url, shard_total=3, shard_index=2,
                                 heartbeat_interval_s=60)
        a.register_node("lease-a", {})
        claimed = a._claimed
        a.close()                        # 模拟宕机：心跳停止
        time.sleep(0.8)                  # 租约 0.6s 到期
        b = HttpShardCoordinator(self.url, shard_total=3, shard_index=claimed,
                                 heartbeat_interval_s=60)
        b.register_node("lease-b", {})
        self.assertEqual(b._claimed, claimed, "到期分片必须可被新节点认领")
        b.close()

    def test_my_targets_matches_hashed_sharder(self):
        """HTTP 客户端的目标过滤必须与无状态 HashedSharder 完全一致。"""
        c = HttpShardCoordinator(self.url, shard_total=4, shard_index=1,
                                 heartbeat_interval_s=60)
        c.register_node("filter-node", {})
        targets = ["10.9.0.0/16", "172.31.0.0/20"]
        mine = c.my_targets(targets, 4, c._claimed)
        expected = []
        ref = HashedSharder(4, c._claimed)
        for cidr in targets:
            expected.extend(ref.filter_cidr(cidr))
        self.assertEqual(mine, expected)
        # 四分片合起来必须无交集地覆盖全部 /24
        union = [set(HashedSharder(4, i).filter_cidr("10.9.0.0/16")) for i in range(4)]
        for i in range(4):
            for j in range(i + 1, 4):
                self.assertFalse(union[i] & union[j])
        self.assertEqual(len(set().union(*union)), 256)
        c.close()


class TestDegradation(unittest.TestCase):
    DEAD_URL = "http://127.0.0.1:9"      # 保留端口，连接必被拒

    def test_unreachable_coordinator_degrades_gracefully(self):
        """协调器不可达：不抛异常、分片回落配置值、进度视图降级。"""
        c = HttpShardCoordinator(self.DEAD_URL, shard_total=2, shard_index=1,
                                 heartbeat_interval_s=60, timeout_s=0.5)
        c.register_node("lonely", {})
        self.assertEqual(c._claimed, 1)              # 回落配置值，绝不扩量
        gp = c.global_progress()
        self.assertIn("unreachable", gp["coordinator"])
        self.assertEqual(gp["nodes"], 1)
        nodes = c.active_nodes()
        self.assertEqual(len(nodes), 1)              # 降级：仅本节点视图
        mine = c.my_targets(["10.8.0.0/16"], 2, 1)
        self.assertEqual(mine, list(HashedSharder(2, 1).filter_cidr("10.8.0.0/16")))
        c.close()


class TestBuildCoordinatorWiring(unittest.TestCase):
    def test_disabled_falls_back_to_local(self):
        c = build_coordinator(_Cfg({"enabled": True, "coordinator": "http",
                                    "coordinator_url": ""}))
        self.assertIsInstance(c, LocalShardCoordinator)

    def test_http_kind_builds_client(self):
        c = build_coordinator(_Cfg({"enabled": True, "coordinator": "http",
                                    "coordinator_url": "http://127.0.0.1:8765",
                                    "shard_total": 8, "shard_index": 3}))
        self.assertIsInstance(c, HttpShardCoordinator)
        self.assertEqual(c.shard_total, 8)
        c.close()

    def test_unknown_kind_falls_back_to_local(self):
        c = build_coordinator(_Cfg({"enabled": True, "coordinator": "etcd"}))
        self.assertIsInstance(c, LocalShardCoordinator)


if __name__ == "__main__":
    unittest.main()
