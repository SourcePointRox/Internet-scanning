"""富化模块 BGP/开关接线测试：pyasn 离线 RIB、GeoIP 双开关、降级行为。

pyasn 为可选依赖（venv 中未安装），用桩模块注入 sys.modules 验证加载路径。
"""
import queue
import sys
import tempfile
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.enrich import BgpResolver, Enricher, GeoResolver


class _Cfg:
    def __init__(self, data):
        self._data = data
        self.root = Path(self._data.pop("_root", "."))

    def get(self, *keys, default=None):
        node = self._data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    def abs_path(self, *keys):
        p = Path(str(self.get(*keys)))
        return p if p.is_absolute() else self.root / p


class _FakePyasnDb:
    def __init__(self, path):
        self.path = path
        assert Path(path).exists(), "RIB 数据文件必须存在"

    def lookup(self, ip):
        if ip.startswith("203.0.114."):
            return 64496, "203.0.114.0/24"
        return None, None


class TestBgpResolver(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="netatlas-bgp-")

    def _with_stub_pyasn(self):
        stub = types.ModuleType("pyasn")
        stub.pyasn = _FakePyasnDb
        sys.modules["pyasn"] = stub
        self.addCleanup(sys.modules.pop, "pyasn", None)

    def test_graceful_degrade_without_module(self):
        sys.modules.pop("pyasn", None)
        r = BgpResolver(Path(self.td) / "rib.dat")   # pyasn 未安装 -> 降级
        self.assertEqual(r.lookup("203.0.114.1"), {})

    def test_graceful_degrade_missing_dat(self):
        self._with_stub_pyasn()
        r = BgpResolver(Path(self.td) / "nonexistent.dat")
        self.assertIsNone(r.db)
        self.assertEqual(r.lookup("203.0.114.1"), {})

    def test_lookup_with_stub(self):
        self._with_stub_pyasn()
        dat = Path(self.td) / "rib.dat"
        dat.write_bytes(b"fake-rib")
        r = BgpResolver(dat)
        self.assertEqual(r.lookup("203.0.114.9"),
                         {"bgp": {"asn": 64496, "prefix": "203.0.114.0/24"}})
        self.assertEqual(r.lookup("192.0.3.1"), {})   # 未宣告前缀 -> 空

    def test_enricher_merges_bgp_field(self):
        self._with_stub_pyasn()
        dat = Path(self.td) / "rib.dat"
        dat.write_bytes(b"fake-rib")
        cfg = _Cfg({"paths": {"data_geoip": str(Path(self.td) / "geoip")},
                    "enrichment": {"pyasn": {"enabled": True, "dat_file": str(dat)},
                                   "reverse_dns": False}})
        enr = Enricher(cfg, queue.Queue(), queue.Queue())
        rec = enr._enrich_base({"ip": "203.0.114.5", "port": 443})
        self.assertEqual(rec["bgp"], {"asn": 64496, "prefix": "203.0.114.0/24"})

    def test_enricher_pyasn_disabled_by_default(self):
        cfg = _Cfg({"paths": {"data_geoip": str(Path(self.td) / "geoip")},
                    "enrichment": {"reverse_dns": False}})
        enr = Enricher(cfg, queue.Queue(), queue.Queue())
        self.assertIsNone(enr.bgp.db)                  # 默认关闭，不加载


class TestGeoResolverFlags(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="netatlas-geo-")

    def test_flags_off_skip_loading(self):
        r = GeoResolver(Path(self.td), geoip_enabled=False, asn_enabled=False)
        self.assertIsNone(r.city)
        self.assertIsNone(r.asn)
        self.assertEqual(r.lookup("203.0.114.1"), {})

    def test_missing_mmdb_degrades(self):
        r = GeoResolver(Path(self.td), geoip_enabled=True, asn_enabled=True)
        self.assertEqual(r.lookup("203.0.114.1"), {})   # 无 mmdb -> 空 dict，不抛异常


if __name__ == "__main__":
    unittest.main()
