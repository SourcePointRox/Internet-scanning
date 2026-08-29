"""异步 DNS 解析器测试：报文编解码（离线）+ 本地 UDP DNS 服务端到端。

本地 UDP DNS 桩：真实走 asyncio DatagramProtocol 收发，验证 qid 路由、
压缩指针解码、超时路径与并发上限 —— 不依赖外部网络。
"""
import asyncio
import socket
import struct
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.dns_async import (AsyncDNSResolver, NXDomainError, QTYPE_PTR,
                               build_query, parse_answers, system_nameservers,
                               _decode_name, _encode_name)


def _fake_ptr_response(qid: int, arpa: str, hostname: str) -> bytes:
    """构造合法 PTR 响应（答案区使用压缩指针指向问题区域名）。"""
    header = struct.pack(">HHHHHH", qid, 0x8180, 1, 1, 0, 0)
    question = _encode_name(arpa) + struct.pack(">HH", QTYPE_PTR, 1)
    answer = b"\xc0\x0c" + struct.pack(">HHIH", QTYPE_PTR, 1, 300, len(_encode_name(hostname)))
    return header + question + answer + _encode_name(hostname)


class TestPacketCodec(unittest.TestCase):
    def test_name_roundtrip(self):
        encoded = _encode_name("host.example.com")
        name, end = _decode_name(encoded + b"\xde\xad", 0)
        self.assertEqual(name, "host.example.com")
        self.assertEqual(end, len(encoded))

    def test_query_build(self):
        qid, packet = build_query("1.0.0.127.in-addr.arpa", QTYPE_PTR, qid=0x1234)
        self.assertEqual(qid, 0x1234)
        self.assertEqual(struct.unpack(">H", packet[:2])[0], 0x1234)
        self.assertEqual(struct.unpack(">H", packet[4:6])[0], 1)  # QDCOUNT=1

    def test_parse_ptr_with_compression(self):
        arpa = "1.0.0.127.in-addr.arpa"
        msg = _fake_ptr_response(0x1234, arpa, "localhost.example")
        answers = parse_answers(msg, 0x1234, QTYPE_PTR)
        self.assertEqual(answers, ["localhost.example"])

    def test_qid_mismatch_rejected(self):
        msg = _fake_ptr_response(0x9999, "1.0.0.127.in-addr.arpa", "x.example")
        with self.assertRaises(ValueError):
            parse_answers(msg, 0x1234, QTYPE_PTR)

    def test_nxdomain_raises(self):
        header = struct.pack(">HHHHHH", 0x1234, 0x8183, 1, 0, 0, 0)  # RCODE=3
        question = _encode_name("no.such.arpa") + struct.pack(">HH", QTYPE_PTR, 1)
        with self.assertRaises(NXDomainError):
            parse_answers(header + question, 0x1234, QTYPE_PTR)

    def test_truncated_rejected(self):
        with self.assertRaises(ValueError):
            parse_answers(b"\x00" * 5, 0, QTYPE_PTR)


class _StubDNSServer:
    """本地 UDP DNS 桩：响应 PTR 查询，支持延迟/丢包模拟。"""

    def __init__(self, drop_qids: set | None = None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.drop_qids = drop_qids or set()
        self.queries = 0
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._serve, daemon=True)
        self._t.start()

    def _serve(self):
        self.sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                data, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break  # 套接字已关闭
            self.queries += 1
            qid = struct.unpack(">H", data[:2])[0]
            if qid in self.drop_qids:
                continue  # 模拟丢包 -> 客户端应超时
            try:
                qname, _ = _decode_name(data, 12)
            except ValueError:
                continue
            self.sock.sendto(_fake_ptr_response(qid, qname, "stub.host.example"), addr)

    def close(self):
        self._stop.set()
        self.sock.close()


class TestResolverEndToEnd(unittest.TestCase):
    def _make(self, server, timeout=2.0, concurrency=64):
        return AsyncDNSResolver(nameservers=["127.0.0.1"], timeout_s=timeout,
                                concurrency=concurrency, port=server.port)

    def test_reverse_over_udp(self):
        server = _StubDNSServer()
        try:
            async def run():
                r = self._make(server)
                name = await r.reverse("127.0.0.1")
                await r.close()
                return name
            self.assertEqual(asyncio.run(run()), "stub.host.example")
            self.assertGreaterEqual(server.queries, 1)
        finally:
            server.close()

    def test_timeout_returns_none(self):
        server = _StubDNSServer(drop_qids=set(range(65536)))  # 全部丢包
        try:
            async def run():
                r = self._make(server, timeout=0.3)
                result = await r.reverse("127.0.0.1")
                await r.close()
                return result
            self.assertIsNone(asyncio.run(run()))
        finally:
            server.close()

    def test_concurrency_throughput(self):
        """并发 64 个查询应在秒级完成（线程池模式需 64/16×rtt 倍时间）。"""
        server = _StubDNSServer()
        try:
            async def run():
                import time
                r = self._make(server)
                t0 = time.monotonic()
                results = await asyncio.gather(*[r.reverse(f"10.0.0.{i}") for i in range(64)])
                elapsed = time.monotonic() - t0
                await r.close()
                return results, elapsed
            results, elapsed = asyncio.run(run())
            self.assertTrue(all(r == "stub.host.example" for r in results))
            self.assertLess(elapsed, 5.0)
            self.assertEqual(server.queries, 64)
        finally:
            server.close()


class TestSystemNameservers(unittest.TestCase):
    def test_returns_valid_ips(self):
        ns = system_nameservers()
        self.assertTrue(ns, "必须至少返回一个 nameserver（含公共 DNS 兜底）")
        import ipaddress
        for s in ns:
            ipaddress.ip_address(s)


if __name__ == "__main__":
    unittest.main()
