"""wire.py 的测试：JSON-RPC 往返、鉴权、错误传播、并发。"""

from __future__ import annotations

import json
import os
import sys
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import wire as W  # noqa: E402


class TestBase64(unittest.TestCase):
    def test_round_trip(self):
        for data in (b"", b"\x00\xff\xfe", b"hello", bytes(range(256))):
            self.assertEqual(W.b64d(W.b64e(data)), data)

    def test_accepts_bytearray(self):
        self.assertEqual(W.b64d(W.b64e(bytearray(b"abc"))), b"abc")

    def test_decode_empty(self):
        self.assertEqual(W.b64d(""), b"")
        self.assertEqual(W.b64d(None), b"")


class TestServerClient(unittest.TestCase):
    def setUp(self):
        self.server = W.Server(host="127.0.0.1", port=0, node_id="n1")
        self.server.add("echo", lambda p: {"got": p})
        self.server.add("add", lambda p: {"sum": int(p["a"]) + int(p["b"])})
        self.server.add("boom", self._boom)
        self.server.add("nothing", lambda p: None)
        self.server.start()
        self.client = W.Client(self.server.url)

    def tearDown(self):
        self.server.stop()

    @staticmethod
    def _boom(payload):
        raise ValueError("故意炸的")

    def test_echo(self):
        self.assertEqual(self.client.call("echo", {"x": 1}), {"got": {"x": 1}})

    def test_call_without_payload(self):
        self.assertEqual(self.client.call("echo"), {"got": {}})

    def test_falsy_results_survive_the_wire(self):
        """空列表/0/False 不能被换成 {} —— 调用方要能区分。"""
        self.server.add("empty_list", lambda p: [])
        self.server.add("zero", lambda p: 0)
        self.server.add("false", lambda p: False)
        self.assertEqual(self.client.call("empty_list"), [])
        self.assertEqual(self.client.call("zero"), 0)
        self.assertIs(self.client.call("false"), False)

    def test_binary_round_trip(self):
        self.server.add("blob", lambda p: {"data": W.b64e(b"\x00\x01\x02\xff")})
        self.assertEqual(self.client.call("blob")["data"], W.b64e(b"\x00\x01\x02\xff"))

    def test_remote_error_propagates(self):
        with self.assertRaises(W.WireError) as ctx:
            self.client.call("boom")
        self.assertEqual(ctx.exception.kind, "remote")
        self.assertIn("故意炸的", str(ctx.exception))

    def test_unknown_method(self):
        with self.assertRaises(W.WireError) as ctx:
            self.client.call("nope")
        self.assertEqual(ctx.exception.kind, "remote")
        self.assertIn("未知方法", str(ctx.exception))

    def test_none_result_becomes_empty_dict(self):
        self.assertEqual(self.client.call("nothing"), {})

    def test_health(self):
        info = self.client.health()
        self.assertEqual(info["name"], "mesh")
        self.assertEqual(info["node"], "n1")
        self.assertEqual(info["proto"], W.PROTO)

    def test_network_error_on_dead_port(self):
        dead = W.Client("http://127.0.0.1:1", timeout=1.0)
        with self.assertRaises(W.WireError) as ctx:
            dead.call("echo")
        self.assertEqual(ctx.exception.kind, "network")

    def test_unknown_path_404(self):
        try:
            urllib.request.urlopen(self.server.url + "/nope", timeout=5)
            self.fail("应当 404")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 404)

    def test_bad_json_body(self):
        req = urllib.request.Request(self.server.url + "/rpc/echo",
                                     data=b"{not json", method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        self.assertFalse(data["ok"])
        self.assertEqual(data["t"], "BadJSON")

    def test_non_object_body_rejected(self):
        req = urllib.request.Request(self.server.url + "/rpc/echo",
                                     data=b"[1,2,3]", method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        self.assertFalse(data["ok"])

    def test_concurrent_calls(self):
        results = []
        lock = threading.Lock()

        def worker(i):
            r = self.client.call("add", {"a": i, "b": i})
            with lock:
                results.append(r["sum"])

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(sorted(results), [i * 2 for i in range(20)])


class TestAuth(unittest.TestCase):
    def setUp(self):
        self.server = W.Server(host="127.0.0.1", port=0, token="s3cret")
        self.server.add("echo", lambda p: {"ok": 1})
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_correct_token(self):
        client = W.Client(self.server.url, token="s3cret")
        self.assertEqual(client.call("echo"), {"ok": 1})

    def test_wrong_token_401(self):
        client = W.Client(self.server.url, token="wrong")
        with self.assertRaises(W.WireError) as ctx:
            client.call("echo")
        self.assertEqual(ctx.exception.kind, "http")
        self.assertEqual(ctx.exception.status, 401)

    def test_missing_token_401(self):
        client = W.Client(self.server.url)
        with self.assertRaises(W.WireError) as ctx:
            client.call("echo")
        self.assertEqual(ctx.exception.status, 401)

    def test_health_needs_no_token(self):
        client = W.Client(self.server.url)
        self.assertEqual(client.health()["name"], "mesh")


class TestServerLifecycle(unittest.TestCase):
    def test_context_manager_and_ephemeral_port(self):
        with W.Server(port=0) as srv:
            srv.add("ping", lambda p: {"pong": True})
            self.assertGreater(srv.port, 0)
            self.assertEqual(W.Client(srv.url).call("ping"), {"pong": True})
        # 关闭后再连应当失败
        with self.assertRaises(W.WireError):
            W.Client(srv.url, timeout=1.0).call("ping")

    def test_repr(self):
        srv = W.Server(port=0)
        self.assertIn("routes", repr(srv))


if __name__ == "__main__":
    unittest.main(verbosity=2)
