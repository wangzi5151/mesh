"""agent.py 的测试：路径围栏、符号链接逃逸、只读 root、差分流程、作用域强制。"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import agent as A  # noqa: E402
from mesh import queue as Q  # noqa: E402
from mesh import wire as W  # noqa: E402


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-agent-")
        self.work = os.path.join(self.tmp, "work")
        self.ro = os.path.join(self.tmp, "readonly")
        os.makedirs(os.path.join(self.work, "sub"))
        os.makedirs(os.path.join(self.work, "other"))
        os.makedirs(self.ro)
        self.write_file("sub/a.txt", b"hello world")
        self.write_file("other/b.txt", b"other content")
        with open(os.path.join(self.ro, "locked.txt"), "wb") as f:
            f.write(b"read only")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_file(self, rel, data):
        path = os.path.join(self.work, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def node(self, **kw):
        kw.setdefault("roots", [
            {"name": "work", "path": self.work, "mode": "rw"},
            {"name": "ro", "path": self.ro, "mode": "ro"},
        ])
        return A.Node("n1", **kw)


class TestJail(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-jail-")
        self.base = os.path.join(self.tmp, "base")
        os.makedirs(os.path.join(self.base, "inner"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_inside_allowed(self):
        target = os.path.join(self.base, "inner", "x.txt")
        self.assertEqual(A.jail(self.base, target), target)

    def test_base_itself_allowed(self):
        self.assertEqual(A.jail(self.base, self.base), os.path.realpath(self.base))

    def test_outside_rejected(self):
        with self.assertRaises(A.AgentError):
            A.jail(self.base, os.path.join(self.tmp, "outside.txt"))

    def test_dotdot_rejected(self):
        with self.assertRaises(A.AgentError):
            A.jail(self.base, os.path.join(self.base, "..", "outside.txt"))

    def test_symlink_escape_rejected(self):
        """符号链接指向 root 之外 —— 必须被 realpath 拦下。"""
        link = os.path.join(self.base, "escape")
        os.symlink(self.tmp, link)
        with self.assertRaises(A.AgentError) as ctx:
            A.jail(self.base, os.path.join(link, "secret.txt"))
        self.assertIn("越界", str(ctx.exception))

    def test_sibling_prefix_not_confused(self):
        """base=/x/base 时，/x/base-evil 不应被当成在 base 内。"""
        evil = self.base + "-evil"
        os.makedirs(evil, exist_ok=True)
        with self.assertRaises(A.AgentError):
            A.jail(self.base, os.path.join(evil, "f.txt"))


class TestResolve(Base):
    def test_resolve_file(self):
        node = self.node()
        path = node.resolve("mesh://n1/work/sub/a.txt")
        self.assertEqual(path, os.path.join(os.path.realpath(self.work), "sub", "a.txt"))

    def test_missing_root(self):
        node = self.node()
        with self.assertRaises(A.AgentError) as ctx:
            node.resolve("mesh://n1/nope/x.txt")
        self.assertIn("没有 root", str(ctx.exception))

    def test_address_without_root(self):
        node = self.node()
        with self.assertRaises(A.AgentError):
            node.resolve("mesh://n1")

    def test_readonly_root_rejects_write(self):
        node = self.node()
        self.assertTrue(node.resolve("mesh://n1/ro/locked.txt", "read"))
        with self.assertRaises(A.AgentError) as ctx:
            node.resolve("mesh://n1/ro/locked.txt", "write")
        self.assertIn("只读", str(ctx.exception))

    def test_escape_via_address(self):
        node = self.node()
        with self.assertRaises(A.AgentError):
            node.resolve("mesh://n1/work/../../etc/passwd")

    def test_new_file_resolves(self):
        node = self.node()
        path = node.resolve("mesh://n1/work/sub/brand-new.txt", "write")
        self.assertTrue(path.endswith("brand-new.txt"))


class TestFileOps(Base):
    def test_ls(self):
        node = self.node()
        out = node.op_ls({"addr": "mesh://n1/work"})
        names = [e["name"] for e in out["entries"]]
        self.assertIn("sub", names)
        self.assertIn("other", names)
        sub = [e for e in out["entries"] if e["name"] == "sub"][0]
        self.assertTrue(sub["dir"])

    def test_ls_on_file_rejected(self):
        node = self.node()
        with self.assertRaises(A.AgentError):
            node.op_ls({"addr": "mesh://n1/work/sub/a.txt"})

    def test_stat(self):
        node = self.node()
        out = node.op_stat({"addr": "mesh://n1/work/sub/a.txt", "sha": True})
        self.assertTrue(out["exists"])
        self.assertFalse(out["dir"])
        self.assertEqual(out["size"], 11)
        self.assertEqual(out["sha"], __import__("hashlib").sha256(b"hello world").hexdigest())

    def test_stat_missing(self):
        node = self.node()
        self.assertFalse(node.op_stat({"addr": "mesh://n1/work/ghost.txt"})["exists"])

    def test_read_full(self):
        node = self.node()
        out = node.op_read({"addr": "mesh://n1/work/sub/a.txt"})
        self.assertEqual(W.b64d(out["data"]), b"hello world")

    def test_read_slice(self):
        node = self.node()
        out = node.op_read({"addr": "mesh://n1/work/sub/a.txt", "offset": 6, "limit": 5})
        self.assertEqual(W.b64d(out["data"]), b"world")

    def test_write_creates_and_replaces(self):
        node = self.node()
        out = node.op_write({"addr": "mesh://n1/work/sub/new.txt", "data": W.b64e(b"abc")})
        self.assertEqual(out["size"], 3)
        with open(os.path.join(self.work, "sub", "new.txt"), "rb") as f:
            self.assertEqual(f.read(), b"abc")
        node.op_write({"addr": "mesh://n1/work/sub/new.txt", "data": W.b64e(b"xyz")})
        with open(os.path.join(self.work, "sub", "new.txt"), "rb") as f:
            self.assertEqual(f.read(), b"xyz")

    def test_write_expected_sha_mismatch(self):
        node = self.node()
        with self.assertRaises(A.AgentError) as ctx:
            node.op_write({"addr": "mesh://n1/work/sub/a.txt",
                           "data": W.b64e(b"new"),
                           "expected_sha": "0" * 64})
        self.assertIn("已被改动", str(ctx.exception))

    def test_write_scope_enforced(self):
        node = self.node()
        with self.assertRaises(A.AgentError) as ctx:
            node.op_write({"addr": "mesh://n1/work/other/b.txt",
                           "data": W.b64e(b"nope"),
                           "scope": ["sub/*.txt"]})
        self.assertIn("作用域", str(ctx.exception))

    def test_write_scope_allows_match(self):
        node = self.node()
        node.op_write({"addr": "mesh://n1/work/sub/ok.txt",
                       "data": W.b64e(b"fine"),
                       "scope": ["sub/*.txt"]})

    def test_write_to_readonly_root_rejected(self):
        node = self.node()
        with self.assertRaises(A.AgentError):
            node.op_write({"addr": "mesh://n1/ro/locked.txt", "data": W.b64e(b"x")})


class TestDeltaFlow(Base):
    """完整 rsync 流程：接收方出签名 → 发送方出差分 → 接收方重建。"""

    def test_end_to_end(self):
        old = b"".join(b"line %05d\n" % i for i in range(4000))
        new = old[:20000] + b"INSERTED\n" + old[20000:]

        # 接收方：一个可写的独立目录（持有旧版本）
        recv_dir = os.path.join(self.tmp, "recv")
        os.makedirs(recv_dir, exist_ok=True)
        recv_path = os.path.join(recv_dir, "doc.txt")
        with open(recv_path, "wb") as f:
            f.write(old)

        # 发送方：work 节点持有新版本
        self.write_file("sub/doc.txt", new)

        sender = self.node()
        receiver = A.Node("n2", roots=[{"name": "recv", "path": recv_dir, "mode": "rw"}])

        sig_out = receiver.op_sig({"addr": "mesh://n2/recv/doc.txt", "block_size": 1024})
        self.assertEqual(sig_out["size"], len(old))

        delta_out = sender.op_delta({
            "addr": "mesh://n1/work/sub/doc.txt",
            "block_size": 1024,
            "sig": sig_out["sig"],
            "old_sha": sig_out["old_sha"],
        })
        self.assertGreater(delta_out["estimate"]["saved_ratio"], 0.9,
                           "只插入一段，应能复用绝大部分旧块")

        applied = receiver.op_patch({
            "addr": "mesh://n2/recv/doc.txt",
            "delta": delta_out["delta"],
        })
        self.assertEqual(applied["sha"], __import__("hashlib").sha256(new).hexdigest())
        with open(recv_path, "rb") as f:
            self.assertEqual(f.read(), new)

    def test_sig_missing_file(self):
        node = self.node()
        out = node.op_sig({"addr": "mesh://n1/work/ghost.txt", "block_size": 1024})
        self.assertEqual(out["sig"], [])
        self.assertEqual(out["old_sha"], "")

    def test_patch_rejects_wrong_base(self):
        self.write_file("sub/doc.txt", b"version two")
        node = self.node()
        with self.assertRaises(A.AgentError):
            node.op_patch({"addr": "mesh://n1/work/sub/doc.txt",
                           "delta": {"v": 1, "block_size": 64, "old_sha": "f" * 64,
                                     "new_sha": "", "new_size": 0, "ops": []}})


class TestExec(Base):
    def test_disabled_by_default(self):
        node = self.node()
        with self.assertRaises(A.AgentError) as ctx:
            node.op_exec({"cmd": "echo hi"})
        self.assertIn("未开启", str(ctx.exception))

    def test_enabled_runs(self):
        node = self.node(exec_enabled=True)
        out = node.op_exec({"cmd": "echo hi", "cwd": "mesh://n1/work"})
        self.assertEqual(out["rc"], 0)
        self.assertEqual(out["stdout"].strip(), "hi")

    def test_cwd_outside_root_rejected(self):
        node = self.node(exec_enabled=True)
        with self.assertRaises(A.AgentError):
            node.op_exec({"cmd": "pwd", "cwd": "mesh://n1/work/../../"})

    def test_missing_cmd(self):
        node = self.node(exec_enabled=True)
        with self.assertRaises(A.AgentError):
            node.op_exec({})


class TestJobRunner(Base):
    def _job(self, cmd, write):
        return Q.Job("j1", "proj", cmd=cmd, write=write,
                     req={"root": "work"}, prompt="do it")

    def test_in_scope_job_passes(self):
        node = self.node()
        job = self._job("echo hello > sub/out.txt", ["sub/*.txt"])
        out = node.op_job({"job": job.to_dict()})
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["violations"], [])
        self.assertIn("sub/out.txt", out["changed"])

    def test_out_of_scope_write_detected(self):
        """agent 写到作用域之外 —— 必须被抓出来���"""
        node = self.node()
        job = self._job("echo bad > other/sneaky.txt", ["sub/*.txt"])
        out = node.op_job({"job": job.to_dict()})
        self.assertFalse(out["ok"])
        self.assertIn("other/sneaky.txt", out["violations"])
        self.assertIn("越界", out["error"])

    def test_scope_violation_inside_scope_dir(self):
        """写在作用域目录里、但扩展名不对 —— 也要抓出来。"""
        node = self.node()
        job = self._job("echo x > sub/wrong.md", ["sub/*.txt"])
        out = node.op_job({"job": job.to_dict()})
        self.assertFalse(out["ok"])
        self.assertIn("sub/wrong.md", out["violations"])

    def test_prompt_passed_via_env_not_shell(self):
        """提示词里的 shell 语法不应被执行。"""
        node = self.node()
        job = Q.Job("j2", "proj", cmd="echo \"$MESH_PROMPT\" > sub/env.txt",
                    write=["sub/*.txt"], req={"root": "work"},
                    prompt="; touch sub/pwned.txt ;")
        out = node.op_job({"job": job.to_dict()})
        self.assertTrue(out["ok"], out)
        self.assertNotIn("sub/pwned.txt", out["changed"], "提示词被当成 shell 执行了")

    def test_job_without_cmd_or_runner(self):
        node = self.node()
        job = Q.Job("j3", "proj", write=["sub/*.txt"], req={"root": "work"})
        with self.assertRaises(A.AgentError):
            node.op_job({"job": job.to_dict()})

    def test_custom_runner(self):
        seen = []

        def runner(job):
            seen.append(job.id)
            return {"rc": 0, "stdout": "ok"}

        node = self.node(runner=runner)
        job = self._job("", ["sub/*.txt"])
        out = node.op_job({"job": job.to_dict()})
        self.assertTrue(out["ok"])
        self.assertEqual(seen, ["j1"])

    def test_multiple_roots_needs_explicit_root(self):
        node = self.node()
        job = Q.Job("j4", "proj", cmd="echo x > sub/a.txt", write=["sub/*.txt"])
        with self.assertRaises(A.AgentError) as ctx:
            node.op_job({"job": job.to_dict()})
        self.assertIn("未指定 root", str(ctx.exception))

    def test_slot_released_after_job(self):
        node = self.node()
        job = self._job("echo x > sub/a.txt", ["sub/*.txt"])
        node.op_job({"job": job.to_dict()})
        self.assertEqual(node.slots_used, 0)


class TestCapability(Base):
    def test_capability_shape(self):
        node = self.node(tags=["local"], slots_total=3)
        cap = node.capability()
        self.assertEqual(cap.node_id, "n1")
        self.assertEqual(cap.tags, ["local"])
        self.assertEqual(cap.slots_total, 3)
        self.assertEqual(sorted(cap.root_names()), ["ro", "work"])
        self.assertTrue(cap.root("work")["exists"])

    def test_capability_reflects_slots(self):
        node = self.node(slots_total=2)
        node.acquire_slot()
        self.assertEqual(node.capability().slots_used, 1)
        self.assertEqual(node.capability().slots_free, 1)


class TestHttpEndToEnd(Base):
    def setUp(self):
        super(TestHttpEndToEnd, self).setUp()
        self.node = self.node(token="tok")
        self.srv = self.node.build_server(port=0)
        self.srv.start()
        self.client = W.Client(self.srv.url, token="tok")

    def tearDown(self):
        self.srv.stop()
        super(TestHttpEndToEnd, self).tearDown()

    def test_cap_over_http(self):
        cap = self.client.cap()
        self.assertEqual(cap["node_id"], "n1")

    def test_read_write_over_http(self):
        self.client.write("mesh://n1/work/sub/http.txt", b"over the wire")
        self.assertEqual(self.client.read("mesh://n1/work/sub/http.txt"), b"over the wire")

    def test_ls_over_http(self):
        out = self.client.ls("mesh://n1/work")
        self.assertIn("sub", [e["name"] for e in out["entries"]])

    def test_escape_over_http_is_rejected(self):
        with self.assertRaises(W.WireError) as ctx:
            self.client.read("mesh://n1/work/../../etc/passwd")
        msg = str(ctx.exception)
        self.assertTrue("越界" in msg or "地址不合法" in msg, msg)

    def test_wrong_token_rejected(self):
        bad = W.Client(self.srv.url, token="nope")
        with self.assertRaises(W.WireError) as ctx:
            bad.cap()
        self.assertEqual(ctx.exception.status, 401)

    def test_delta_over_http(self):
        old = b"x" * 5000
        self.client.write("mesh://n1/work/sub/base.txt", old)
        self.client.write("mesh://n1/work/sub/new.txt", old + b"tail")
        sig = self.client.sig("mesh://n1/work/sub/base.txt", 512)
        d = self.client.mkdelta("mesh://n1/work/sub/new.txt",
                                sig["old_sha"], 512, sig["sig"])
        self.assertGreater(d["estimate"]["saved_ratio"], 0.8)
        self.client.apply_delta("mesh://n1/work/sub/base.txt",
                                sig["old_sha"], 512, d["delta"])
        self.assertEqual(self.client.read("mesh://n1/work/sub/base.txt"), old + b"tail")


if __name__ == "__main__":
    unittest.main(verbosity=2)
