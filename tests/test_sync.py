"""产物双向分发：计划里写 sync，作业跑完收回来，再按增量推给别的节点。

和 test_hub 一样全部走真实路径：真起两个节点、真跑作业、真往对面写文件。
这一层的价值就是"A 机器改完的东西真的出现在 B 机器上"，把 HTTP 换掉就什么都没测到。
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import agent as A  # noqa: E402
from mesh import cli as CLI  # noqa: E402
from mesh import hub as H  # noqa: E402
from mesh import queue as Q  # noqa: E402
from mesh import task as T  # noqa: E402
from mesh import wire as W  # noqa: E402

BODY = "一大段可复用内容，重复很多次好形成可复用的块\n" * 500     # ≈ 24KB


# --------------------------------------------------------------------------
# 分发目标的表达（纯规格，不联网）
# --------------------------------------------------------------------------
class TestSyncTarget(unittest.TestCase):
    def test_string_form(self):
        t = T.SyncTarget.from_any("mesh://笔记本/work")
        self.assertEqual((t.node, t.root, t.paths), ("笔记本", "work", []))
        self.assertEqual(str(t), "mesh://笔记本/work")

    def test_string_with_path_becomes_filter(self):
        t = T.SyncTarget.from_any("mesh://笔记本/work/docs")
        self.assertEqual(t.paths, ["docs", "docs/**"])
        self.assertTrue(t.accepts("docs/a.md"))
        self.assertTrue(t.accepts("docs"))
        self.assertFalse(t.accepts("other/a.md"))

    def test_env_segment_kept(self):
        t = T.SyncTarget.from_any("mesh://笔记本/ubuntu:work")
        self.assertEqual((t.env, t.root), ("ubuntu", "work"))
        self.assertEqual(str(t), "mesh://笔记本/ubuntu:work")

    def test_dict_form_with_paths(self):
        t = T.SyncTarget.from_any({"to": "mesh://树莓派/store",
                                   "paths": ["src/**/*.py", "/docs"]})
        self.assertEqual(t.paths, ["src/**/*.py", "docs"])
        self.assertTrue(t.accepts("src/a/b.py"))
        self.assertFalse(t.accepts("README.md"))

    def test_dict_paths_override_addr_path(self):
        t = T.SyncTarget.from_any({"to": "mesh://n/work/docs", "paths": ["a.txt"]})
        self.assertEqual(t.paths, ["a.txt"])

    def test_missing_root_rejected(self):
        # 没有 root 就不知道该落到哪个目录 —— 不能猜，猜错就是往别人机器上乱写
        with self.assertRaises(T.PlanError):
            T.SyncTarget.from_any("mesh://笔记本")

    def test_wildcard_rejected(self):
        with self.assertRaises(T.PlanError):
            T.SyncTarget.from_any("mesh://*/work")

    def test_bad_address_rejected(self):
        for bad in ("", "http://n/work", "mesh://", "mesh://n/work/../etc"):
            with self.assertRaises(T.PlanError):
                T.SyncTarget.from_any(bad)

    def test_non_string_rejected(self):
        with self.assertRaises(T.PlanError):
            T.SyncTarget.from_any(42)

    def test_dict_without_to_rejected(self):
        with self.assertRaises(T.PlanError):
            T.SyncTarget.from_any({"paths": ["a"]})

    def test_empty_pattern_rejected(self):
        with self.assertRaises(T.PlanError):
            T.SyncTarget.from_any({"to": "mesh://n/work", "paths": ["  "]})

    def test_from_any_copies(self):
        """拿到手的是副本：改它不能污染计划里那一份。"""
        first = T.SyncTarget.from_any("mesh://n/work")
        second = T.SyncTarget.from_any(first)
        second.paths.append("x")
        self.assertEqual(first.paths, [])

    def test_to_dict_round_trip(self):
        t = T.SyncTarget.from_any({"to": "mesh://n/work", "paths": ["a/**"]})
        self.assertEqual(T.SyncTarget.from_any(t.to_dict()).paths, ["a/**"])


class TestMergeSync(unittest.TestCase):
    def test_dedup_by_node_and_root(self):
        merged = T.merge_sync(["mesh://a/work"], ["mesh://a/work", "mesh://b/work"])
        self.assertEqual([str(t) for t in merged], ["mesh://a/work", "mesh://b/work"])

    def test_any_unfiltered_wins(self):
        merged = T.merge_sync(["mesh://a/work"], [{"to": "mesh://a/work",
                                                   "paths": ["x"]}])
        self.assertEqual(merged[0].paths, [], "一处要全部，就得按全部算")

    def test_filters_union(self):
        merged = T.merge_sync([{"to": "mesh://a/work", "paths": ["x"]}],
                              [{"to": "mesh://a/work", "paths": ["y", "x"]}])
        self.assertEqual(merged[0].paths, ["x", "y"])


class TestPlanSync(unittest.TestCase):
    def plan(self, raw):
        return T.Plan.from_dict(raw)

    def test_task_sync_round_trip(self):
        spec = T.TaskSpec.from_dict({"id": "t", "sync": ["mesh://n/work"]})
        self.assertEqual(len(spec.sync), 1)
        back = T.TaskSpec.from_dict(spec.to_dict())
        self.assertEqual([str(t) for t in back.sync], ["mesh://n/work"])

    def test_plan_level_default_reaches_every_job(self):
        plan = self.plan({"project": "p", "sync": ["mesh://n/work"],
                          "tasks": [{"id": "a"}, {"id": "b"}]})
        for job in plan.to_jobs():
            self.assertEqual([t["to"] for t in job.sync], ["mesh://n/work"],
                             "计划级默认值要落到每一条作业上")

    def test_task_and_plan_targets_merge(self):
        plan = self.plan({"project": "p", "sync": ["mesh://n/work"],
                          "tasks": [{"id": "a", "sync": ["mesh://m/store"]}]})
        job = plan.to_jobs()[0]
        self.assertEqual([t["to"] for t in job.sync],
                         ["mesh://n/work", "mesh://m/store"])

    def test_no_sync_means_empty(self):
        plan = self.plan({"project": "p", "tasks": [{"id": "a"}]})
        self.assertEqual(plan.to_jobs()[0].sync, [])

    def test_bad_target_fails_at_submit(self):
        with self.assertRaises(T.PlanError):
            self.plan({"project": "p", "tasks": [{"id": "a", "sync": ["mesh://n"]}]})

    def test_to_dict_keeps_plan_level_sync(self):
        plan = self.plan({"project": "p", "sync": ["mesh://n/work"],
                          "tasks": [{"id": "a"}]})
        self.assertEqual(T.Plan.from_dict(plan.to_dict()).sync_targets()[0].root, "work")

    def test_stats_count_targets(self):
        plan = self.plan({"project": "p",
                          "tasks": [{"id": "a", "sync": ["mesh://n/work"]},
                                    {"id": "b", "sync": ["mesh://n/work"]}]})
        self.assertEqual(plan.stats()["sync"], 1, "同一个目标算一个")


# --------------------------------------------------------------------------
# 队列：sync 要能落库、要能补列
# --------------------------------------------------------------------------
class TestSyncPersistence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-sync-db-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_sync_survives_the_database(self):
        path = os.path.join(self.tmp, "q.db")
        q = Q.Queue(path)
        try:
            q.add(Q.Job("t", "p", sync=[{"to": "mesh://n/work", "paths": ["a"]}]))
        finally:
            q.close()
        q = Q.Queue(path)
        try:
            self.assertEqual(q.get("t").sync,
                             [{"to": "mesh://n/work", "paths": ["a"]}])
        finally:
            q.close()

    def test_old_database_gains_the_column(self):
        """0.2 的库直接跑 0.3 不能炸：CREATE TABLE IF NOT EXISTS 不会补列。"""
        path = os.path.join(self.tmp, "old.db")
        old = sqlite3.connect(path)
        old.executescript("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY, project TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '', prompt TEXT NOT NULL DEFAULT '',
                read_json TEXT NOT NULL DEFAULT '[]',
                write_json TEXT NOT NULL DEFAULT '[]',
                priority INTEGER NOT NULL DEFAULT 0,
                req_json TEXT NOT NULL DEFAULT '{}',
                agent TEXT NOT NULL DEFAULT '', cmd TEXT NOT NULL DEFAULT '',
                model TEXT NOT NULL DEFAULT '', verify TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 2,
                lease_owner TEXT NOT NULL DEFAULT '',
                lease_expires REAL NOT NULL DEFAULT 0,
                feedback TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                started_at REAL NOT NULL DEFAULT 0,
                finished_at REAL NOT NULL DEFAULT 0
            );
            INSERT INTO jobs (id, project, created_at, updated_at)
            VALUES ('old', 'p', 1, 1);
        """)
        old.commit()
        old.close()

        q = Q.Queue(path)
        try:
            old_job = q.get("old")
            self.assertEqual(old_job.id, "old", "老作业不能因为补列丢掉")
            self.assertEqual(old_job.sync, [])
            q.add(Q.Job("new", "p", sync=[{"to": "mesh://n/work"}]))
            self.assertEqual(q.get("new").sync, [{"to": "mesh://n/work"}])
        finally:
            q.close()

    def test_migrate_is_idempotent(self):
        path = os.path.join(self.tmp, "q.db")
        for _ in range(3):
            q = Q.Queue(path)
            try:
                q.add(Q.Job("t", "p"))
            except Q.QueueError:
                pass                      # 第二次开始是主键冲突，正好
            finally:
                q.close()
        q = Q.Queue(path)
        try:
            self.assertEqual(q.get("t").sync, [])
        finally:
            q.close()

    def test_update_result_keeps_state(self):
        q = Q.Queue(":memory:")
        self.addCleanup(q.close)
        q.add(Q.Job("t", "p"))
        q.complete("t", result={"changed": ["a"]})
        q.update_result("t", {"changed": ["a"], "sync": {"ok": True}})
        job = q.get("t")
        self.assertEqual(job.state, Q.DONE)
        self.assertTrue(job.result["sync"]["ok"])

    def test_update_result_unknown_job(self):
        q = Q.Queue(":memory:")
        self.addCleanup(q.close)
        with self.assertRaises(Q.QueueError):
            q.update_result("nope", {})


# --------------------------------------------------------------------------
# 节点侧：rm
# --------------------------------------------------------------------------
class TestAgentRm(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-sync-rm-")
        self.work = os.path.join(self.tmp, "work")
        self.ro = os.path.join(self.tmp, "ro")
        os.makedirs(self.work)
        os.makedirs(self.ro)
        self.node = A.Node("n", roots=[{"name": "work", "path": self.work},
                                       {"name": "ro", "path": self.ro, "mode": "ro"}])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_removes_file(self):
        path = os.path.join(self.work, "a.txt")
        with open(path, "w") as f:
            f.write("x")
        r = self.node.op_rm({"addr": "mesh://n/work/a.txt"})
        self.assertTrue(r["removed"])
        self.assertFalse(os.path.exists(path))

    def test_missing_file_is_not_an_error(self):
        r = self.node.op_rm({"addr": "mesh://n/work/nope.txt"})
        self.assertEqual((r["removed"], r["existed"]), (False, False))

    def test_directory_refused(self):
        os.makedirs(os.path.join(self.work, "d"))
        with self.assertRaises(A.AgentError):
            self.node.op_rm({"addr": "mesh://n/work/d"})

    def test_read_only_root_refused(self):
        path = os.path.join(self.ro, "a.txt")
        with open(path, "w") as f:
            f.write("x")
        with self.assertRaises(A.AgentError):
            self.node.op_rm({"addr": "mesh://n/ro/a.txt"})
        self.assertTrue(os.path.exists(path))

    def test_escape_refused(self):
        outside = os.path.join(self.tmp, "outside.txt")
        with open(outside, "w") as f:
            f.write("x")
        with self.assertRaises(A.AgentError):
            self.node.op_rm({"addr": "mesh://n/work/../outside.txt"})
        self.assertTrue(os.path.exists(outside))

    def test_scope_enforced(self):
        path = os.path.join(self.work, "keep.txt")
        with open(path, "w") as f:
            f.write("x")
        with self.assertRaises(A.AgentError):
            self.node.op_rm({"addr": "mesh://n/work/keep.txt", "scope": ["other/*"]})
        self.assertTrue(os.path.exists(path))

    def test_rm_over_http(self):
        node, srv = self.node, self.node.build_server(port=0)
        srv.start()
        self.addCleanup(srv.stop)
        with open(os.path.join(self.work, "b.txt"), "w") as f:
            f.write("x")
        client = W.Client(srv.url, timeout=5)
        self.assertTrue(client.rm("mesh://n/work/b.txt")["removed"])
        self.assertFalse(os.path.exists(os.path.join(self.work, "b.txt")))


# --------------------------------------------------------------------------
# 分发本身：两台机器 + 真 HTTP
# --------------------------------------------------------------------------
class SyncBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-sync-")
        self.src_dir = os.path.join(self.tmp, "手机")
        self.dst_dir = os.path.join(self.tmp, "笔记本")
        os.makedirs(self.src_dir)
        os.makedirs(self.dst_dir)
        self.db = os.path.join(self.tmp, "queue.db")
        self.mirror = os.path.join(self.tmp, "mirror")
        self._servers = []
        self._hubs = []

    def tearDown(self):
        for srv in self._servers:
            srv.stop()
        for h in self._hubs:
            h.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- 装配 ------------------------------------------------------------
    def serve(self, node):
        srv = node.build_server(port=0)
        srv.start()
        self._servers.append(srv)
        return srv

    def sink(self, node_id="笔记本", mode="rw", path=None, tags=None):
        node = A.Node(node_id, roots=[{"name": "work", "mode": mode,
                                       "path": path or self.dst_dir}],
                      tags=tags or [], runner=lambda job: {"stdout": "不干活"})
        return node, self.serve(node)

    def writer(self, node_id="手机", tags=("源",), path=None):
        """源节点：把 ``rel`` 写成一整段 BODY + 这一轮的标记。"""

        def runner(job):
            for rel in (job.write or ["doc.txt"]):
                target = os.path.join(path or self.src_dir, rel)
                parent = os.path.dirname(target)
                if parent:
                    os.makedirs(parent, exist_ok=True)
                prev = ""
                if os.path.exists(target):
                    with open(target) as f:
                        prev = f.read()
                with open(target, "w") as f:
                    f.write((prev or BODY) + "第 %s 次修改\n" % job.id)
            return {"stdout": "写好了"}

        node = A.Node(node_id, roots=[{"name": "work", "path": path or self.src_dir}],
                      tags=list(tags), runner=runner)
        return node, self.serve(node)

    def hub(self, mirror=True, **kw):
        kw.setdefault("poll", 0.01)
        kw.setdefault("dispatch_timeout", 10.0)
        if mirror:
            kw.setdefault("mirror", self.mirror)
        h = H.Hub(self.db, **kw)
        self._hubs.append(h)
        return h

    def setup_pair(self, mirror=True, dst_mode="rw"):
        src_node, self.src_srv = self.writer()
        dst_node, self.dst_srv = self.sink(mode=dst_mode)
        h = self.hub(mirror=mirror)
        h.add_node("手机", self.src_srv.url)
        h.add_node("笔记本", self.dst_srv.url)
        return h

    def submit(self, h, tasks):
        h.submit(T.Plan.from_dict({"project": "p", "tasks": tasks}))
        return h.run_until_idle(timeout=30, poll=0.02)

    def chain(self, h, count=2, sync=None, write=None):
        """t1 → t2 → … 一串作业，每条都往同一个文件上追加。"""
        tasks = []
        for i in range(1, count + 1):
            spec = self.task("t%d" % i, sync=sync, write=write)
            if i > 1:
                spec["deps"] = ["t%d" % (i - 1)]
            tasks.append(spec)
        return self.submit(h, tasks)

    def task(self, task_id="t1", sync=None, write=None, deps=None):
        """源作业：用标签钉在源节点上，否则它可能跑到目标节点上去。"""
        out = {"id": task_id, "write": write or ["doc.txt"], "max_attempts": 1,
               "req": {"root": "work", "tags": ["源"]}}
        if sync is not None:
            out["sync"] = sync
        if deps:
            out["deps"] = deps
        return out

    def dst(self, rel="doc.txt"):
        return os.path.join(self.dst_dir, rel)

    def read(self, path):
        with open(path) as f:
            return f.read()

    def job(self, h, job_id="t1"):
        return h.queue.get(job_id)


class TestPush(SyncBase):
    def test_a_machine_writes_and_b_machine_gets_it(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])

        self.assertEqual(self.job(h).state, Q.DONE)
        self.assertTrue(os.path.isfile(self.dst("doc.txt")))
        self.assertEqual(self.read(self.dst()), self.read(os.path.join(self.src_dir, "doc.txt")))
        report = self.job(h).result["sync"]
        self.assertTrue(report["ok"])
        self.assertEqual(report["targets"][0]["to"], "mesh://笔记本/work")
        self.assertEqual(report["files"], 1)

    def test_first_push_is_full(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        files = self.job(h).result["sync"]["targets"][0]["files"]
        self.assertEqual([f["mode"] for f in files], ["full"],
                         "目标上什么都没有，只能整取")

    def test_second_push_uses_delta(self):
        h = self.setup_pair()
        self.chain(h, 2, sync=["mesh://笔记本/work"])

        files = self.job(h, "t2").result["sync"]["targets"][0]["files"]
        self.assertEqual([f["mode"] for f in files], ["delta"])
        self.assertEqual(self.read(self.dst()), self.read(os.path.join(self.src_dir, "doc.txt")))

    def test_delta_saves_bandwidth(self):
        h = self.setup_pair()
        self.chain(h, 2, sync=["mesh://笔记本/work"])
        report = self.job(h, "t2").result["sync"]
        self.assertGreater(report["ratio"], 0.5)
        self.assertLess(report["sent"], report["size"])

    def test_push_without_mirror_relays_through_the_source_node(self):
        """不开 --mirror 也要能分发：让源节点算差分，控制面只当中转。"""
        h = self.setup_pair(mirror=False)
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        self.assertFalse(os.path.exists(self.mirror))
        self.assertEqual(self.read(self.dst()), self.read(os.path.join(self.src_dir, "doc.txt")))
        self.assertTrue(self.job(h).result["sync"]["ok"])

    def test_relay_also_uses_delta(self):
        h = self.setup_pair(mirror=False)
        self.chain(h, 2, sync=["mesh://笔记本/work"])
        files = self.job(h, "t2").result["sync"]["targets"][0]["files"]
        self.assertEqual([f["mode"] for f in files], ["delta"])

    def test_identical_target_file_is_skipped(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        first = self.job(h).result["sync"]
        self.assertEqual([f["mode"] for f in first["targets"][0]["files"]], ["full"])

        again = h.sync_job("t1")            # 目标上已经是这一版了
        self.assertEqual([f["mode"] for f in again["targets"][0]["files"]], ["same"])
        self.assertEqual(again["sent"], 0, "内容一样就别再传了")

    def test_subdirectories_created_on_target(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"], write=["sub/deep/doc.txt"])])
        self.assertTrue(os.path.isfile(self.dst("sub/deep/doc.txt")))

    def test_deletion_is_propagated(self):
        with open(self.dst("gone.txt"), "w") as f:
            f.write("目标上的旧版本")

        def runner(job):
            path = os.path.join(self.src_dir, "gone.txt")
            if job.id == "t1":
                with open(path, "w") as f:
                    f.write("源上的旧版本")
                return {"stdout": "建了"}
            os.remove(path)            # t2：把上一轮建的文件删掉
            return {"stdout": "删了"}

        node = A.Node("手机", roots=[{"name": "work", "path": self.src_dir}],
                      tags=["源"], runner=runner)
        h = self.hub(mirror=False)
        h.add_node("手机", self.serve(node).url)
        dst_node, dst_srv = self.sink()
        h.add_node("笔记本", dst_srv.url)

        self.chain(h, 2, sync=["mesh://笔记本/work"], write=["gone.txt"])

        self.assertFalse(os.path.exists(self.dst("gone.txt")),
                         "源那边删了，目标也得删")
        modes = [f["mode"] for f in self.job(h, "t2").result["sync"]["targets"][0]["files"]]
        self.assertIn("delete", modes)

    def test_path_filter(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=[{"to": "mesh://笔记本/work",
                                         "paths": ["keep/**"]}],
                                  write=["keep/a.txt", "drop/b.txt"])])
        self.assertTrue(os.path.exists(self.dst("keep/a.txt")))
        self.assertFalse(os.path.exists(self.dst("drop/b.txt")))

    def test_path_traversal_from_a_node_is_refused(self):
        """节点报上来的路径是不可信的：不能拿它往别的机器上写文件。"""
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        job = self.job(h)
        report = h.push(job, "手机", "work",
                        {"changed": ["../../evil.txt", "/etc/passwd", "a/../../evil.txt"]})
        self.assertTrue(report["errors"])
        self.assertEqual(report["files"], 0)
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "evil.txt")))

    def test_same_root_is_skipped_not_failed(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://手机/work"])])
        entry = self.job(h).result["sync"]["targets"][0]
        self.assertTrue(entry["ok"])
        self.assertTrue(entry["skipped"])
        self.assertEqual(entry["files"], [])

    def test_unknown_target_node_is_reported_not_fatal(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://不存在的机器/work"])])
        job = self.job(h)
        self.assertEqual(job.state, Q.DONE, "分发不了不代表作业失败")
        self.assertFalse(job.result["sync"]["ok"])
        self.assertIn("未注册", job.result["sync"]["errors"][0])

    def test_offline_target_does_not_fail_the_job(self):
        h = self.setup_pair()
        # 目标先下线：把它的地址改成没人监听的端口
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        job = self.job(h)
        self.assertEqual(job.state, Q.DONE)
        self.assertFalse(job.result["sync"]["ok"])

    def test_read_only_target_root_is_reported(self):
        h = self.setup_pair(dst_mode="ro")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        job = self.job(h)
        self.assertEqual(job.state, Q.DONE)
        self.assertFalse(job.result["sync"]["ok"])
        self.assertFalse(os.path.exists(self.dst()))

    def test_result_records_the_source_for_later_repush(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        result = self.job(h).result
        self.assertEqual((result["node"], result["root"]), ("手机", "work"))
        self.assertEqual(result["changed"], ["doc.txt"])

    def test_multiple_targets(self):
        other = os.path.join(self.tmp, "树莓派")
        os.makedirs(other)
        h = self.setup_pair()
        _, srv = self.sink("树莓派", path=other)
        h.add_node("树莓派", srv.url)
        self.submit(h, [self.task(sync=["mesh://笔记本/work", "mesh://树莓派/work"])])
        self.assertTrue(os.path.isfile(self.dst()))
        self.assertTrue(os.path.isfile(os.path.join(other, "doc.txt")))
        self.assertEqual(len(self.job(h).result["sync"]["targets"]), 2)

    def test_sync_errors_show_up_in_the_tick_report(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")
        h.submit(T.Plan.from_dict({"project": "p", "tasks": [
            self.task(sync=["mesh://笔记本/work"])]}))
        report = h.tick()
        self.assertTrue(report.get("sync_errors"), "分发失败要让运维看得见")

    def test_no_sync_no_push(self):
        h = self.setup_pair()
        self.submit(h, [self.task()])
        self.assertNotIn("sync", self.job(h).result or {})
        self.assertFalse(os.path.exists(self.dst()))


class TestRepush(SyncBase):
    def failed_then_ok(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")      # 目标暂时连不上
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        self.assertFalse(self.job(h).result["sync"]["ok"])
        return h

    def test_sync_job_after_target_comes_back(self):
        h = self.failed_then_ok()
        h.add_node("笔记本", self.dst_srv.url)          # 目标回来了
        report = h.sync_job("t1")
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual(self.read(self.dst()), self.read(os.path.join(self.src_dir, "doc.txt")))
        # 重新记的这份要落库，不然重启后又变成"失败"
        self.assertTrue(self.job(h).result["sync"]["ok"])
        self.assertEqual(self.job(h).state, Q.DONE)

    def test_sync_job_with_ad_hoc_targets(self):
        h = self.setup_pair()
        self.submit(h, [self.task()])                   # 计划里没有 sync
        report = h.sync_job("t1", targets=["mesh://笔记本/work"])
        self.assertTrue(report["ok"], report["errors"])
        self.assertTrue(os.path.exists(self.dst()))

    def test_sync_job_without_targets_is_refused(self):
        h = self.setup_pair()
        self.submit(h, [self.task()])
        with self.assertRaises(H.HubError):
            h.sync_job("t1")

    def test_sync_job_requires_done(self):
        h = self.setup_pair()
        h.submit(T.Plan.from_dict({"project": "p", "tasks": [
            self.task(sync=["mesh://笔记本/work"])]}))
        with self.assertRaises(H.HubError):
            h.sync_job("t1")                            # 还 queued 着呢

    def test_sync_job_unknown_id(self):
        h = self.setup_pair()
        with self.assertRaises(H.HubError):
            h.sync_job("nope")

    def test_sync_job_with_old_result_is_refused_clearly(self):
        """0.3 之前的作业结果里没有 node/root —— 要说清楚，而不是猜。"""
        h = self.setup_pair()
        h.queue.add(Q.Job("old", "p", state=Q.DONE, sync=[{"to": "mesh://笔记本/work"}],
                          result={"changed": ["doc.txt"]}))
        with self.assertRaises(H.HubError) as ctx:
            h.sync_job("old")
        self.assertIn("没记下", str(ctx.exception))


class TestLargeFile(SyncBase):
    """单次 read 被节点截在 8MB：整取路径必须分块读，否则镜像和分发都是残的。"""

    def big_body(self, size=(9 << 20)):
        head = b"BIG\n"
        return head + (b"0123456789abcdef" * ((size - len(head)) // 16 + 1))[:size - len(head)]

    def test_bigger_than_one_read_block(self):
        body = self.big_body()

        def runner(job):
            with open(os.path.join(self.src_dir, "big.bin"), "wb") as f:
                f.write(body)
            return {"stdout": "写了个大文件"}

        node = A.Node("手机", roots=[{"name": "work", "path": self.src_dir}],
                      tags=["源"], runner=runner)
        srv = self.serve(node)
        h = self.hub()
        h.add_node("手机", srv.url)
        dst_node, dst_srv = self.sink()
        h.add_node("笔记本", dst_srv.url)

        self.submit(h, [self.task(sync=["mesh://笔记本/work"], write=["big.bin"])])

        mirrored = os.path.join(self.mirror, "手机", "work", "big.bin")
        self.assertEqual(os.path.getsize(mirrored), len(body), "镜像不能被截断")
        self.assertEqual(os.path.getsize(self.dst("big.bin")), len(body), "分发也不能")
        with open(self.dst("big.bin"), "rb") as f:
            self.assertEqual(f.read(), body)


class TestCliSync(SyncBase):
    def run_cli(self, argv):
        """跑一条命令，返回 (退出码, stdout)。"""
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = CLI.main(argv)
        return code, buf.getvalue()

    def test_sync_command_repushes(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        h.add_node("笔记本", self.dst_srv.url)
        h.close()
        self._hubs.remove(h)

        code, out = self.run_cli(["sync", "t1", "--db", self.db,
                                  "--mirror", self.mirror])
        self.assertEqual(code, 0, out)
        self.assertIn("mesh://笔记本/work", out)
        self.assertTrue(os.path.exists(self.dst()))

    def test_sync_command_with_to(self):
        h = self.setup_pair()
        self.submit(h, [self.task()])
        h.close()
        self._hubs.remove(h)
        code, out = self.run_cli(["sync", "t1", "--db", self.db,
                                  "--to", "mesh://笔记本/work", "--json"])
        self.assertEqual(code, 0, out)
        self.assertIn('"ok": true', out)
        self.assertTrue(os.path.exists(self.dst()))

    def test_sync_command_reports_unknown_job(self):
        h = self.setup_pair()
        h.close()
        self._hubs.remove(h)
        code, out = self.run_cli(["sync", "nope", "--db", self.db])
        self.assertEqual(code, 1)
        self.assertIn("nope", out)


# --------------------------------------------------------------------------
# 补送：作业 done 了，但产物没送到
# --------------------------------------------------------------------------
class TestDeliveryRetry(SyncBase):
    """目标离线不该让人手工盯着 —— 控制面自己退避重试，但**不重跑作业**。"""

    def offline_pair(self):
        """目标一开始就连不上，作业照常跑完。"""
        h = self.setup_pair(mirror=True)
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        return h

    def test_failed_push_leaves_a_pending_delivery(self):
        h = self.offline_pair()
        rows = h.queue.deliveries()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["state"], Q.DELIVERY_PENDING)
        self.assertEqual(rows[0]["to"], "mesh://笔记本/work")
        self.assertEqual(rows[0]["attempts"], 1)
        self.assertEqual(h.queue.get("t1").state, Q.DONE, "作业本身是成功的")
        self.assertEqual(self.job(h).result["sync"]["pending"], ["mesh://笔记本/work"])

    def test_not_retried_before_backoff(self):
        h = self.offline_pair()
        self.assertEqual(h.retry_deliveries(now=time.time()), [],
                         "刚失败就重试等于原地打转")

    def test_retry_delivers_when_the_target_comes_back(self):
        h = self.offline_pair()
        h.add_node("笔记本", self.dst_srv.url)          # 目标回来了
        later = time.time() + H.SYNC_RETRY_BASE + 1
        out = h.retry_deliveries(now=later)

        self.assertEqual([r["state"] for r in out], ["done"])
        self.assertEqual(self.read(self.dst()), self.read(os.path.join(self.src_dir, "doc.txt")))
        row = h.queue.deliveries()[0]
        self.assertEqual((row["state"], row["attempts"]), (Q.DELIVERY_DONE, 2))

    def test_result_is_rewritten_after_a_successful_retry(self):
        h = self.offline_pair()
        h.add_node("笔记本", self.dst_srv.url)
        h.retry_deliveries(now=time.time() + H.SYNC_RETRY_BASE + 1)
        sync = self.job(h).result["sync"]
        self.assertTrue(sync["ok"], "送到了就要改口，不能还挂着失败")
        self.assertEqual(sync["errors"], [])
        self.assertEqual(sync["delivered"], ["mesh://笔记本/work"])
        self.assertEqual(sync["pending"], [])
        self.assertIn("连不上", sync["first_error"],
                      "第一次为什么没送到要留着，别只剩一句'成功了'")
        self.assertEqual(self.job(h).state, Q.DONE)

    def test_retry_never_reruns_the_job(self):
        calls = []

        def runner(job):
            calls.append(job.id)
            with open(os.path.join(self.src_dir, "doc.txt"), "w") as f:
                f.write(BODY)
            return {"stdout": "ok"}

        node = A.Node("手机", roots=[{"name": "work", "path": self.src_dir}],
                      tags=["源"], runner=runner)
        h = self.hub()
        h.add_node("手机", self.serve(node).url)
        _, dst_srv = self.sink()
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        h.add_node("笔记本", dst_srv.url)
        for i in range(3):
            h.retry_deliveries(now=time.time() + 3600 * (i + 1))
        self.assertEqual(calls, ["t1"], "补送产物，不是重做作业")

    def test_backoff_grows_and_caps(self):
        self.assertEqual(H._backoff(1), H.SYNC_RETRY_BASE)
        self.assertEqual(H._backoff(2), H.SYNC_RETRY_BASE * 2)
        self.assertEqual(H._backoff(3), H.SYNC_RETRY_BASE * 4)
        self.assertEqual(H._backoff(50), H.SYNC_RETRY_MAX)

    def test_backoff_is_actually_applied(self):
        h = self.offline_pair()
        then = time.time() + H.SYNC_RETRY_BASE + 1
        h.retry_deliveries(now=then)                    # 第二次也失败
        row = h.queue.deliveries()[0]
        self.assertEqual(row["state"], Q.DELIVERY_PENDING)
        self.assertEqual(row["attempts"], 2)
        self.assertGreaterEqual(row["next_at"] - then, H._backoff(2) - 1)

    def test_gives_up_after_max_attempts(self):
        h = self.offline_pair()
        row = h.queue.deliveries()[0]
        h.queue.update_delivery(row["id"], attempts=H.SYNC_MAX_ATTEMPTS - 1)
        h.retry_deliveries(now=time.time() + H.SYNC_RETRY_MAX + 1)
        row = h.queue.deliveries()[0]
        self.assertEqual(row["state"], Q.DELIVERY_FAILED)
        self.assertIn("补送", row["error"])
        self.assertEqual(h.retry_deliveries(now=time.time() + 10 ** 6), [],
                         "failed 的不再自动重试")

    def test_permanent_error_is_not_retried(self):
        """只读 root 这种事再试一百次也一样，立刻置 failed 让人看见。"""
        h = self.setup_pair(dst_mode="ro")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        row = h.queue.deliveries()[0]
        self.assertEqual(row["state"], Q.DELIVERY_FAILED)
        self.assertIn("只读", row["error"])
        self.assertEqual(h.retry_deliveries(now=time.time() + 10 ** 6), [])

    def test_unregistered_target_is_terminal(self):
        h = self.setup_pair()
        self.submit(h, [self.task(sync=["mesh://还没装的机器/work"])])
        row = h.queue.deliveries()[0]
        self.assertEqual(row["state"], Q.DELIVERY_FAILED)
        self.assertIn("未注册", row["error"])

    def test_one_offline_target_does_not_block_the_other(self):
        other = os.path.join(self.tmp, "树莓派")
        os.makedirs(other)
        h = self.setup_pair()
        _, srv = self.sink("树莓派", path=other)
        h.add_node("树莓派", srv.url)
        h.add_node("离线机", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://树莓派/work", "mesh://离线机/work"])])

        states = {r["to"]: r["state"] for r in h.queue.deliveries()}
        self.assertEqual(states["mesh://树莓派/work"], Q.DELIVERY_DONE)
        self.assertEqual(states["mesh://离线机/work"], Q.DELIVERY_PENDING)
        self.assertTrue(os.path.exists(os.path.join(other, "doc.txt")))

    def test_deliveries_survive_a_hub_restart(self):
        """补送表在库里：控制面重启之后接着补，不用人再推一次。"""
        h = self.offline_pair()
        h.close()
        self._hubs.remove(h)

        h2 = self.hub()                                # 新进程
        h2.add_node("笔记本", self.dst_srv.url)
        out = h2.retry_deliveries(now=time.time() + H.SYNC_RETRY_BASE + 1)
        self.assertEqual([r["state"] for r in out], ["done"])
        self.assertTrue(os.path.exists(self.dst()))

    def test_tick_runs_the_retry(self):
        h = self.offline_pair()
        h.add_node("笔记本", self.dst_srv.url)
        row = h.queue.deliveries()[0]
        h.queue.update_delivery(row["id"], next_at=0)  # 让它这一轮就到点
        report = h.tick()
        self.assertEqual([r["state"] for r in report.get("retried") or []], ["done"])
        self.assertTrue(os.path.exists(self.dst()))

    def test_standby_hub_does_not_retry(self):
        """两个控制面只有一个在派发，补送也一样 —— 否则同一个文件推两遍。"""
        h = self.offline_pair()
        h.add_node("笔记本", self.dst_srv.url)
        row = h.queue.deliveries()[0]
        h.queue.update_delivery(row["id"], next_at=0)

        h.ensure_leadership()                          # 这一台拿着调度权
        standby = self.hub()
        standby.add_node("笔记本", self.dst_srv.url)
        report = standby.tick()
        self.assertIsNotNone(report.get("standby"))
        self.assertNotIn("retried", report)
        self.assertEqual(h.queue.deliveries()[0]["state"], Q.DELIVERY_PENDING)

    def test_manual_sync_clears_a_failed_delivery(self):
        h = self.setup_pair(dst_mode="ro")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        self.assertEqual(h.queue.deliveries()[0]["state"], Q.DELIVERY_FAILED)

        # 把目标改成可写（换一个 root），手工重推
        os.makedirs(os.path.join(self.tmp, "可写"))
        node = A.Node("笔记本", roots=[{"name": "work", "path": self.dst_dir}],
                      runner=lambda job: {"stdout": "-"})
        srv = self.serve(node)
        h.add_node("笔记本", srv.url)
        report = h.sync_job("t1")
        self.assertTrue(report["ok"], report["errors"])
        self.assertEqual(h.queue.deliveries()[0]["state"], Q.DELIVERY_DONE)


class TestDeliveriesCli(SyncBase):
    def run_cli(self, argv):
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            code = CLI.main(argv)
        return code, buf.getvalue()

    def test_lists_pending(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        h.close()
        self._hubs.remove(h)

        code, out = self.run_cli(["deliveries", "--db", self.db])
        self.assertEqual(code, 0, out)
        self.assertIn("待补送", out)
        self.assertIn("mesh://笔记本/work", out)

    def test_json_and_state_filter(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        h.close()
        self._hubs.remove(h)

        code, out = self.run_cli(["deliveries", "--db", self.db,
                                  "--state", "pending", "--json"])
        self.assertEqual(code, 0, out)
        self.assertIn('"state": "pending"', out)
        code, out = self.run_cli(["deliveries", "--db", self.db, "--state", "done"])
        self.assertEqual(code, 0, out)
        self.assertIn("没有符合条件", out)

    def test_empty_queue(self):
        h = self.setup_pair()
        self.submit(h, [self.task()])              # 没有 sync，自然没有补送
        h.close()
        self._hubs.remove(h)
        code, out = self.run_cli(["deliveries", "--db", self.db])
        self.assertEqual(code, 0, out)
        self.assertIn("没有要补送", out)

    def test_status_mentions_outstanding_deliveries(self):
        h = self.setup_pair()
        h.add_node("笔记本", "http://127.0.0.1:1")
        self.submit(h, [self.task(sync=["mesh://笔记本/work"])])
        h.close()
        self._hubs.remove(h)
        code, out = self.run_cli(["status", "--db", self.db])
        self.assertEqual(code, 0, out)
        self.assertIn("待补送 1", out)


if __name__ == "__main__":
    unittest.main()
