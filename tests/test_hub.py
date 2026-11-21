"""hub.py 的测试：节点注册、心跳摘除、冲突拒绝、派发、失联退回、失败重试、租约回收。

这一层是控制面，所以测试全部走**真实的 Node + 真实 HTTP**：调度器的价值就在
"把作业扔到另一台机器上跑完再收回来"，用 mock 把网络换掉就测不到真正的故障路径了。
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import agent as A  # noqa: E402
from mesh import capability as C  # noqa: E402
from mesh import hub as H  # noqa: E402
from mesh import queue as Q  # noqa: E402
from mesh import task as T  # noqa: E402
from mesh import wire as W  # noqa: E402

DEAD_URL = "http://127.0.0.1:1"          # 谁都不会在这儿监听


def http_get(url, token=""):
    """浏览器式的裸 GET：返回 (状态码, 正文)。"""
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


class HubBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-hub-")
        self.work = os.path.join(self.tmp, "work")
        os.makedirs(os.path.join(self.work, "sub"))
        os.makedirs(os.path.join(self.work, "other"))
        self.db = os.path.join(self.tmp, "queue.db")
        self._servers = []
        self._hubs = []

    def tearDown(self):
        for srv in self._servers:
            srv.stop()
        for h in self._hubs:
            h.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- 装配 ------------------------------------------------------------
    def hub(self, **kw):
        kw.setdefault("poll", 0.01)
        kw.setdefault("dispatch_timeout", 10.0)
        h = H.Hub(self.db, **kw)
        self._hubs.append(h)
        return h

    def node(self, node_id="n1", **kw):
        kw.setdefault("roots", [{"name": "work", "path": self.work, "mode": "rw"}])
        kw.setdefault("runner", lambda job: {"stdout": "done " + job.id})
        node = A.Node(node_id, **kw)
        srv = node.build_server(port=0)
        srv.start()
        self._servers.append(srv)
        return node, srv

    def attach(self, h, node, srv, node_id=None, **kw):
        h.add_node(node_id or node.node_id, srv.url, **kw)
        return h

    def plan(self, project="p", tasks=None):
        return T.Plan(project, [T.TaskSpec.from_dict(t) for t in (tasks or [])])


# --------------------------------------------------------------------------
# 节点接入信息
# --------------------------------------------------------------------------
class TestNodeRef(unittest.TestCase):
    def test_url_trailing_slash_stripped(self):
        ref = H.NodeRef("n1", "http://h:1/")
        self.assertEqual(ref.url, "http://h:1")

    def test_defaults(self):
        ref = H.NodeRef("n1", "http://h:1")
        self.assertEqual((ref.token, ref.tags, ref.weight, ref.failures), ("", [], 1.0, 0))

    def test_to_dict_has_no_token(self):
        ref = H.NodeRef("n1", "http://h:1", token="secret")
        self.assertNotIn("token", ref.to_dict())
        self.assertEqual(ref.to_dict()["node_id"], "n1")

    def test_weight_coerced_to_float(self):
        self.assertEqual(H.NodeRef("n1", "http://h:1", weight=2).weight, 2.0)


class TestNodeManagement(HubBase):
    def test_add_requires_id_and_url(self):
        h = self.hub()
        with self.assertRaises(H.HubError):
            h.add_node("", "http://h:1")
        with self.assertRaises(H.HubError):
            h.add_node("n1", "")

    def test_nodes_sorted_by_id(self):
        h = self.hub()
        h.add_node("b", "http://h:2")
        h.add_node("a", "http://h:1")
        self.assertEqual([n["node_id"] for n in h.nodes()], ["a", "b"])

    def test_add_is_idempotent_by_id(self):
        h = self.hub()
        h.add_node("n1", "http://h:1")
        h.add_node("n1", "http://h:2")
        self.assertEqual(len(h.nodes()), 1)
        self.assertEqual(h.nodes()[0]["url"], "http://h:2")

    def test_remove(self):
        h = self.hub()
        h.add_node("n1", "http://h:1")
        self.assertTrue(h.remove_node("n1"))
        self.assertFalse(h.remove_node("n1"))
        self.assertEqual(h.nodes(), [])

    def test_client_for_unknown_node(self):
        with self.assertRaises(H.HubError) as ctx:
            self.hub().client_for("nope")
        self.assertIn("未注册", str(ctx.exception))

    def test_client_carries_token_and_timeout(self):
        h = self.hub(dispatch_timeout=7.0)
        h.add_node("n1", "http://h:1", token="tok")
        client = h.client_for("n1")
        self.assertEqual(client.token, "tok")
        self.assertEqual(client.timeout, 7.0)

    def test_context_manager_closes(self):
        with H.Hub(self.db) as h:
            h.add_node("n1", "http://h:1")
        with self.assertRaises(Exception):
            h.queue.stats()


# --------------------------------------------------------------------------
# 提交
# --------------------------------------------------------------------------
class TestSubmit(HubBase):
    def test_submit_returns_ids_and_persists(self):
        h = self.hub()
        ids = h.submit({"project": "p", "tasks": [
            {"id": "a", "title": "A"},
            {"id": "b", "title": "B"},
        ]})
        self.assertEqual(sorted(ids), ["a", "b"])
        self.assertEqual(sorted(j.id for j in h.jobs(project="p")), ["a", "b"])

    def test_submit_accepts_plan_object(self):
        h = self.hub()
        ids = h.submit(self.plan(tasks=[{"id": "a"}]))
        self.assertEqual(ids, ["a"])

    def test_submit_rejects_unsorted_write_conflict(self):
        h = self.hub()
        plan = self.plan(tasks=[
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"]},
        ])
        with self.assertRaises(H.HubError) as ctx:
            h.submit(plan)
        msg = str(ctx.exception)
        self.assertIn("冲突", msg)
        self.assertIn("都写 src/*.py", msg)
        self.assertEqual(h.jobs(), [])          # 一条都不该落库

    def test_conflict_with_dependency_is_allowed(self):
        h = self.hub()
        ids = h.submit(self.plan(tasks=[
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"], "deps": ["a"]},
        ]))
        self.assertEqual(len(ids), 2)

    def test_conflict_can_be_forced(self):
        h = self.hub()
        ids = h.submit(self.plan(tasks=[
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"]},
        ]), allow_conflicts=True)
        self.assertEqual(len(ids), 2)

    def test_empty_plan_rejected(self):
        with self.assertRaises(T.PlanError):
            self.hub().submit({"project": "p", "tasks": []})

    def test_duplicate_id_rejected(self):
        h = self.hub()
        h.submit(self.plan(project="p", tasks=[{"id": "a"}]))
        with self.assertRaises(Q.QueueError):
            h.submit(self.plan(project="p", tasks=[{"id": "a"}]))

    def test_deps_persisted(self):
        h = self.hub()
        h.submit(self.plan(tasks=[{"id": "a"}, {"id": "b", "deps": ["a"]}]))
        self.assertEqual(h.queue.get("b").deps, ["a"])


# --------------------------------------------------------------------------
# 心跳
# --------------------------------------------------------------------------
class TestRefresh(HubBase):
    def test_live_node_enters_registry(self):
        node, srv = self.node(slots_total=2)
        h = self.attach(self.hub(), node, srv)
        report = h.refresh()
        self.assertEqual(report["live"], ["n1"])
        self.assertEqual(report["dead"], [])
        cap = h.registry.get("n1")
        self.assertIsNotNone(cap)
        self.assertEqual(cap.slots_total, 2)

    def test_registry_tags_merged_with_hub_tags(self):
        node, srv = self.node(tags=["local"])
        h = self.attach(self.hub(), node, srv, tags=["gpu", "local"])
        h.refresh()
        self.assertEqual(h.registry.get("n1").tags, ["gpu", "local"])

    def test_dead_node_dropped_from_registry(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.refresh()
        self.assertIn("n1", h.registry)

        srv.stop()
        self._servers.remove(srv)
        report = h.refresh()
        self.assertEqual(report["dead"], ["n1"])
        self.assertNotIn("n1", h.registry)
        self.assertEqual(h.nodes()[0]["failures"], 1)
        self.assertNotEqual(h.nodes()[0]["last_error"], "")

    def test_failures_reset_when_node_returns(self):
        node, srv = self.node()
        h = self.hub()
        ref = h.add_node("n1", DEAD_URL)
        self.assertEqual(h.refresh()["dead"], ["n1"])
        self.assertEqual(ref.failures, 1)

        ref.url = srv.url.replace("http://", "http://")   # 模拟节点重新上线
        self.assertEqual(h.refresh()["live"], ["n1"])
        self.assertEqual(ref.failures, 0)
        self.assertEqual(ref.last_error, "")

    def test_refresh_with_no_nodes(self):
        self.assertEqual(self.hub().refresh(), {"live": [], "dead": []})


# --------------------------------------------------------------------------
# 派发
# --------------------------------------------------------------------------
class TestDispatch(HubBase):
    def test_single_task_completes(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "cmd": "true"}]))

        report = h.tick()
        self.assertEqual(report["completed"], ["a"])
        self.assertEqual(report["dispatched"][0], {"job": "a", "node": "n1"})

        job = h.queue.get("a")
        self.assertEqual(job.state, Q.DONE)
        self.assertTrue(job.result["ok"])
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.lease_owner, "")
        self.assertEqual(job.lease_expires, 0.0)

    def test_feedback_saved_from_stdout(self):
        node, srv = self.node(runner=lambda job: {"stdout": "hello from node"})
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "cmd": "x"}]))
        h.tick()
        self.assertEqual(h.queue.get("a").feedback, "hello from node")

    def test_dependencies_are_respected_across_rounds(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a"}, {"id": "b", "deps": ["a"]}]))

        first = h.tick()
        self.assertEqual([d["job"] for d in first["dispatched"]], ["a"])
        self.assertEqual(h.queue.get("b").state, Q.QUEUED)

        second = h.tick()
        self.assertEqual([d["job"] for d in second["dispatched"]], ["b"])
        self.assertEqual(h.queue.get("b").state, Q.DONE)

    def test_priority_order(self):
        node, srv = self.node(slots_total=1)
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[
            {"id": "low", "priority": 0},
            {"id": "high", "priority": 9},
        ]))
        h.tick()
        self.assertEqual(h.queue.get("high").state, Q.DONE)

    def test_two_nodes_share_load(self):
        n1, s1 = self.node("n1")
        n2, s2 = self.node("n2")
        h = self.hub()
        self.attach(h, n1, s1)
        self.attach(h, n2, s2)
        h.submit(self.plan(tasks=[{"id": "a"}, {"id": "b"}]))
        report = h.tick()
        self.assertEqual(len(report["completed"]), 2)
        self.assertEqual({d["node"] for d in report["dispatched"]}, {"n1", "n2"})

    def test_scope_violation_becomes_failure(self):
        def runner(job):
            with open(os.path.join(self.work, "other", "escape.txt"), "w") as f:
                f.write("out of scope")
            return {"stdout": "wrote outside"}

        node, srv = self.node(runner=runner)
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "write": ["sub/*.txt"], "max_attempts": 1}]))
        report = h.tick()
        self.assertEqual(report["failed"][0]["job"], "a")
        self.assertIn("越界", report["failed"][0]["error"])
        self.assertEqual(h.queue.get("a").state, Q.FAILED)


# --------------------------------------------------------------------------
# 派不出去
# --------------------------------------------------------------------------
class TestBlocked(HubBase):
    def test_no_nodes_at_all(self):
        h = self.hub()
        h.submit(self.plan(tasks=[{"id": "a"}]))
        report = h.tick()
        self.assertEqual(report["blocked"][0]["job"], "a")
        self.assertEqual(h.queue.get("a").state, Q.QUEUED)
        self.assertEqual(h.queue.get("a").attempts, 0)     # 没被领过

    def test_missing_root_explained(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "req": {"root": "nope"}}]))
        report = h.tick()
        self.assertEqual(report["blocked"][0]["job"], "a")
        self.assertTrue(any("nope" in line for line in report["blocked"][0]["why"]))

    def test_slots_exhausted_explained(self):
        node, srv = self.node(slots_total=1)
        node.slots_used = 1                      # 节点自己报"占满了"
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a"}]))
        report = h.tick()
        self.assertEqual(report["blocked"][0]["job"], "a")
        self.assertTrue(any("槽位已满" in line for line in report["blocked"][0]["why"]))

    def test_round_capacity_not_overcommitted(self):
        """一轮之内也不能把作业超发给同一个槽位 —— 快照是上一轮拍的。"""
        node, srv = self.node(slots_total=1)
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a"}, {"id": "b"}]))
        report = h.tick()
        self.assertEqual(len(report["dispatched"]), 1)
        self.assertEqual(len(report["completed"]), 1)
        self.assertEqual(len(report["blocked"]), 1)
        self.assertTrue(any("本轮已派" in line for line in report["blocked"][0]["why"]))

    def test_stale_registry_entry_not_picked(self):
        h = self.hub(ttl=0.01)
        cap = C.Capability("ghost", slots_total=1, last_seen=time.time() - 100)
        h.registry.register(cap)
        h.submit(self.plan(tasks=[{"id": "a"}]))
        self.assertEqual(h.tick()["blocked"][0]["job"], "a")


class TestUnlease(HubBase):
    def test_unreachable_node_returns_job_to_queue(self):
        """注册表里能挑到节点，但连接失败 —— 作业必须退回队列。"""
        h = self.hub()
        h.registry.register(C.Capability("ghost", slots_total=1, last_seen=time.time()))
        h.submit(self.plan(tasks=[{"id": "a"}]))

        report = h.tick()
        self.assertEqual(report["unleased"], ["a"])
        self.assertEqual(report["dispatched"], [])
        job = h.queue.get("a")
        self.assertEqual(job.state, Q.QUEUED)
        self.assertIn("不可达", job.error)

    def test_unlease_does_not_mark_failed(self):
        h = self.hub()
        h.registry.register(C.Capability("ghost", slots_total=1, last_seen=time.time()))
        h.submit(self.plan(tasks=[{"id": "a"}]))
        self.assertEqual(h.tick()["failed"], [])
        self.assertEqual(h.queue.get("a").state, Q.QUEUED)


# --------------------------------------------------------------------------
# 失败与重试
# --------------------------------------------------------------------------
class TestFailure(HubBase):
    def failing_node(self, **kw):
        kw.setdefault("runner", lambda job: {"rc": 3, "stderr": "boom"})
        return self.node(**kw)

    def test_exhausted_attempts_mark_failed(self):
        node, srv = self.failing_node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "max_attempts": 1}]))

        report = h.tick()
        self.assertEqual(report["failed"][0]["job"], "a")
        self.assertEqual(report["failed"][0]["state"], Q.FAILED)
        job = h.queue.get("a")
        self.assertEqual(job.state, Q.FAILED)
        self.assertEqual(job.error, "boom")

    def test_retryable_failure_goes_back_to_queue(self):
        node, srv = self.failing_node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "max_attempts": 2}]))

        first = h.tick()
        self.assertEqual(first["failed"][0]["state"], Q.QUEUED)
        job = h.queue.get("a")
        self.assertEqual(job.state, Q.QUEUED)
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.feedback, "boom")     # 下一次 attempt 带着失败原因

        second = h.tick()
        self.assertEqual(second["failed"][0]["state"], Q.FAILED)
        self.assertEqual(h.queue.get("a").attempts, 2)

    def test_failure_blocks_downstream(self):
        node, srv = self.failing_node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[
            {"id": "a", "max_attempts": 1},
            {"id": "b", "deps": ["a"]},
        ]))
        h.tick()
        self.assertEqual(h.queue.get("a").state, Q.FAILED)
        self.assertEqual(h.queue.get("b").state, Q.QUEUED)
        self.assertEqual([d["job"] for d in h.tick()["dispatched"]], [])

    def test_error_from_exception_field_wins(self):
        node, srv = self.node(runner=lambda job: {"ok": False, "error": "自定义错误"})
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a", "max_attempts": 1}]))
        h.tick()
        self.assertEqual(h.queue.get("a").error, "自定义错误")


# --------------------------------------------------------------------------
# 租约
# --------------------------------------------------------------------------
class TestLease(HubBase):
    def test_expired_lease_is_reaped(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a"}]))
        self.assertIsNotNone(h.queue.lease("a", "other-hub", now=1000.0))

        report = h.tick(now=1000.0 + h.queue.lease_secs + 1)
        self.assertEqual(report["reaped"], ["a"])

    def test_live_lease_not_reaped(self):
        node, srv = self.node()
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[{"id": "a"}]))
        h.queue.lease("a", "other-hub", now=1000.0)
        self.assertEqual(h.queue.reap(now=1000.0), [])
        self.assertEqual(h.queue.get("a").state, Q.RUNNING)

    def test_default_lease_outlives_dispatch_timeout(self):
        """回归：租约短于派发超时会让长作业被重复执行。"""
        h = H.Hub(self.db, dispatch_timeout=1800.0)
        self._hubs.append(h)
        self.assertGreater(h.queue.lease_secs, 1800.0)

    def test_explicit_lease_honoured(self):
        h = H.Hub(self.db, dispatch_timeout=10.0, lease=42.0)
        self._hubs.append(h)
        self.assertEqual(h.queue.lease_secs, 42.0)

    def test_lease_method_not_shadowed_by_duration(self):
        """回归：租约秒数曾经叫 ``self.lease``，把 ``Queue.lease()`` 方法整个遮住，
        调度器一调就 ``'float' object is not callable`` —— 作业一条也派不出去。"""
        q = Q.Queue(os.path.join(self.tmp, "shadow.db"))
        try:
            self.assertTrue(callable(q.lease))
            q.add(Q.Job("a", "p"))
            self.assertIsNotNone(q.lease("a", "owner"))
            self.assertEqual(q.get("a").state, Q.RUNNING)
        finally:
            q.close()


# --------------------------------------------------------------------------
# 跑到空闲
# --------------------------------------------------------------------------
class TestRunUntilIdle(HubBase):
    def test_drains_a_chain(self):
        node, srv = self.node(slots_total=4)
        h = self.attach(self.hub(), node, srv)
        h.submit(self.plan(tasks=[
            {"id": "a"},
            {"id": "b", "deps": ["a"]},
            {"id": "c", "deps": ["b"]},
        ]))
        out = h.run_until_idle(timeout=10.0, poll=0.01)
        self.assertNotIn("timeout", out)
        self.assertEqual(out["stats"][Q.DONE], 3)
        self.assertGreaterEqual(out["rounds"], 3)

    def test_timeout_when_nothing_can_run(self):
        h = self.hub()
        h.submit(self.plan(tasks=[{"id": "a"}]))
        out = h.run_until_idle(timeout=0.05, poll=0.01)
        self.assertTrue(out.get("timeout"))
        self.assertEqual(out["stats"][Q.QUEUED], 1)


# --------------------------------------------------------------------------
# 面板
# --------------------------------------------------------------------------
class TestStatus(HubBase):
    def test_status_shape(self):
        node, srv = self.node(slots_total=3, tags=["local"])
        h = self.attach(self.hub(), node, srv, tags=["gpu"])
        h.refresh()
        h.submit(self.plan(project="proj", tasks=[{"id": "a"}]))
        st = h.status()

        self.assertEqual(len(st["nodes"]), 1)
        entry = st["nodes"][0]
        self.assertEqual(entry["node_id"], "n1")
        self.assertTrue(entry["online"])
        self.assertEqual(entry["slots_total"], 3)
        self.assertEqual(entry["tags"], ["gpu", "local"])
        self.assertEqual(entry["roots"], ["work"])
        self.assertEqual(st["jobs"], {Q.QUEUED: 1})
        self.assertEqual(st["projects"], ["proj"])

    def test_offline_node_reported(self):
        h = self.hub()
        h.add_node("n1", DEAD_URL)
        self.assertFalse(h.status()["nodes"][0]["online"])

    def test_jobs_filters(self):
        h = self.hub()
        h.submit(self.plan(project="p1", tasks=[{"id": "a"}, {"id": "b"}]))
        h.submit(self.plan(project="p2", tasks=[{"id": "c"}]))
        self.assertEqual(len(h.jobs(project="p1")), 2)
        self.assertEqual([j.id for j in h.jobs(project="p2")], ["c"])
        self.assertEqual(h.jobs(state=Q.DONE), [])
        self.assertEqual(len(h.jobs(limit=1)), 1)

    def test_repr(self):
        h = self.hub()
        h.add_node("n1", "http://h:1")
        self.assertIn("1 nodes", repr(h))


# --------------------------------------------------------------------------
# 并发提交
# --------------------------------------------------------------------------
class TestConcurrency(HubBase):
    def test_concurrent_submits_lose_nothing(self):
        h = self.hub()
        errors = []

        def worker(n):
            try:
                h.submit(self.plan(project="p%d" % n, tasks=[{"id": "t%d" % n}]))
            except Exception as exc:      # pragma: no cover - 只在失败时触发
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(h.jobs()), 8)


# --------------------------------------------------------------------------
# 对外接口与面板
# --------------------------------------------------------------------------
class TestServer(HubBase):
    def setUp(self):
        super(TestServer, self).setUp()
        node, srv = self.node(runner=lambda job: {"stdout": "done " + job.id})
        self.hub = self.attach(self.hub_factory(token="tok"), node, srv)
        self.hub.submit(self.plan(tasks=[{"id": "a", "title": "跑一下"}]))
        self.hub.tick()
        self.server = self.hub.build_server(port=0)
        self.server.start()

    def tearDown(self):
        self.server.stop()
        super(TestServer, self).tearDown()

    # 面板路径长这样：self.hub 是 Hub，但 HubBase.hub 是工厂函数
    def hub_factory(self, **kw):
        kw.setdefault("poll", 0.01)
        h = H.Hub(self.db, **kw)
        self._hubs.append(h)
        return h

    def test_health_needs_no_token(self):
        status, body = http_get(self.server.url + "/health")
        self.assertEqual(status, 200)
        self.assertIn("mesh", body)

    def test_get_without_token_rejected(self):
        status, _body = http_get(self.server.url + "/")
        self.assertEqual(status, 401)

    def test_get_with_query_token(self):
        status, body = http_get(self.server.url + "/?token=tok")
        self.assertEqual(status, 200)
        self.assertIn("a", body)
        self.assertIn("跑一下", body)

    def test_get_unknown_path(self):
        status, _body = http_get(self.server.url + "/nope?token=tok")
        self.assertEqual(status, 404)

    def test_status_json(self):
        status, body = http_get(self.server.url + "/status?token=tok")
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["r"]["jobs"][Q.DONE], 1)
        self.assertEqual(payload["r"]["nodes"][0]["node_id"], "n1")

    def test_rpc_ping_and_status(self):
        client = W.Client(self.server.url, token="tok")
        self.assertIn("pong", client.call("ping"))
        self.assertEqual(client.call("status")["projects"], ["p"])

    def test_rpc_jobs_filter(self):
        client = W.Client(self.server.url, token="tok")
        self.assertEqual([j["id"] for j in client.call("jobs")], ["a"])
        self.assertEqual(client.call("jobs", {"state": Q.QUEUED}), [])

    def test_rpc_submit(self):
        client = W.Client(self.server.url, token="tok")
        out = client.call("submit", {"plan": {"project": "p2",
                                              "tasks": [{"id": "z"}]}})
        self.assertEqual(out["ids"], ["z"])
        self.assertEqual(self.hub.queue.get("z").project, "p2")

    def test_rpc_needs_token(self):
        client = W.Client(self.server.url)
        with self.assertRaises(W.WireError) as ctx:
            client.call("status")
        self.assertEqual(ctx.exception.status, 401)


if __name__ == "__main__":
    unittest.main(verbosity=2)
