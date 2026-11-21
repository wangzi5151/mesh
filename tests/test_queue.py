"""queue.py 的测试：入队、依赖门控、租约、重试、崩溃恢复、并发抢占。"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import queue as Q  # noqa: E402


def job(jid, project="p1", deps=None, priority=0, **kw):
    return Q.Job(jid, project, deps=deps or [], priority=priority, **kw)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-queue-")
        self.path = os.path.join(self.tmp, "q.db")
        self.q = Q.Queue(self.path)

    def tearDown(self):
        self.q.close()
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestEnqueue(Base):
    def test_add_and_get(self):
        self.q.add(job("j1", title="写文档", prompt="hi", read=["src/*"], req={"tags": ["gpu"]}))
        got = self.q.get("j1")
        self.assertEqual(got.id, "j1")
        self.assertEqual(got.title, "写文档")
        self.assertEqual(got.read, ["src/*"])
        self.assertEqual(got.req, {"tags": ["gpu"]})
        self.assertEqual(got.state, Q.QUEUED)
        self.assertGreater(got.created_at, 0)

    def test_deps_stored(self):
        self.q.add(job("a"))
        self.q.add(job("b", deps=["a"]))
        self.assertEqual(self.q.get("b").deps, ["a"])
        self.assertEqual(self.q.dependents("a"), ["b"])

    def test_duplicate_rejected(self):
        self.q.add(job("j1"))
        with self.assertRaises(Q.QueueError):
            self.q.add(job("j1"))

    def test_missing_fields_rejected(self):
        with self.assertRaises(Q.QueueError):
            self.q.add(Q.Job("", "p1"))
        with self.assertRaises(Q.QueueError):
            self.q.add(Q.Job("x", ""))

    def test_add_many_forward_reference(self):
        """依赖可以先于父作业插入。"""
        self.q.add_many([job("child", deps=["parent"]), job("parent")])
        self.assertEqual(self.q.get("child").deps, ["parent"])

    def test_list_filters(self):
        self.q.add(job("a", project="p1"))
        self.q.add(job("b", project="p2"))
        self.assertEqual([j.id for j in self.q.list(project="p1")], ["a"])
        self.assertEqual(len(self.q.list()), 2)


class TestReady(Base):
    def test_no_deps_is_ready(self):
        self.q.add(job("a"))
        self.assertEqual([j.id for j in self.q.ready()], ["a"])

    def test_blocked_by_unfinished_dep(self):
        self.q.add(job("a"))
        self.q.add(job("b", deps=["a"]))
        self.assertEqual([j.id for j in self.q.ready()], ["a"])

    def test_unblocked_when_dep_done(self):
        self.q.add(job("a"))
        self.q.add(job("b", deps=["a"]))
        self.q.complete("a")
        self.assertEqual([j.id for j in self.q.ready()], ["b"])

    def test_priority_ordering(self):
        self.q.add(job("low", priority=0))
        self.q.add(job("high", priority=10))
        self.assertEqual([j.id for j in self.q.ready()], ["high", "low"])


class TestLease(Base):
    def test_lease_marks_running(self):
        self.q.add(job("a"))
        leased = self.q.lease_one("node-1")
        self.assertIsNotNone(leased)
        self.assertEqual(leased.id, "a")
        got = self.q.get("a")
        self.assertEqual(got.state, Q.RUNNING)
        self.assertEqual(got.lease_owner, "node-1")
        self.assertGreater(got.lease_expires, got.started_at)
        self.assertEqual(got.attempts, 1, "领任务应把 attempts 计为 1")

    def test_two_leases_get_different_jobs(self):
        self.q.add(job("a"))
        self.q.add(job("b"))
        first = self.q.lease_one("n1")
        second = self.q.lease_one("n2")
        self.assertNotEqual(first.id, second.id)
        self.assertIsNone(self.q.lease_one("n3"))

    def test_lease_skips_non_ready(self):
        self.q.add(job("a"))
        self.q.add(job("b", deps=["a"]))
        leased = self.q.lease_one("n1")
        self.assertEqual(leased.id, "a")

    def test_lease_empty_queue(self):
        self.assertIsNone(self.q.lease_one("n1"))


class TestOutcome(Base):
    def test_complete(self):
        self.q.add(job("a"))
        self.q.lease_one("n1")
        self.q.complete("a", result={"ok": True}, feedback="done")
        got = self.q.get("a")
        self.assertEqual(got.state, Q.DONE)
        self.assertEqual(got.result, {"ok": True})
        self.assertEqual(got.lease_owner, "")
        self.assertGreater(got.finished_at, 0)

    def test_fail_does_not_double_count_attempts(self):
        """关键回归：lease 已经把 attempts 计为 1，fail 不应再加 1。"""
        self.q.add(job("a", max_attempts=2))
        self.q.lease_one("n1")
        self.assertEqual(self.q.get("a").attempts, 1)
        state = self.q.fail("a", error="boom")
        got = self.q.get("a")
        self.assertEqual(state, Q.QUEUED, "还有重试机会时应回队列")
        self.assertEqual(got.attempts, 1, "fail 不应重复累加 attempts")
        self.assertEqual(got.feedback, "boom")

    def test_fail_exhausts_attempts(self):
        self.q.add(job("a", max_attempts=2))
        self.q.lease_one("n1")
        self.assertEqual(self.q.fail("a", error="e1"), Q.QUEUED)
        self.q.lease_one("n1")
        self.assertEqual(self.q.get("a").attempts, 2)
        self.assertEqual(self.q.fail("a", error="e2"), Q.FAILED)
        self.assertEqual(self.q.get("a").state, Q.FAILED)

    def test_fail_non_retryable_goes_straight_to_failed(self):
        self.q.add(job("a", max_attempts=5))
        self.q.lease_one("n1")
        self.assertEqual(self.q.fail("a", error="fatal", retryable=False), Q.FAILED)

    def test_fail_unknown_job(self):
        with self.assertRaises(Q.QueueError):
            self.q.fail("nope")

    def test_cancel(self):
        self.q.add(job("a"))
        self.q.cancel("a", reason="不要了")
        got = self.q.get("a")
        self.assertEqual(got.state, Q.CANCELED)
        self.assertEqual(got.feedback, "不要了")


class TestReap(Base):
    def test_expired_lease_returns_to_queue(self):
        self.q.add(job("a"))
        t0 = 1000.0
        self.q.lease_one("n1", now=t0)
        self.assertEqual(self.q.get("a").attempts, 1)
        self.assertEqual(self.q.reap(now=t0 + 10), [], "租约未到期不应回收")
        reapd = self.q.reap(now=t0 + 10_000)
        self.assertEqual(reapd, ["a"])
        got = self.q.get("a")
        self.assertEqual(got.state, Q.QUEUED)
        self.assertEqual(got.lease_owner, "")
        self.assertEqual(got.attempts, 1, "基础设施故障不应消耗 agent 重试次数")

    def test_reap_then_release_again(self):
        self.q.add(job("a"))
        t0 = 1000.0
        self.q.lease_one("n1", now=t0)
        self.q.reap(now=t0 + 10_000)
        again = self.q.lease_one("n2", now=t0 + 10_001)
        self.assertIsNotNone(again)
        self.assertEqual(again.lease_owner, "n2")


class TestRetry(Base):
    def test_retry_failed(self):
        self.q.add(job("a", max_attempts=1))
        self.q.lease_one("n1")
        self.q.fail("a", error="x")
        self.assertEqual(self.q.get("a").state, Q.FAILED)
        self.q.retry("a", reset_attempts=True)
        got = self.q.get("a")
        self.assertEqual(got.state, Q.QUEUED)
        self.assertEqual(got.attempts, 0)

    def test_retry_rejects_queued(self):
        self.q.add(job("a"))
        with self.assertRaises(Q.QueueError):
            self.q.retry("a")

    def test_retry_unknown(self):
        with self.assertRaises(Q.QueueError):
            self.q.retry("nope")

    def test_block_then_retry(self):
        self.q.add(job("a"))
        self.q.block("a", reason="上游挂了")
        self.assertEqual(self.q.get("a").state, Q.BLOCKED)
        self.q.retry("a")
        self.assertEqual(self.q.get("a").state, Q.QUEUED)


class TestPersistence(Base):
    def test_survives_reopen(self):
        self.q.add(job("a", title="持久"))
        self.q.close()
        again = Q.Queue(self.path)
        try:
            got = again.get("a")
            self.assertEqual(got.title, "持久")
            self.assertEqual(got.state, Q.QUEUED)
        finally:
            again.close()
        self.q = Q.Queue(self.path)      # 给 tearDown 收尾

    def test_running_state_survives_crash(self):
        """模拟进程崩溃：关掉连接再打开，running 作业仍在，可被 reap 回收。"""
        self.q.add(job("a"))
        t0 = 1000.0
        self.q.lease_one("dead-node", now=t0)
        self.q.close()

        revived = Q.Queue(self.path)
        try:
            self.assertEqual(revived.get("a").state, Q.RUNNING)
            self.assertEqual(revived.reap(now=t0 + 10_000), ["a"])
            self.assertEqual(revived.get("a").state, Q.QUEUED)
        finally:
            revived.close()
        self.q = Q.Queue(self.path)


class TestConcurrency(Base):
    def test_two_connections_never_double_lease(self):
        """两个独立连接争抢同一批作业，每条只能被领一次。"""
        for i in range(20):
            self.q.add(job("j%02d" % i))

        claimed = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def worker(name):
            q2 = Q.Queue(self.path)
            try:
                barrier.wait()
                while True:
                    leased = q2.lease_one(name)
                    if leased is None:
                        break
                    with lock:
                        claimed.append(leased.id)
            finally:
                q2.close()

        threads = [threading.Thread(target=worker, args=("n%d" % i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertEqual(len(claimed), 20, "应有 20 条被领走")
        self.assertEqual(len(set(claimed)), 20, "同一条作业被领了两次：%s" % claimed)


class TestStatsAndEvents(Base):
    def test_stats(self):
        self.q.add(job("a"))
        self.q.add(job("b"))
        self.q.lease_one("n1")
        stats = self.q.stats()
        self.assertEqual(stats.get(Q.QUEUED), 1)
        self.assertEqual(stats.get(Q.RUNNING), 1)

    def test_events_recorded(self):
        self.q.add(job("a"))
        self.q.lease_one("n1")
        self.q.complete("a")
        with self.q._lock:
            kinds = [r["kind"] for r in self.q._db.execute(
                "SELECT kind FROM events ORDER BY seq")]
        self.assertIn("add", kinds)
        self.assertIn("lease", kinds)
        self.assertIn("complete", kinds)


class TestJobModel(unittest.TestCase):
    def test_scope(self):
        j = Q.Job("x", "p", read=["a/*"], write=["b/*"])
        self.assertEqual(j.scope(), {"a/*", "b/*"})

    def test_dict_round_trip(self):
        j = Q.Job("x", "p", title="t", deps=["y"], req={"tags": ["g"]})
        back = Q.Job.from_dict(j.to_dict())
        self.assertEqual(back.id, "x")
        self.assertEqual(back.deps, ["y"])
        self.assertEqual(back.req, {"tags": ["g"]})

    def test_from_dict_missing_id(self):
        with self.assertRaises(Q.QueueError):
            Q.Job.from_dict({"project": "p"})

    def test_is_terminal(self):
        self.assertTrue(Q.Job("x", "p", state=Q.DONE).is_terminal)
        self.assertTrue(Q.Job("x", "p", state=Q.FAILED).is_terminal)
        self.assertFalse(Q.Job("x", "p", state=Q.QUEUED).is_terminal)

    def test_corrupt_json_tolerated(self):
        row = {"id": "x", "project": "p", "title": "", "prompt": "",
               "read_json": "{bad json", "write_json": "[]", "priority": 0,
               "req_json": "nope", "agent": "", "cmd": "", "model": "", "verify": "",
               "state": "queued", "attempts": 0, "max_attempts": 2,
               "lease_owner": "", "lease_expires": 0, "feedback": "",
               "result_json": "", "error": "", "created_at": 1, "updated_at": 1,
               "started_at": 0, "finished_at": 0}
        j = Q.Job.from_row(row)
        self.assertEqual(j.read, [])
        self.assertEqual(j.req, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
