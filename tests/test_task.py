"""task.py 的测试：glob 匹配、作用域强制、DAG 校验、并行冲突检测。"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import task as T  # noqa: E402
from mesh.queue import Job  # noqa: E402


class TestGlobMatch(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(T.glob_match("a/b.py", "a/b.py"))
        self.assertFalse(T.glob_match("a/b.py", "a/c.py"))

    def test_single_star_within_segment(self):
        self.assertTrue(T.glob_match("src/*.py", "src/a.py"))
        self.assertFalse(T.glob_match("src/*.py", "src/a/b.py"), "单星不应跨目录")

    def test_double_star_crosses_dirs(self):
        self.assertTrue(T.glob_match("src/**/*.py", "src/a/b/c.py"))
        self.assertTrue(T.glob_match("src/**/*.py", "src/c.py"), "** 应能匹配 0 段")

    def test_double_star_tail(self):
        self.assertTrue(T.glob_match("src/**", "src/anything/at/all.txt"))

    def test_question_mark(self):
        self.assertTrue(T.glob_match("a?.py", "ab.py"))
        self.assertFalse(T.glob_match("a?.py", "abc.py"))

    def test_normalizes_slashes(self):
        self.assertTrue(T.glob_match("/a//b.py", "/a/b.py"))

    def test_empty_pattern(self):
        self.assertFalse(T.glob_match("", "a.py"))


class TestOverlap(unittest.TestCase):
    def test_identical(self):
        self.assertTrue(T.globs_may_overlap("a/b.py", "a/b.py"))

    def test_distinct_literals_do_not_overlap(self):
        self.assertFalse(T.globs_may_overlap("a/b.py", "a/c.py"))

    def test_literal_inside_wildcard(self):
        self.assertTrue(T.globs_may_overlap("src/*.py", "src/a.py"))

    def test_disjoint_dirs(self):
        self.assertFalse(T.globs_may_overlap("src/**", "docs/**"))

    def test_recursive_covers_literal(self):
        self.assertTrue(T.globs_may_overlap("src/**", "src/x/y.py"))

    def test_static_prefix(self):
        self.assertEqual(T.static_prefix("a/b/*/c.py"), ["a", "b"])
        self.assertEqual(T.static_prefix("a/b/c.py"), ["a", "b", "c.py"])


class TestScope(unittest.TestCase):
    def setUp(self):
        self.job = Job("j1", "p", read=["docs/**"], write=["src/**/*.py"])

    def test_in_scope_write(self):
        self.assertTrue(T.in_scope(self.job, "src/a/b.py"))
        self.assertFalse(T.in_scope(self.job, "src/a/b.md"))

    def test_check_scope_passes(self):
        self.assertEqual(T.check_scope(self.job, ["src/a.py", "src/b/c.py"]), [])

    def test_check_scope_reports_violations(self):
        bad = T.check_scope(self.job, ["src/a.py", "README.md", "src/x.md"])
        self.assertEqual(bad, ["README.md", "src/x.md"])

    def test_read_scope_includes_both(self):
        self.assertTrue(T.in_scope(self.job, "docs/x.md", mode="read"))
        self.assertTrue(T.in_scope(self.job, "src/a.py", mode="read"))

    def test_job_without_write_scope_cannot_write(self):
        job = Job("j2", "p", read=["docs/**"])
        self.assertEqual(T.check_scope(job, ["docs/x.md"]), ["docs/x.md"])


class TestTaskSpec(unittest.TestCase):
    def test_dict_round_trip(self):
        spec = T.TaskSpec("t1", title="x", read=["a"], write=["b"],
                          deps=["t0"], req={"tags": ["gpu"]}, priority=5)
        back = T.TaskSpec.from_dict(spec.to_dict())
        self.assertEqual(back.to_dict(), spec.to_dict())

    def test_missing_id(self):
        with self.assertRaises(T.PlanError):
            T.TaskSpec.from_dict({"title": "x"})

    def test_to_job(self):
        spec = T.TaskSpec("t1", title="x", write=["src/*.py"], deps=["t0"])
        job = spec.to_job("proj")
        self.assertEqual(job.id, "t1")
        self.assertEqual(job.project, "proj")
        self.assertEqual(job.write, ["src/*.py"])
        self.assertEqual(job.deps, ["t0"])


def spec(tid, deps=None, write=None):
    return T.TaskSpec(tid, deps=deps or [], write=write or [])


class TestPlanValidate(unittest.TestCase):
    def test_empty_rejected(self):
        with self.assertRaises(T.PlanError):
            T.Plan("p", []).validate()

    def test_duplicate_id(self):
        with self.assertRaises(T.PlanError) as ctx:
            T.Plan("p", [spec("a"), spec("a")]).validate()
        self.assertIn("重复", str(ctx.exception))

    def test_self_dependency(self):
        with self.assertRaises(T.PlanError):
            T.Plan("p", [spec("a", deps=["a"])]).validate()

    def test_dangling_dependency(self):
        with self.assertRaises(T.PlanError) as ctx:
            T.Plan("p", [spec("a", deps=["ghost"])]).validate()
        self.assertIn("ghost", str(ctx.exception))

    def test_cycle_detected(self):
        with self.assertRaises(T.PlanError) as ctx:
            T.Plan("p", [spec("a", deps=["b"]), spec("b", deps=["a"])]).validate()
        self.assertIn("环", str(ctx.exception))

    def test_valid_passes(self):
        plan = T.Plan("p", [spec("a"), spec("b", deps=["a"])]).validate()
        self.assertEqual(len(plan), 2)

    def test_from_dict_validates(self):
        with self.assertRaises(T.PlanError):
            T.Plan.from_dict({"project": "p", "tasks": [{"id": "a", "deps": ["x"]}]})
        with self.assertRaises(T.PlanError):
            T.Plan.from_dict({"tasks": [{"id": "a"}]})


class TestPlanOrdering(unittest.TestCase):
    def _plan(self):
        return T.Plan("p", [
            spec("c", deps=["a", "b"]),
            spec("a"),
            spec("b", deps=["a"]),
        ])

    def test_topo_order(self):
        order = [t.id for t in self._plan().topo_order()]
        self.assertLess(order.index("a"), order.index("b"))
        self.assertLess(order.index("b"), order.index("c"))
        self.assertEqual(len(order), 3)

    def test_waves(self):
        waves = [[t.id for t in w] for w in self._plan().waves()]
        self.assertEqual(waves[0], ["a"])
        self.assertEqual(waves[1], ["b"])
        self.assertEqual(waves[2], ["c"])

    def test_parallel_same_wave(self):
        plan = T.Plan("p", [spec("root"), spec("x", deps=["root"]), spec("y", deps=["root"])])
        waves = [[t.id for t in w] for w in plan.waves()]
        self.assertEqual(waves[0], ["root"])
        self.assertEqual(sorted(waves[1]), ["x", "y"])

    def test_to_jobs_in_topo_order(self):
        jobs = self._plan().to_jobs()
        self.assertEqual([j.id for j in jobs][0], "a")
        self.assertTrue(all(j.project == "p" for j in jobs))


class TestConflicts(unittest.TestCase):
    def test_detects_unsynchronised_writers(self):
        plan = T.Plan("p", [
            spec("a", write=["src/*.py"]),
            spec("b", write=["src/main.py"]),
        ]).validate()
        found = plan.conflicts()
        self.assertEqual(len(found), 1)
        self.assertEqual({found[0][0], found[0][1]}, {"a", "b"})

    def test_no_conflict_when_ordered(self):
        plan = T.Plan("p", [
            spec("a", write=["src/main.py"]),
            spec("b", deps=["a"], write=["src/main.py"]),
        ]).validate()
        self.assertEqual(plan.conflicts(), [], "有依赖顺序就不算冲突")

    def test_no_conflict_when_disjoint(self):
        plan = T.Plan("p", [
            spec("a", write=["src/*.py"]),
            spec("b", write=["docs/*.md"]),
        ]).validate()
        self.assertEqual(plan.conflicts(), [])

    def test_stats(self):
        plan = T.Plan("p", [
            spec("a", write=["src/*.py"]),
            spec("b", write=["src/x.py"]),
        ]).validate()
        s = plan.stats()
        self.assertEqual(s["tasks"], 2)
        self.assertEqual(s["edges"], 0)
        self.assertEqual(s["conflicts"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
