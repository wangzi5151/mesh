"""capability.py 的测试：本机探测、调度打分、注册表增删改查与持久化。"""

from __future__ import annotations

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import capability as C  # noqa: E402


def cap(node_id, **kw):
    kw.setdefault("last_seen", time.time())
    return C.Capability(node_id, **kw)


class TestProbe(unittest.TestCase):
    def test_local_probe_has_sane_fields(self):
        c = C.probe("n1", name="测试机", tags=["local"], slots_total=2)
        self.assertEqual(c.node_id, "n1")
        self.assertEqual(c.name, "测试机")
        self.assertEqual(c.tags, ["local"])
        self.assertGreaterEqual(c.cpu_count, 1)
        self.assertTrue(c.os_name)
        self.assertTrue(c.arch)
        self.assertTrue(c.python)
        self.assertGreater(c.last_seen, 0)
        self.assertEqual(c.slots_total, 2)

    def test_probe_marks_root_existence(self):
        c = C.probe("n1", roots=[
            {"name": "here", "path": os.getcwd(), "mode": "rw"},
            {"name": "gone", "path": "/definitely/not/here", "mode": "ro"},
        ], with_versions=False)
        self.assertTrue(c.root("here")["exists"])
        self.assertFalse(c.root("gone")["exists"])
        self.assertEqual(c.root_names(), ["here", "gone"])

    def test_probe_tools_only_lists_installed(self):
        tools = C.probe_tools(["python3", "no-such-tool-xyz"], with_versions=False)
        self.assertIn("python3", tools)
        self.assertNotIn("no-such-tool-xyz", tools)

    def test_tool_version_missing_returns_none(self):
        self.assertIsNone(C.tool_version("no-such-tool-xyz"))

    def test_tool_version_present_returns_string(self):
        v = C.tool_version("python3")
        self.assertIsInstance(v, str)
        self.assertTrue(v)

    def test_meminfo_readable_on_linux(self):
        info = C._meminfo()
        if os.path.exists("/proc/meminfo"):
            self.assertGreater(info.get("MemTotal", 0), 0)


class TestScore(unittest.TestCase):
    def test_stale_node_rejected(self):
        c = cap("n1", last_seen=time.time() - 1000)
        ok, s, why = C.score(c)
        self.assertFalse(ok)
        self.assertEqual(s, 0.0)
        self.assertIn("失联", why[0])

    def test_healthy_node_passes(self):
        ok, s, why = C.score(cap("n1", slots_total=2, cpu_count=4))
        self.assertTrue(ok)
        self.assertGreater(s, 0)
        self.assertEqual(why, [])

    def test_missing_tags(self):
        ok, _s, why = C.score(cap("n1", tags=["a"]), {"tags": ["a", "b"]})
        self.assertFalse(ok)
        self.assertIn("b", why[0])

    def test_min_cores(self):
        ok, _s, why = C.score(cap("n1", cpu_count=2), {"min_cores": 8})
        self.assertFalse(ok)
        self.assertIn("CPU", why[0])

    def test_missing_tools(self):
        ok, _s, why = C.score(cap("n1", tools={"git": "2.0"}), {"tools": ["git", "aider"]})
        self.assertFalse(ok)
        self.assertIn("aider", why[0])

    def test_agent_kind(self):
        ok, _s, why = C.score(cap("n1", agents=["mock"]), {"agent": "cli"})
        self.assertFalse(ok)
        self.assertIn("cli", why[0])

    def test_missing_root(self):
        ok, _s, why = C.score(cap("n1", roots=[{"name": "a"}]), {"root": "b"})
        self.assertFalse(ok)
        self.assertIn("b", why[0])

    def test_memory_and_disk(self):
        c = cap("n1", mem_total=1000, mem_avail=10, disk_free=10)
        self.assertFalse(C.score(c, {"min_mem_mb": 100})[0])
        self.assertFalse(C.score(c, {"min_disk_mb": 100})[0])

    def test_unknown_memory_is_not_a_rejection(self):
        ok, _s, _why = C.score(cap("n1", mem_total=0, mem_avail=0), {"min_mem_mb": 100})
        self.assertTrue(ok, "内存未知时不应误判为不合格")

    def test_more_free_slots_scores_higher(self):
        busy = cap("n1", slots_total=4, slots_used=3, cpu_count=4)
        idle = cap("n2", slots_total=4, slots_used=0, cpu_count=4)
        self.assertGreater(C.score(idle)[1], C.score(busy)[1])


class TestCapability(unittest.TestCase):
    def test_derived_values(self):
        c = cap("n1", slots_total=3, slots_used=1, loadavg=[1.5, 1.0, 0.5],
                roots=[{"name": "a"}, {"name": "b"}], tools={"git": "2"})
        self.assertEqual(c.slots_free, 2)
        self.assertEqual(c.load1, 1.5)
        self.assertEqual(c.root_names(), ["a", "b"])
        self.assertIsNone(c.root("zzz"))
        self.assertTrue(c.has_tools(["git"]))
        self.assertFalse(c.has_tools(["git", "nope"]))

    def test_age_and_stale(self):
        now = 10000.0
        c = cap("n1", last_seen=now - 50)
        self.assertAlmostEqual(c.age(now), 50.0)
        self.assertTrue(c.is_stale(ttl=30, now=now))
        self.assertFalse(c.is_stale(ttl=90, now=now))

    def test_dict_round_trip(self):
        c = C.probe("n1", tags=["x"], roots=[{"name": "r", "path": os.getcwd(), "mode": "rw"}],
                    with_versions=False)
        back = C.Capability.from_dict(c.to_dict())
        self.assertEqual(back.to_dict(), c.to_dict())

    def test_missing_node_id_rejected(self):
        with self.assertRaises(ValueError):
            C.Capability.from_dict({"name": "x"})


class TestRegistry(unittest.TestCase):
    def setUp(self):
        self.reg = C.Registry(ttl=60)

    def test_register_get_list(self):
        self.reg.register(cap("n1"))
        self.reg.register(cap("n2"))
        self.assertEqual(len(self.reg), 2)
        self.assertIn("n1", self.reg)
        self.assertIsNotNone(self.reg.get("n1"))
        self.assertEqual([c.node_id for c in self.reg.list()], ["n1", "n2"])

    def test_register_accepts_dict(self):
        self.reg.register(C.probe("n1", with_versions=False).to_dict())
        self.assertIn("n1", self.reg)

    def test_register_rejects_empty_id(self):
        with self.assertRaises(ValueError):
            self.reg.register(C.Capability(""))

    def test_remove(self):
        self.reg.register(cap("n1"))
        self.assertTrue(self.reg.remove("n1"))
        self.assertFalse(self.reg.remove("n1"))
        self.assertEqual(len(self.reg), 0)

    def test_update_partial(self):
        self.reg.register(cap("n1", slots_used=0))
        self.reg.update("n1", slots_used=2, tags=["busy"])
        c = self.reg.get("n1")
        self.assertEqual(c.slots_used, 2)
        self.assertEqual(c.tags, ["busy"])

    def test_update_unknown_node(self):
        with self.assertRaises(KeyError):
            self.reg.update("nope", slots_used=1)

    def test_touch_refreshes_last_seen(self):
        self.reg.register(cap("n1", last_seen=1.0))
        self.reg.touch("n1", now=500.0)
        self.assertEqual(self.reg.get("n1").last_seen, 500.0)

    def test_stale_and_prune(self):
        now = 10000.0
        self.reg.register(cap("old", last_seen=now - 500))
        self.reg.register(cap("new", last_seen=now - 1))
        self.assertEqual([c.node_id for c in self.reg.stale(now)], ["old"])
        self.assertEqual(self.reg.prune(now), ["old"])
        self.assertEqual([c.node_id for c in self.reg.list(now=now)], ["new"])

    def test_list_hides_stale_by_default(self):
        self.reg.register(cap("old", last_seen=time.time() - 500))
        self.reg.register(cap("new"))
        self.assertEqual([c.node_id for c in self.reg.list()], ["new"])
        self.assertEqual(len(self.reg.list(include_stale=True)), 2)

    def test_candidates_skips_full_nodes(self):
        self.reg.register(cap("full", slots_total=1, slots_used=1))
        self.reg.register(cap("free", slots_total=1, slots_used=0))
        self.assertEqual([c.node_id for c, _s in self.reg.candidates()], ["free"])

    def test_candidates_respects_requirements(self):
        self.reg.register(cap("nogit", slots_total=2, tools={}))
        self.reg.register(cap("hasgit", slots_total=2, tools={"git": "2"}))
        picked = self.reg.pick({"tools": ["git"]})
        self.assertEqual(picked.node_id, "hasgit")

    def test_pick_prefers_idle(self):
        self.reg.register(cap("busy", slots_total=4, slots_used=3, cpu_count=4))
        self.reg.register(cap("idle", slots_total=4, slots_used=0, cpu_count=4))
        self.assertEqual(self.reg.pick().node_id, "idle")

    def test_pick_returns_none_when_nothing_fits(self):
        self.reg.register(cap("n1", tags=["x"]))
        self.assertIsNone(self.reg.pick({"tags": ["y"]}))

    def test_explain_is_readable(self):
        self.reg.register(cap("full", slots_total=1, slots_used=1))
        self.reg.register(cap("nogit", slots_total=2, tools={}))
        lines = self.reg.explain({"tools": ["git"]})
        joined = "\n".join(lines)
        self.assertIn("1/1", joined)
        self.assertIn("git", joined)

    def test_registry_dict_round_trip(self):
        self.reg.register(cap("n1", tags=["a"]))
        self.reg.register(cap("n2", tags=["b"]))
        back = C.Registry.from_dict(self.reg.to_dict())
        self.assertEqual(len(back), 2)
        self.assertEqual(back.get("n1").tags, ["a"])
        self.assertEqual(back.ttl, self.reg.ttl)


if __name__ == "__main__":
    unittest.main(verbosity=2)
