"""cli.py 的测试：参数解析、表格排版、各子命令的成败路径。

命令行是给人用的，所以这里不打桩到内部函数，尽量走 ``cli.main([...])`` ——
退出码和屏幕上写的东西就是它的全部契约。
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import cli  # noqa: E402
from mesh import hub as H  # noqa: E402
from mesh import queue as Q  # noqa: E402


def run(argv):
    """跑一条命令，返回 (退出码, stdout, stderr)。"""
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = cli.main(argv)
    return code, out.getvalue(), err.getvalue()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-cli-")
        self.db = os.path.join(self.tmp, "mesh.db")
        self.work = os.path.join(self.tmp, "work")
        os.makedirs(self.work)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name, obj):
        path = os.path.join(self.tmp, name)
        with open(path, "w", encoding="utf-8") as f:
            if isinstance(obj, str):
                f.write(obj)
            else:
                json.dump(obj, f, ensure_ascii=False)
        return path

    def plan_file(self, tasks=None, project="demo"):
        return self.write("plan.json", {
            "project": project,
            "tasks": tasks or [{"id": "t1", "title": "第一件事", "cmd": "true",
                                "write": ["a.txt"]}],
        })


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------
class TestHelpers(unittest.TestCase):
    def test_split_list(self):
        self.assertEqual(cli.split_list("a, b ,c"), ["a", "b", "c"])
        self.assertEqual(cli.split_list(""), [])
        self.assertEqual(cli.split_list(None), [])
        self.assertEqual(cli.split_list(["x", "", "y"]), ["x", "y"])

    def test_parse_root_rw(self):
        got = cli.parse_root("work=/tmp/x")
        self.assertEqual((got["name"], got["path"], got["mode"]), ("work", "/tmp/x", "rw"))

    def test_parse_root_readonly(self):
        got = cli.parse_root("data=/srv/data:ro")
        self.assertEqual((got["name"], got["path"], got["mode"]), ("data", "/srv/data", "ro"))

    def test_parse_root_expands_home(self):
        self.assertTrue(cli.parse_root("h=~/proj")["path"].startswith(os.path.expanduser("~")))

    def test_parse_root_rejects_garbage(self):
        with self.assertRaises(cli.CliError):
            cli.parse_root("/tmp/x")
        with self.assertRaises(cli.CliError):
            cli.parse_root("=  /tmp/x")

    def test_width_counts_cjk_as_two(self):
        self.assertEqual(cli.width("abc"), 3)
        self.assertEqual(cli.width("中文"), 4)
        self.assertEqual(cli.width("a中"), 3)

    def test_pad_aligns_cjk(self):
        self.assertEqual(cli.width(cli.pad("中", 6)), 6)

    def test_table_columns_line_up(self):
        text = cli.table([["中文", "1"], ["ab", "22"]], ["名", "数"])
        lines = text.splitlines()
        self.assertIn("名", lines[0])
        # 第二列的**显示**起点必须一致：CJK 按两格算才对得齐
        def display_col_start(line, needle):
            return cli.width(line[:line.index(needle)])
        self.assertEqual(display_col_start(lines[2], "1"), display_col_start(lines[3], "22"))

    def test_table_empty(self):
        self.assertEqual(cli.table([]), "")


class TestSummaryLine(unittest.TestCase):
    def test_empty_round_is_silent(self):
        line, state = cli._summary_line({"nodes": {"live": [], "dead": []}})
        self.assertEqual(line, "")
        self.assertEqual(state["live"], set())
        self.assertEqual(state["dead"], set())

    def test_stuck_jobs_reported_once(self):
        report = {"nodes": {"live": ["n1"], "dead": []},
                  "blocked": [{"job": "a", "why": []}, {"job": "b", "why": []}]}
        line, state = cli._summary_line(report)
        self.assertIn("等待 2", line)
        again, _state = cli._summary_line(report, state)
        self.assertEqual(again, "", "同一批卡住的作业不该每轮刷一遍")

    def test_node_state_reported_once(self):
        report = {"nodes": {"live": ["n1"], "dead": []}}
        line, state = cli._summary_line(report)
        self.assertIn("节点 1 在线", line)

        again, _state = cli._summary_line(report, state)
        self.assertEqual(again, "", "节点状态没变就不该再打一遍")

    def test_node_going_down_is_reported_again(self):
        report = {"nodes": {"live": ["n1"], "dead": []}}
        _line, state = cli._summary_line(report)
        gone = {"nodes": {"live": [], "dead": ["n1"]}}
        line, _state = cli._summary_line(gone, state)
        self.assertIn("节点 0 在线/1 掉线", line)

    def test_job_events_always_reported(self):
        report = {"nodes": {"live": ["n1"], "dead": []},
                  "completed": ["a"], "failed": [{"job": "b"}]}
        line, state = cli._summary_line(report)
        self.assertIn("完成 1", line)
        self.assertIn("失败 1", line)
        again, _state = cli._summary_line(report, state)
        self.assertIn("完成 1", again, "作业事件每轮都是新的，不受节点去重影响")


# --------------------------------------------------------------------------
# 读文件
# --------------------------------------------------------------------------
class TestLoaders(Base):
    def test_load_plan_ok(self):
        plan = cli.load_plan(self.plan_file())
        self.assertEqual(plan.project, "demo")
        self.assertEqual(len(plan), 1)

    def test_load_plan_bad_json(self):
        path = self.write("bad.json", "{not json")
        with self.assertRaises(cli.CliError) as ctx:
            cli.load_plan(path)
        self.assertIn("JSON", str(ctx.exception))

    def test_load_plan_missing_file(self):
        with self.assertRaises(cli.CliError) as ctx:
            cli.load_plan(os.path.join(self.tmp, "nope.json"))
        self.assertIn("读不了", str(ctx.exception))

    def test_load_plan_invalid_dag(self):
        path = self.write("bad.json", {"project": "p", "tasks": [{"id": "a", "deps": ["zz"]}]})
        with self.assertRaises(cli.CliError) as ctx:
            cli.load_plan(path)
        self.assertIn("计划不合法", str(ctx.exception))

    def test_load_nodes_list_form(self):
        path = self.write("n.json", [{"id": "n1", "url": "http://h:1", "tags": "a,b"}])
        nodes = cli.load_nodes(path)
        self.assertEqual(nodes[0]["node_id"], "n1")
        self.assertEqual(nodes[0]["tags"], ["a", "b"])
        self.assertEqual(nodes[0]["weight"], 1.0)

    def test_load_nodes_wrapped_form_with_node_id_key(self):
        path = self.write("n.json", {"nodes": [{"node_id": "n1", "url": "http://h:1"}]})
        self.assertEqual(cli.load_nodes(path)[0]["node_id"], "n1")

    def test_load_nodes_missing_url(self):
        path = self.write("n.json", [{"id": "n1"}])
        with self.assertRaises(cli.CliError) as ctx:
            cli.load_nodes(path)
        self.assertIn("缺 id 或 url", str(ctx.exception))

    def test_load_nodes_not_a_list(self):
        path = self.write("n.json", {"nodes": "算了"})
        with self.assertRaises(cli.CliError):
            cli.load_nodes(path)


# --------------------------------------------------------------------------
# 子命令
# --------------------------------------------------------------------------
class TestCommands(Base):
    def test_no_args_prints_help(self):
        code, out, _err = run([])
        self.assertEqual(code, 0)
        self.assertIn("usage: mesh", out)

    def test_version_exits_zero(self):
        with self.assertRaises(SystemExit) as ctx:
            run(["--version"])
        self.assertEqual(ctx.exception.code, 0)

    def test_plan_preview(self):
        path = self.plan_file([
            {"id": "b", "deps": ["a"], "write": ["b.txt"]},
            {"id": "a", "write": ["a.txt"]},
        ])
        code, out, _err = run(["plan", path])
        self.assertEqual(code, 0)
        self.assertIn("2 个并行批次", out)
        self.assertIn("第 1 批  a", out)
        self.assertIn("没有冲突", out)

    def test_plan_reports_conflicts(self):
        path = self.plan_file([
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"]},
        ])
        code, out, _err = run(["plan", path])
        self.assertEqual(code, 0)
        self.assertIn("写冲突 1 处", out)

    def test_submit_then_status_and_jobs(self):
        code, out, _err = run(["submit", self.plan_file(), "--db", self.db])
        self.assertEqual(code, 0)
        self.assertIn("已提交 1 条作业", out)

        code, out, _err = run(["status", "--db", self.db])
        self.assertEqual(code, 0)
        self.assertIn("queued 1", out)
        self.assertIn("t1", out)
        self.assertIn("第一件事", out)

        code, out, _err = run(["jobs", "--db", self.db, "--json"])
        payload = json.loads(out)
        self.assertEqual(payload[0]["id"], "t1")

    def test_submit_rejects_conflict(self):
        path = self.plan_file([
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"]},
        ])
        code, _out, err = run(["submit", path, "--db", self.db])
        self.assertEqual(code, 2)
        self.assertIn("写冲突", err)
        self.assertFalse(os.path.exists(self.db), "被拒绝的计划不该留下队列文件")

    def test_submit_force(self):
        path = self.plan_file([
            {"id": "a", "write": ["src/*.py"]},
            {"id": "b", "write": ["src/main.py"]},
        ])
        code, out, _err = run(["submit", path, "--db", self.db, "--force"])
        self.assertEqual(code, 0)
        self.assertIn("已提交 2 条作业", out)

    def test_status_without_db(self):
        code, _out, err = run(["status", "--db", os.path.join(self.tmp, "none.db")])
        self.assertEqual(code, 2)
        self.assertIn("找不到队列文件", err)

    def test_status_empty_queue(self):
        queue = Q.Queue(self.db)
        queue.close()
        code, out, _err = run(["status", "--db", self.db])
        self.assertEqual(code, 0)
        self.assertIn("空", out)

    def test_jobs_show(self):
        run(["submit", self.plan_file(), "--db", self.db])
        code, out, _err = run(["jobs", "--db", self.db, "--show", "t1"])
        self.assertEqual(code, 0)
        self.assertIn('"id": "t1"', out)

    def test_jobs_show_unknown(self):
        run(["submit", self.plan_file(), "--db", self.db])
        code, _out, err = run(["jobs", "--db", self.db, "--show", "zz"])
        self.assertEqual(code, 2)
        self.assertIn("没有作业 zz", err)

    def test_retry_and_cancel(self):
        run(["submit", self.plan_file(), "--db", self.db])
        queue = Q.Queue(self.db)
        queue.cancel("t1")
        queue.close()

        code, out, _err = run(["retry", "t1", "--db", self.db, "--reset"])
        self.assertEqual(code, 2, "canceled 作业不该能直接 retry")

        queue = Q.Queue(self.db)
        queue.add(Q.Job("t2", "demo"))
        queue.close()
        code, out, _err = run(["cancel", "t2", "--db", self.db])
        self.assertEqual(code, 0)
        self.assertIn("已取消 t2", out)

    def test_retry_unknown_job(self):
        Q.Queue(self.db).close()
        code, _out, err = run(["retry", "nope", "--db", self.db])
        self.assertEqual(code, 2)
        self.assertIn("作业不存在", err)

    def test_doctor_ok(self):
        code, out, err = run(["doctor", "--db", self.db, "--root", "work=" + self.work])
        self.assertIn(code, (0, 2))
        if code == 0:
            self.assertIn("没有发现问题", out)
        else:                                   # 环境缺些什么也别崩
            self.assertNotIn("Traceback", err)

    def test_doctor_missing_root(self):
        code, _out, err = run(["doctor", "--db", self.db,
                               "--root", "gone=" + os.path.join(self.tmp, "nope")])
        self.assertEqual(code, 2)
        self.assertIn("目录不存在", err)

    def test_node_needs_a_root(self):
        code, _out, err = run(["node", "--id", "n1"])
        self.assertEqual(code, 2)
        self.assertIn("至少给一个 --root", err)

    def test_node_exec_requires_token(self):
        code, _out, err = run(["node", "--id", "n1", "--root", "w=" + self.work, "--exec"])
        self.assertEqual(code, 2)
        self.assertIn("必须给 --token", err)

    def test_hub_once(self):
        code, out, _err = run(["hub", "--db", self.db, "--once",
                               "--node", "n1=http://127.0.0.1:1"])
        self.assertEqual(code, 0)
        report = json.loads(out[out.index("{"):])
        self.assertEqual(report["nodes"]["dead"], ["n1"])

    def test_hub_bad_node_spec(self):
        code, _out, err = run(["hub", "--db", self.db, "--once", "--node", "没等号"])
        self.assertEqual(code, 2)
        self.assertIn("id=url", err)

    def test_hub_bad_interval(self):
        code, _out, err = run(["hub", "--db", self.db, "--interval", "0"])
        self.assertEqual(code, 2)
        self.assertIn("--interval", err)

    def test_hub_nodes_file(self):
        path = self.write("nodes.json", {"nodes": [
            {"id": "n1", "url": "http://127.0.0.1:1"}]})
        code, out, _err = run(["hub", "--db", self.db, "--once", "--nodes-file", path])
        self.assertEqual(code, 0)
        self.assertIn("接入节点 n1", out)


# --------------------------------------------------------------------------
# 对着真控制面看状态
# --------------------------------------------------------------------------
class TestStatusFromHub(Base):
    def setUp(self):
        super(TestStatusFromHub, self).setUp()
        self.hub = H.Hub(self.db, token="tok", poll=0.01)
        self.hub.add_node("n1", "http://127.0.0.1:1")      # 掉线，但能列出
        self.hub.submit({"project": "p", "tasks": [{"id": "a", "title": "活儿"}]})
        self.server = self.hub.build_server(port=0)
        self.server.start()

    def tearDown(self):
        self.server.stop()
        self.hub.close()
        super(TestStatusFromHub, self).tearDown()

    def test_status_from_hub(self):
        code, out, _err = run(["status", "--hub", self.server.url, "--hub-token", "tok"])
        self.assertEqual(code, 0)
        self.assertIn("n1", out)
        self.assertIn("离线", out)
        self.assertIn("queued 1", out)

    def test_status_from_hub_wrong_token(self):
        code, _out, err = run(["status", "--hub", self.server.url, "--hub-token", "bad"])
        self.assertEqual(code, 2)
        self.assertIn("连不上控制面", err)

    def test_status_from_hub_unreachable(self):
        code, _out, err = run(["status", "--hub", "http://127.0.0.1:1"])
        self.assertEqual(code, 2)
        self.assertIn("连不上控制面", err)


if __name__ == "__main__":
    unittest.main(verbosity=2)
