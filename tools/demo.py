#!/usr/bin/env python3
"""端到端演示：一个节点 + 一个控制面 + 一份 3 条任务的 DAG，跑完打印结果。

    python3 tools/demo.py

不联网、不要 ip、不要第三方包 —— 全在一条进程里跑，用来确认"调度这条路通了"。
真实用法见 README（节点跑在另一台机器上，``mesh node`` / ``mesh hub``）。
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import agent, hub  # noqa: E402


def main():
    work = tempfile.mkdtemp(prefix="mesh-demo-")

    def runner(job):
        """假装是个 agent：把提示词写进第一个写作用域文件；有依赖就接在上游产物后面。"""
        target = os.path.join(work, job.write[0])
        body = job.prompt
        if job.deps and os.path.exists(target):
            with open(target, "r", encoding="utf-8") as f:
                body = f.read() + body
        with open(target, "w", encoding="utf-8") as f:
            f.write(body)
        return {"stdout": "写入 %s（%d 字节）" % (job.write[0], len(body))}

    node = agent.Node("demo", roots=[{"name": "work", "path": work, "mode": "rw"}],
                      slots_total=2, tags=["demo"], runner=runner)
    server = node.build_server(port=0)
    server.start()

    plane = hub.Hub(":memory:", poll=0.05)
    plane.add_node("demo", server.url)
    ids = plane.submit({
        "project": "demo",
        "tasks": [
            {"id": "t1", "title": "打地基", "prompt": "第一行\n",
             "write": ["a.txt"]},
            {"id": "t2", "title": "接上去", "prompt": "第二行\n",
             "write": ["a.txt"], "deps": ["t1"]},
            {"id": "t3", "title": "另一条线", "prompt": "自己一份\n",
             "write": ["b.txt"]},
        ],
    })

    print("节点   %s  %s" % (node.node_id, server.url))
    print("提交   %d 条作业：%s" % (len(ids), " ".join(ids)))
    print()

    try:
        for round_no in range(1, 21):
            report = plane.tick()
            print("第 %d 轮  完成 %-2d 失败 %-2d 退回 %-2d 等待 %-2d"
                  % (round_no, len(report["completed"]), len(report["failed"]),
                     len(report["unleased"]), len(report["blocked"])))
            if not (plane.queue.stats().get("queued") or plane.queue.stats().get("running")):
                break

        print()
        stats = plane.queue.stats()
        print("结果   %s" % ", ".join("%s %d" % (k, v) for k, v in sorted(stats.items())))
        for name in sorted(os.listdir(work)):
            path = os.path.join(work, name)
            if os.path.isfile(path):
                with open(path, "r", encoding="utf-8") as f:
                    print("  %-8s %r" % (name, f.read()))
        print()
        for job in plane.jobs(project="demo"):
            print("  %-4s %-7s 尝试 %d  %s"
                  % (job.id, job.state, job.attempts, job.feedback))
    finally:
        server.stop()
        plane.close()
        shutil.rmtree(work, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
