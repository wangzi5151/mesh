#!/usr/bin/env python3
"""端到端演示：两个节点 + 一个控制面 + 一份 3 条任务的 DAG，跑完打印结果。

    python3 tools/demo.py

第 1 个节点干活，产物按增量收回来，再**推给第 2 个节点** —— 这就是
"在 A 上改完，B 上也自动有了"。不联网、不要 ip、不要第三方包，全在一条进程里跑。

真实用法见 README（节点跑在另一台机器上，``mesh node`` / ``mesh hub``）。
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import agent, hub  # noqa: E402


def peek(path, limit=24):
    """文件内容的一小段：够看出对不对，又不至于把终端刷满。"""
    with open(path, "r", encoding="utf-8") as f:
        body = f.read()
    text = body.replace("\n", "\\n")
    if len(text) > limit:
        text = text[:limit] + "…"
    return "%6d B  %r" % (os.path.getsize(path), text)


def main():
    root = tempfile.mkdtemp(prefix="mesh-demo-")
    work = os.path.join(root, "手机")
    peer = os.path.join(root, "镜像")
    os.makedirs(work)
    os.makedirs(peer)

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

    node = agent.Node("手机", roots=[{"name": "work", "path": work, "mode": "rw"}],
                      slots_total=2, tags=["demo"], runner=runner)
    server = node.build_server(port=0)
    server.start()

    # 第二个节点：不干活，只当"另一台机器"，让产物自己飘过去
    mirror_node = agent.Node("笔记本", roots=[{"name": "work", "path": peer, "mode": "rw"}],
                             runner=lambda job: {"stdout": "不干活"})
    mirror_server = mirror_node.build_server(port=0)
    mirror_server.start()

    plane = hub.Hub(":memory:", poll=0.05, mirror=os.path.join(root, "产物"))
    plane.add_node("手机", server.url)
    plane.add_node("笔记本", mirror_server.url)
    ids = plane.submit({
        "project": "demo",
        "tasks": [
            # 第一段写长一点：t2 只是往后面追一行，推给第二台机器时才看得出差分
            {"id": "t1", "title": "打地基", "prompt": "第一行\n" * 2400,
             "write": ["a.txt"], "req": {"tags": ["demo"]},
             "sync": ["mesh://笔记本/work"]},
            {"id": "t2", "title": "接上去", "prompt": "第二行\n",
             "write": ["a.txt"], "deps": ["t1"], "req": {"tags": ["demo"]},
             "sync": ["mesh://笔记本/work"]},
            {"id": "t3", "title": "另一条线", "prompt": "自己一份\n",
             "write": ["b.txt"], "req": {"tags": ["demo"]},
             "sync": ["mesh://笔记本/work"]},
        ],
    })

    print("节点   手机 %s（干活）" % server.url)
    print("节点   笔记本 %s（收产物）" % mirror_server.url)
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
                print("  手机   %-6s %s" % (name, peek(path)))
        for name in sorted(os.listdir(peer)):
            path = os.path.join(peer, name)
            if os.path.isfile(path):
                print("  笔记本 %-6s %s" % (name, peek(path)))
        print()
        for job in plane.jobs(project="demo"):
            sync = (job.result or {}).get("sync") or {}
            hop = ""
            if sync.get("targets"):
                modes = [f["mode"] for t in sync["targets"] for f in t["files"]]
                hop = "  分发 %s（%d B / %d B）" % (
                    "/".join(sorted(set(modes))) or "-", sync.get("sent", 0),
                    sync.get("size", 0))
            print("  %-4s %-7s 尝试 %d  %s%s"
                  % (job.id, job.state, job.attempts, job.feedback, hop))
    finally:
        server.stop()
        mirror_server.stop()
        plane.close()
        shutil.rmtree(root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
