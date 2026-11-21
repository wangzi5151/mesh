"""mesh 命令行。

设计上只有一条原则：**队列文件就是接口**。``mesh hub`` 跑调度，``mesh submit`` /
``mesh status`` / ``mesh jobs`` 在别的进程、别的机器（挂同一个目录）上操作同一份
SQLite 队列，不需要额外服务，也不需要账号。

    mesh node   --id 手机 --root work=~/proj --port 8765
    mesh hub    --db mesh.db --nodes nodes.json --port 8788
    mesh submit plan.json --db mesh.db
    mesh status --db mesh.db

零第三方依赖（只用 argparse），这样才能在 Termux / proot 里直接跑。
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import unicodedata

from . import __version__
from . import agent, auth as auth_mod, capability, hub as hub_mod
from . import queue as queue_mod, task, wire

EXIT_OK = 0
EXIT_USAGE = 2


class CliError(Exception):
    """面向用户的错误：打印一行人话，不吐栈。"""


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------
def _out(text=""):
    # flush：`mesh hub` 是被重定向到日志里跑的，不冲就什么都看不到
    sys.stdout.write(text + "\n")
    sys.stdout.flush()


def _err(text):
    sys.stderr.write("mesh: " + text + "\n")
    sys.stderr.flush()


def width(text):
    """终端显示宽度：CJK 算两格，否则表格永远对不齐。"""
    total = 0
    for ch in str(text):
        total += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return total


def pad(text, size):
    text = str(text)
    return text + " " * max(0, size - width(text))


def table(rows, headers=None):
    """把二维列表排版成等宽表格（对 CJK 也成立）。"""
    rows = [[("" if c is None else str(c)) for c in row] for row in rows]
    if headers:
        rows = [[str(h) for h in headers]] + rows
    if not rows:
        return ""
    cols = max(len(r) for r in rows)
    widths = [0] * cols
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], width(cell))
    lines = []
    for index, row in enumerate(rows):
        line = "  ".join(pad(cell, widths[i]) for i, cell in enumerate(row)).rstrip()
        lines.append(line)
        if headers and index == 0:
            lines.append("-" * min(len(line), 120))
    return "\n".join(lines)


def split_list(text):
    """``"a, b ,c"`` → ``["a", "b", "c"]``，空项丢掉。"""
    if not text:
        return []
    if isinstance(text, (list, tuple)):
        items = list(text)
    else:
        items = str(text).replace(",", " ").split()
    return [i for i in items if i]


def parse_root(text):
    """``name=path`` 或 ``name=path:ro`` → Node.add_root 的参数。"""
    if "=" not in text:
        raise CliError("--root 要写成 name=path 或 name=path:ro，收到 %r" % text)
    name, path = text.split("=", 1)
    name, path = name.strip(), path.strip()
    if not name or not path:
        raise CliError("--root 的 name 和 path 都不能为空：%r" % text)
    mode = "rw"
    for suffix in (":ro", ":rw"):
        if path.endswith(suffix):
            path, mode = path[: -len(suffix)], suffix[1:]
            break
    return {"name": name, "path": os.path.expanduser(path), "mode": mode}


def load_json_file(path, what):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except OSError as exc:
        raise CliError("读不了%s %s：%s" % (what, path, exc))
    except ValueError as exc:
        raise CliError("%s %s 不是合法 JSON：%s" % (what, path, exc))


def load_plan(path):
    """读一份计划（``{"project": ..., "tasks": [...]}``）并校验。"""
    obj = load_json_file(path, "计划")
    try:
        return task.Plan.from_dict(obj)
    except task.PlanError as exc:
        raise CliError("计划不合法：%s" % exc)


def load_nodes(path):
    """读节点清单。接受 ``{"nodes": [...]}`` 或直接一个列表。"""
    obj = load_json_file(path, "节点清单")
    items = obj.get("nodes") if isinstance(obj, dict) else obj
    if not isinstance(items, list):
        raise CliError("节点清单要是个列表（或 {'nodes': [...]}）")
    out = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise CliError("第 %d 个节点不是对象" % (index + 1))
        node_id = item.get("id") or item.get("node_id") or ""
        url = item.get("url") or ""
        if not node_id or not url:
            raise CliError("第 %d 个节点缺 id 或 url" % (index + 1))
        out.append({
            "node_id": node_id, "url": url,
            "token": item.get("token") or "",
            "tags": split_list(item.get("tags")),
            "weight": float(item.get("weight") or 1.0),
        })
    return out


def open_queue(args, create=False):
    """直接开队列文件，不需要 hub 在跑。

    ``create=False`` 时文件必须已经存在 —— 只读命令打错路径时，给一行
    "找不到队列文件" 比悄悄建一个空库让人以为作业丢了要好。
    """
    path = args.db
    if not create and path != ":memory:" and not os.path.exists(path):
        raise CliError("找不到队列文件 %s（先跑一次 mesh hub --db %s）" % (path, path))
    return queue_mod.Queue(path)


def wait_for_signal(label):
    """阻塞到 Ctrl-C / SIGTERM，然后干净地收尾。"""
    stop = threading.Event()

    def handler(signum, frame):
        stop.set()

    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):        # 非主线程 / 平台不支持
                pass

    _out("%s（Ctrl-C 退出）" % label)
    try:
        while not stop.wait(0.5):
            pass
    except KeyboardInterrupt:
        pass
    return True


# --------------------------------------------------------------------------
# mesh node
# --------------------------------------------------------------------------
def build_node(args):
    roots = [parse_root(r) for r in (args.root or [])]
    if not roots:
        raise CliError("至少给一个 --root name=path（节点要有能看的目录）")
    if args.exec and not args.token:
        raise CliError("开了 --exec 就必须给 --token：能执行命令的节点不能裸奔")
    try:
        return agent.Node(
            args.id, roots=roots, token=args.token or "", tags=split_list(args.tags),
            slots_total=args.slots, models=split_list(args.models),
            agents=split_list(args.agents) or None,
            tool_names=split_list(args.tools) or None,
            exec_enabled=bool(args.exec), exec_allow_shell=bool(args.exec),
            rollback=not args.no_rollback,
            backup_max_bytes=int(args.backup_max_mb * (1 << 20)),
        )
    except agent.AgentError as exc:
        raise CliError(str(exc))


def _tls_from(args):
    """--tls-cert/--tls-key 要么都给，要么都不给。"""
    cert, key = getattr(args, "tls_cert", None), getattr(args, "tls_key", None)
    if bool(cert) != bool(key):
        raise CliError("--tls-cert 和 --tls-key 必须成对给")
    return (cert, key) if cert else None


def cmd_node(args):
    node = build_node(args)
    tls = _tls_from(args)
    srv = node.build_server(host=args.bind, port=args.port, tls=tls)
    srv.start()
    cap = node.capability()
    _out("节点 %s 已上线：%s" % (node.node_id, srv.url))
    _out("  名称    %s" % node.name)
    _out("  平台    %s %s / %s" % (cap.os_name, cap.os_release, cap.arch))
    _out("  CPU     %d 核，负载 %s" % (cap.cpu_count, cap.loadavg or "-"))
    _out("  槽位    %d" % cap.slots_total)
    _out("  终端    %s" % ("已开启" if cap.exec_enabled else "未开启（--exec 打开）"))
    _out("  回滚    %s" % ("开启（越界改动会被还原）" if node.rollback else "关闭"))
    _out("  链路    %s" % ("HTTPS" if tls else "HTTP（内网明文）"))
    _out("  令牌    %s" % ("已启用" if args.token else "无 —— 同网段谁能连谁能读"))
    for entry in node.roots_info():
        _out("  root    %-10s %-6s %s" % (entry["name"], entry["mode"], entry["path"]))
    try:
        wait_for_signal("节点在跑")
    finally:
        srv.stop()
        _out("节点已关闭")
    return EXIT_OK


# --------------------------------------------------------------------------
# mesh hub
# --------------------------------------------------------------------------
def cmd_hub(args):
    if args.interval <= 0:
        raise CliError("--interval 必须大于 0")
    tls = _tls_from(args)
    hub = hub_mod.Hub(
        args.db, token=args.token or "", poll=args.interval, ttl=args.ttl,
        dispatch_timeout=args.dispatch_timeout, lease=args.lease,
        mirror=args.mirror, ca=args.tls_ca, tls_insecure=args.tls_insecure)
    # 令牌从库里读；--token 额外算一把管理员口令
    hub.auth = auth_mod.Auth.from_store(
        hub.queue, admin_tokens=[args.token] if args.token else [])
    try:
        restored = hub.nodes()
        if restored:
            _out("从库里恢复 %d 个节点" % len(restored))
        for item in (load_nodes(args.nodes_file) if args.nodes_file else []):
            hub.add_node(item["node_id"], item["url"], item["token"],
                         item["tags"], item["weight"])
            _out("接入节点 %s → %s" % (item["node_id"], item["url"]))
        for text in (args.node or []):
            if "=" not in text:
                raise CliError("--node 要写成 id=url，收到 %r" % text)
            node_id, url = text.split("=", 1)
            hub.add_node(node_id.strip(), url.strip(), args.node_token or "")
            _out("接入节点 %s → %s" % (node_id.strip(), url.strip()))

        if args.mirror:
            _out("产物镜像：%s" % hub.mirror)
        if not hub.auth.enabled:
            _out("提示：没有配置任何令牌，控制面对同网段完全开放（mesh token add 可以加）")

        srv = None
        if args.port is not None:
            srv = hub.build_server(host=args.bind, port=args.port, tls=tls)
            srv.start()
            _out("控制面面板：%s/" % srv.url)
            if args.token:
                _out("  手机访问：%s/?token=%s" % (srv.url, args.token))

        _out("调度间隔 %.1fs，队列 %s" % (args.interval, os.path.abspath(args.db)))
        try:
            if args.once:
                report = hub.tick()
                _out(json.dumps(report, ensure_ascii=False, indent=2))
                return EXIT_OK
            _run_loop(hub, args.interval)
        finally:
            if srv is not None:
                srv.stop()
    finally:
        hub.close()
    return EXIT_OK


def _run_loop(hub, interval):
    stop = threading.Event()

    def handler(signum, frame):
        stop.set()

    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass

    _out("按 Ctrl-C 停止")
    prev = None
    standby_seen = ""
    try:
        while not stop.is_set():
            report = hub.tick()
            holder = report.get("standby")
            if holder:
                # 待命也要说出来：不然跑第二个控制面的人以为它坏了
                if holder != standby_seen:
                    _out("[%s] 待命：调度权在 %s 手上"
                         % (time.strftime("%H:%M:%S"), holder))
                    standby_seen = holder
                stop.wait(interval)
                continue
            if standby_seen:
                _out("[%s] 接手调度" % time.strftime("%H:%M:%S"))
                standby_seen = ""
            line, prev = _summary_line(report, prev)
            if line:
                _out(line)
            stop.wait(interval)
    except KeyboardInterrupt:
        pass
    _out("调度已停止")
    return True


def _summary_line(report, prev=None):
    """一轮调度只打一行；**节点状态没变化就不提**，空转绝不刷屏。

    返回 ``(这一行, 供下一轮对比的状态)``。
    """
    nodes = report.get("nodes") or {}
    live = set(nodes.get("live") or [])
    dead = set(nodes.get("dead") or [])
    state = {"live": live, "dead": dead}

    done = report.get("completed") or []
    failed = report.get("failed") or []
    unleased = report.get("unleased") or []
    blocked = report.get("blocked") or []
    reaped = report.get("reaped") or []

    # 卡住的作业每轮都会"等待"，只报一次；等它动起来（或者换了另一批）再说
    waiting = frozenset(d.get("job") for d in blocked)
    state["blocked"] = waiting

    parts = []
    if prev is None or prev.get("live") != live or prev.get("dead") != dead:
        if live or dead:
            parts.append("节点 %d 在线/%d 掉线" % (len(live), len(dead)))
    if waiting and (prev is None or prev.get("blocked") != waiting):
        parts.append("等待 %d" % len(waiting))
    if done:
        parts.append("完成 %d" % len(done))
    if failed:
        parts.append("失败 %d" % len(failed))
    if unleased:
        parts.append("退回 %d" % len(unleased))
    if reaped:
        parts.append("回收 %d" % len(reaped))
    # 分发失败不改作业状态，所以必须单独说出来 —— 否则它就静默地没送到
    sync_errors = report.get("sync_errors") or []
    if sync_errors:
        parts.append("分发失败 %d" % len(sync_errors))
    retried = report.get("retried") or []
    if retried:
        ok = [r for r in retried if r.get("state") == "done"]
        if ok:
            parts.append("补送到 %d" % len(ok))
        give_up = [r for r in retried if r.get("state") == "failed"]
        if give_up:
            parts.append("补送放弃 %d" % len(give_up))
    if not parts:
        return "", state
    return "[%s] %s" % (time.strftime("%H:%M:%S"), "，".join(parts)), state


# --------------------------------------------------------------------------
# 队列操作（离线，不需要 hub 在跑）
# --------------------------------------------------------------------------
def cmd_submit(args):
    plan = load_plan(args.plan)
    conflicts = plan.conflicts()
    if conflicts and not args.force:
        detail = "；".join("%s 与 %s 都写 %s" % (a, b, ga) for a, b, ga, _ in conflicts[:5])
        raise CliError("计划里有 %d 处未排序的写冲突：%s\n加依赖边，或者 --force 强行提交"
                       % (len(conflicts), detail))
    queue = open_queue(args, create=True)
    try:
        ids = []
        for job in plan.to_jobs():
            queue.add(job)
            ids.append(job.id)
    finally:
        queue.close()
    _out("已提交 %d 条作业（项目 %s）：%s" % (len(ids), plan.project, " ".join(ids)))
    return EXIT_OK


def cmd_plan(args):
    plan = load_plan(args.plan)
    waves = plan.waves()
    _out("项目 %s：%d 条任务，%d 个并行批次" % (plan.project, len(plan), len(waves)))
    for index, wave in enumerate(waves, 1):
        _out("  第 %d 批  %s" % (index, " ".join(sorted(t.id for t in wave))))
    conflicts = plan.conflicts()
    if conflicts:
        _out("写冲突 %d 处：" % len(conflicts))
        for a, b, ga, gb in conflicts[:20]:
            _out("  %s ↔ %s（%s / %s）" % (a, b, ga, gb))
    else:
        _out("写作用域没有冲突")
    targets = plan.sync_targets()
    if targets:
        _out("产物分发 %d 处：" % len(targets))
        for target in targets:
            users = [t.id for t in plan.tasks
                     if any(x.key() == target.key()
                            for x in task.merge_sync(plan.sync, t.sync))]
            _out("  %s  ←  %s%s" % (
                target, " ".join(sorted(users)),
                "（只推 %s）" % " ".join(target.paths) if target.paths else ""))
    if args.json:
        _out(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))
    return EXIT_OK


def cmd_status(args):
    if args.hub_url:
        return _status_from_hub(args)

    queue = open_queue(args)
    try:
        stats = queue.stats()
        jobs = queue.list(limit=args.limit)
        projects = sorted({j.project for j in queue.list()})
    finally:
        queue.close()

    order = [queue_mod.QUEUED, queue_mod.RUNNING, queue_mod.DONE,
             queue_mod.FAILED, queue_mod.BLOCKED, queue_mod.CANCELED]
    parts = ["%s %d" % (state, stats[state]) for state in order if stats.get(state)]
    _out("队列 %s" % args.db)
    _out("  作业  %s" % ("，".join(parts) if parts else "空"))
    _out("  项目  %s" % ("，".join(projects) if projects else "-"))
    queue = open_queue(args)
    try:
        deliv = queue.delivery_stats()
    finally:
        queue.close()
    if deliv.get(queue_mod.DELIVERY_PENDING) or deliv.get(queue_mod.DELIVERY_FAILED):
        bits = []
        if deliv.get(queue_mod.DELIVERY_PENDING):
            bits.append("待补送 %d" % deliv[queue_mod.DELIVERY_PENDING])
        if deliv.get(queue_mod.DELIVERY_FAILED):
            bits.append("失败 %d" % deliv[queue_mod.DELIVERY_FAILED])
        _out("  分发  %s（mesh deliveries 看详情）" % "，".join(bits))
    if jobs:
        _out("")
        _out(_jobs_table(jobs))
    else:
        _out("  （没有作业。用 mesh submit 推一份计划进来）")
    return EXIT_OK


def _status_from_hub(args):
    """问一个在跑的 hub 要状态（能看到节点在线情况，队列文件看不到）。"""
    client = wire.Client(args.hub_url, args.hub_token or "",
                         ca=getattr(args, "tls_ca", None),
                         insecure=getattr(args, "tls_insecure", False))
    try:
        result = client.call("status")
        jobs = client.call("jobs", {"limit": args.limit})
    except wire.WireError as exc:
        raise CliError("连不上控制面 %s：%s" % (args.hub_url, exc))

    _out("控制面 %s" % args.hub_url)
    if result["nodes"]:
        rows = []
        for n in result["nodes"]:
            rows.append([
                n["node_id"], "在线" if n["online"] else "离线",
                "%s/%s" % (n.get("slots_used", 0), n.get("slots_total", 0)),
                n.get("load1", "-"), ",".join(n.get("tags") or []) or "-",
                ",".join(n.get("roots") or []) or "-",
            ])
        _out(table(rows, ["节点", "状态", "槽位", "负载", "标签", "root"]))
    else:
        _out("  （还没有节点接入）")

    bits = ["调度权 %s" % ("在派发" if result.get("leader") else "待命（别人在派）")]
    if result.get("mirror"):
        bits.append("产物镜像 %s" % result["mirror"])
    auth_info = result.get("auth") or {}
    bits.append("鉴权 %s" % ("已开（%d 把令牌）" % (auth_info.get("grants", 0))
                            if auth_info.get("enabled") else "未配置（同网段开放）"))
    _out("")
    _out("  ".join(bits))

    stats = result["jobs"]
    order = [queue_mod.QUEUED, queue_mod.RUNNING, queue_mod.DONE,
             queue_mod.FAILED, queue_mod.BLOCKED, queue_mod.CANCELED]
    parts = ["%s %d" % (s, stats[s]) for s in order if stats.get(s)]
    _out("作业 %s" % ("，".join(parts) if parts else "空"))
    if jobs:
        _out("")
        _out(_jobs_table([queue_mod.Job.from_dict(j) for j in jobs]))
    return EXIT_OK


def _jobs_table(jobs):
    rows = []
    for job in jobs:
        rows.append([
            job.id, job.project, job.state,
            "%d/%d" % (job.attempts, job.max_attempts),
            (job.title or job.cmd or "")[:40],
        ])
    return table(rows, ["id", "项目", "状态", "尝试", "标题"])


def cmd_jobs(args):
    queue = open_queue(args)
    try:
        jobs = queue.list(project=args.project, state=args.state, limit=args.limit)
        if args.json:
            _out(json.dumps([j.to_dict() for j in jobs], ensure_ascii=False, indent=2))
        elif jobs:
            _out(_jobs_table(jobs))
        else:
            _out("没有符合条件的作业")
        if args.show:
            job = queue.get(args.show)
            if job is None:
                raise CliError("队列里没有作业 %s" % args.show)
            _out("")
            _out(json.dumps(job.to_dict(), ensure_ascii=False, indent=2))
    finally:
        queue.close()
    return EXIT_OK


def cmd_retry(args):
    queue = open_queue(args)
    try:
        for job_id in args.job_id:
            queue.retry(job_id, reset_attempts=args.reset)
            _out("已重新入队 %s" % job_id)
    except queue_mod.QueueError as exc:
        raise CliError(str(exc))
    finally:
        queue.close()
    return EXIT_OK


def cmd_cancel(args):
    queue = open_queue(args)
    try:
        for job_id in args.job_id:
            queue.cancel(job_id, reason=args.reason)
            _out("已取消 %s" % job_id)
    finally:
        queue.close()
    return EXIT_OK


def cmd_sync(args):
    """重推产物：目标当时离线、或者想再发一遍的时候用。"""
    hub = hub_mod.Hub(args.db, mirror=args.mirror,
                      ca=getattr(args, "tls_ca", None),
                      tls_insecure=getattr(args, "tls_insecure", False))
    try:
        failed = 0
        for job_id in args.job_id:
            try:
                report = hub.sync_job(job_id, targets=args.to)
            except hub_mod.HubError as exc:
                _out("%s：%s" % (job_id, exc))
                failed += 1
                continue

            if args.json:
                _out(json.dumps(report, ensure_ascii=False, indent=2))
                continue

            _out("%s  源 mesh://%s/%s" % (job_id, report["node"], report["root"]))
            if report["targets"]:
                rows = []
                for t in report["targets"]:
                    if t["skipped"]:
                        state = t["skipped"]
                    elif not t["ok"]:
                        state = "失败：%s" % "；".join(t["errors"][:2])
                    else:
                        state = "已同步"
                    modes = {}
                    for f in t["files"]:
                        modes[f["mode"]] = modes.get(f["mode"], 0) + 1
                    rows.append([t["to"], _count_modes(modes),
                                 _bytes_human(t["sent"]), _bytes_human(t["size"]),
                                 (_saved_text(t) if t["size"] else "-"), state])
                _out(table(rows, ["目标", "文件", "传输", "大小", "省", "状态"]))
            for err in report["errors"]:
                _out("  失败  %s" % err)
            if not report["ok"]:
                failed += 1
        return EXIT_OK if failed == 0 else 1
    finally:
        hub.close()


def _count_modes(modes):
    """``{'delta': 2, 'same': 1}`` → ``差分 2 / 相同 1``。"""
    labels = {"delta": "差分", "full": "整取", "same": "相同",
              "delete": "删除", "absent": "已无"}
    total = sum(modes.values())
    if not total:
        return "-"
    parts = ["%s %d" % (labels.get(k, k), n) for k, n in sorted(modes.items())]
    return "%d（%s）" % (total, "，".join(parts))


def _bytes_human(size):
    size = float(size or 0)
    for unit in ("B", "KB", "MB"):
        if size < 1024 or unit == "MB":
            return ("%d %s" % (size, unit)) if unit == "B" else ("%.1f %s" % (size, unit))
        size /= 1024.0
    return "%.1f MB" % size


def _saved_text(target):
    if not target["size"]:
        return "-"
    saved = max(0, target["size"] - target["sent"])
    return "%.0f%%" % (100.0 * saved / float(target["size"]))


def cmd_deliveries(args):
    """补送队列：作业都 done 了，但产物还有没送到的。"""
    queue = open_queue(args)
    try:
        rows = queue.deliveries(job_id=args.job, state=args.state, limit=args.limit)
        stats = queue.delivery_stats()
    finally:
        queue.close()

    if args.json:
        _out(json.dumps(rows, ensure_ascii=False, indent=2))
        return EXIT_OK

    if not rows:
        _out("没有要补送的产物" if not stats else "没有符合条件的记录")
        return EXIT_OK

    labels = {queue_mod.DELIVERY_PENDING: "待补送",
              queue_mod.DELIVERY_DONE: "已送到",
              queue_mod.DELIVERY_FAILED: "失败"}
    table_rows = []
    for r in rows:
        when = "-"
        if r["state"] == queue_mod.DELIVERY_PENDING and r["next_at"]:
            left = int(r["next_at"] - time.time())
            when = "马上" if left <= 0 else "%d 秒后" % left
        table_rows.append([
            r["job_id"], r["to"], r["state"] and labels.get(r["state"], r["state"]),
            r["attempts"], when, (r["error"] or "-")[:40],
        ])
    _out(table(table_rows, ["作业", "目标", "状态", "尝试", "下次", "上次错误"]))
    bits = ["%s %d" % (labels.get(k, k), v) for k, v in sorted(stats.items())]
    if bits:
        _out("")
        _out("合计  %s" % "，".join(bits))
        if stats.get(queue_mod.DELIVERY_PENDING) or stats.get(queue_mod.DELIVERY_FAILED):
            _out("控制面在跑的话，会用退避自动重试；想立刻重来一遍：mesh sync <作业>")
    return EXIT_OK


# --------------------------------------------------------------------------
# mesh nodes / mesh token（直接改库，不需要 hub 在跑）
# --------------------------------------------------------------------------
def cmd_nodes(args):
    if args.action == "ls":
        queue = open_queue(args)
        try:
            rows = queue.load_nodes()
        finally:
            queue.close()
        if args.json:
            _out(json.dumps(rows, ensure_ascii=False, indent=2))
        elif rows:
            _out(table([[r["node_id"], r["url"], ",".join(r["tags"]) or "-",
                         r["weight"], "有" if r["token"] else "-"] for r in rows],
                       ["节点", "地址", "标签", "权重", "令牌"]))
        else:
            _out("还没有节点。mesh nodes add <id> <url> 加一个")
        return EXIT_OK

    if args.node_id is None:
        raise CliError("要加/删节点就得给 node_id")
    queue = open_queue(args, create=True)
    try:
        if args.action == "add":
            if not args.url:
                raise CliError("nodes add 需要地址：mesh nodes add 手机 http://ip:8765")
            queue.save_node(args.node_id, args.url, args.token or "",
                            split_list(args.tags), args.weight)
            _out("已接入 %s → %s" % (args.node_id, args.url))
        else:
            if not queue.delete_node(args.node_id):
                raise CliError("库里没有节点 %s" % args.node_id)
            _out("已移除 %s" % args.node_id)
    finally:
        queue.close()
    return EXIT_OK


def cmd_token(args):
    if args.action == "ls":
        queue = open_queue(args)
        try:
            rows = queue.load_tokens()
        finally:
            queue.close()
        if args.json:
            _out(json.dumps(rows, ensure_ascii=False, indent=2))
        elif rows:
            _out(table([[r["label"] or "-", r["project"] or "（全部项目）",
                         ",".join(r["perms"]), r["token_hash"][:12] + "…"] for r in rows],
                       ["标签", "项目", "权限", "指纹"]))
        else:
            _out("还没有令牌 —— 控制面对同网段完全开放")
        return EXIT_OK

    if args.action == "add":
        perms = split_list(args.perms) or ["read"]
        try:
            auth_mod.normalize(perms)          # 早点炸，别等写进库才发现拼错
        except ValueError as exc:
            raise CliError(str(exc))
        token = auth_mod.new_token()
        queue = open_queue(args, create=True)
        try:
            queue.add_token(token, perms, args.project or "", args.label or "")
        finally:
            queue.close()
        _out("已创建令牌（只显示这一次，记下来）：")
        _out("")
        _out("  %s" % token)
        _out("")
        _out("  项目  %s" % (args.project or "（全部项目）"))
        _out("  权限  %s" % ",".join(perms))
        return EXIT_OK

    queue = open_queue(args)
    try:
        ok = queue.delete_token(label=args.label, token_hash=args.hash)
    finally:
        queue.close()
    if not ok:
        raise CliError("没找到要删的令牌（给 --label 或 --hash）")
    _out("已删除")
    return EXIT_OK


# --------------------------------------------------------------------------
# mesh doctor
# --------------------------------------------------------------------------
def cmd_doctor(args):
    """体检：本机能不能当节点、队列文件能不能写。"""
    problems = []

    _out("mesh %s（协议 %d）" % (__version__, wire.PROTO))
    _out("  Python  %s" % sys.version.split()[0])
    if sys.version_info < (3, 8):
        problems.append("Python 需要 3.8 以上")

    cap = capability.probe("doctor", with_versions=False)
    _out("  平台    %s %s / %s" % (cap.os_name, cap.os_release, cap.arch))
    _out("  CPU     %d 核，负载 %s" % (cap.cpu_count, cap.loadavg or "-"))
    _out("  内存    可用 %d MB / 共 %d MB" % (cap.mem_avail >> 20, cap.mem_total >> 20))
    if cap.tools:
        _out("  工具    %s" % ", ".join(sorted(cap.tools)))

    for text in (args.root or []):
        entry = parse_root(text)
        if os.path.isdir(entry["path"]):
            if not os.access(entry["path"], os.R_OK):
                problems.append("root %s 不可读：%s" % (entry["name"], entry["path"]))
            mode = "可写" if os.access(entry["path"], os.W_OK) else "只读"
            _out("  root    %-10s %-6s %s（%s）"
                 % (entry["name"], entry["mode"], entry["path"], mode))
        else:
            problems.append("root %s 目录不存在：%s" % (entry["name"], entry["path"]))

    parent = os.path.dirname(os.path.abspath(args.db)) or "."
    if os.path.isdir(parent) and os.access(parent, os.W_OK):
        _out("  队列    %s（可写）" % os.path.abspath(args.db))
    else:
        problems.append("队列目录不可写：%s" % parent)

    _out("  依赖    只用标准库")
    if problems:
        _out("")
        for item in problems:
            _err(item)
        return EXIT_USAGE
    _out("")
    _out("没有发现问题")
    return EXIT_OK


# --------------------------------------------------------------------------
# 参数
# --------------------------------------------------------------------------
def build_parser():
    parser = argparse.ArgumentParser(
        prog="mesh", description="个人 AI 网格：把文件、能力、算力放一张网里",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""常用流程：
  mesh node --id 手机 --root work=~/proj --port 8765      # 一台机器上线
  mesh hub  --db mesh.db --node 手机=http://手机IP:8765   # 控制面开始调度
  mesh submit plan.json --db mesh.db                     # 推一份任务图
  mesh status --hub http://127.0.0.1:8788                # 看全程
""")
    parser.add_argument("--version", action="version",
                        version="mesh %s" % __version__)
    sub = parser.add_subparsers(dest="command")

    # -- node
    p = sub.add_parser("node", help="起一个节点（把本机的目录和能力暴露出去）")
    p.add_argument("--id", required=True, help="节点名，比如 手机 / 笔记本")
    p.add_argument("--root", action="append", metavar="NAME=PATH[:ro]",
                   help="命名目录，可重复：--root work=~/proj --root data=/srv:ro")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--bind", default="0.0.0.0", help="监听地址（默认全部网卡）")
    p.add_argument("--token", default="", help="访问令牌；不给就是同网段裸奔")
    p.add_argument("--slots", type=int, default=1, help="同时能跑几个作业")
    p.add_argument("--tags", default="", help="标签，逗号分隔：gpu,fast")
    p.add_argument("--models", default="", help="可用模型，逗号分隔")
    p.add_argument("--agents", default="", help="可用 agent 类型，逗号分隔")
    p.add_argument("--tools", default="", help="只探测这些工具，逗号分隔")
    p.add_argument("--exec", action="store_true", help="允许执行命令（需要 --token）")
    p.add_argument("--no-rollback", action="store_true",
                   help="不做作业前备份（默认开启：越界改动会被还原）")
    p.add_argument("--backup-max-mb", type=float, default=64.0,
                   help="回滚备份的体积上限（MB，默认 64；超出只检测不撤销）")
    p.add_argument("--tls-cert", metavar="FILE", help="证书（和 --tls-key 成对）")
    p.add_argument("--tls-key", metavar="FILE", help="私钥")
    p.set_defaults(func=cmd_node)

    # -- hub
    p = sub.add_parser("hub", help="起控制面：回收、心跳、派发、收尾")
    p.add_argument("--db", default="mesh.db", help="队列文件（默认 mesh.db）")
    p.add_argument("--nodes-file", metavar="FILE", help="节点清单 JSON（会写进库里）")
    p.add_argument("--mirror", metavar="DIR",
                   help="产物落地目录：作业改过的文件按增量收回来")
    p.add_argument("--tls-cert", metavar="FILE", help="面板/接口证书（和 --tls-key 成对）")
    p.add_argument("--tls-key", metavar="FILE", help="面板/接口私钥")
    p.add_argument("--tls-ca", metavar="FILE", help="连节点时用来校验的 CA")
    p.add_argument("--tls-insecure", action="store_true",
                   help="连节点时不校验证书（自签内网先用这个）")
    p.add_argument("--node", action="append", metavar="ID=URL", help="直接给一个节点，可重复")
    p.add_argument("--node-token", default="", help="--node 用的令牌")
    p.add_argument("--port", type=int, default=None,
                   help="开面板端口（不给就不开）；手机浏览器可看")
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--token", default="", help="面板/接口令牌")
    p.add_argument("--interval", type=float, default=2.0, help="调度间隔秒（默认 2）")
    p.add_argument("--ttl", type=float, default=capability.STALE_TTL,
                   help="多久没心跳算掉线（秒）")
    p.add_argument("--dispatch-timeout", type=float,
                   default=hub_mod.DEFAULT_DISPATCH_TIMEOUT,
                   help="单个作业派发的超时（秒）")
    p.add_argument("--lease", type=float, default=None,
                   help="租约秒数（默认 = 派发超时 + 60）")
    p.add_argument("--once", action="store_true", help="只跑一轮就退出（调试用）")
    p.set_defaults(func=cmd_hub)

    # -- submit
    p = sub.add_parser("submit", help="把一份计划推进队列")
    p.add_argument("plan", help="计划 JSON：{project, tasks:[...]}")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--force", action="store_true", help="有写冲突也提交")
    p.set_defaults(func=cmd_submit)

    # -- plan
    p = sub.add_parser("plan", help="校验计划并预览批次/冲突")
    p.add_argument("plan")
    p.add_argument("--json", action="store_true", help="顺带打印规范化后的 JSON")
    p.set_defaults(func=cmd_plan)

    # -- status / jobs
    p = sub.add_parser("status", help="看队列或控制面状态")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--hub", dest="hub_url", default="",
                   help="问一个在跑的控制面（能看节点在线情况）")
    p.add_argument("--hub-token", default="")
    p.add_argument("--tls-ca", metavar="FILE", help="连控制面时用来校验的 CA")
    p.add_argument("--tls-insecure", action="store_true", help="不校验控制面证书")
    p.add_argument("--limit", type=int, default=15)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("jobs", help="列作业")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--project", default=None)
    p.add_argument("--state", default=None, choices=list(queue_mod.STATES),
                   help="只看某个状态")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--json", action="store_true")
    p.add_argument("--show", default=None, metavar="JOB_ID", help="打印某条作业的全文")
    p.set_defaults(func=cmd_jobs)

    # -- retry / cancel
    p = sub.add_parser("retry", help="把失败/阻塞的作业放回队列")
    p.add_argument("job_id", nargs="+")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--reset", action="store_true", help="连 attempts 一起清零")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("cancel", help="取消作业")
    p.add_argument("job_id", nargs="+")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--reason", default=None)
    p.set_defaults(func=cmd_cancel)

    # -- sync
    p = sub.add_parser("sync", help="把已完成的作业产物重新推给目标节点")
    p.add_argument("job_id", nargs="+")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--mirror", metavar="DIR",
                   help="产物镜像目录（有就直接用本地的，省一趟源节点往返）")
    p.add_argument("--to", action="append", metavar="ADDR",
                   help="临时指定目标（mesh://节点/root），可重复；不给就用计划里的")
    p.add_argument("--tls-ca", metavar="FILE", help="连节点时用来校验的 CA")
    p.add_argument("--tls-insecure", action="store_true", help="连节点时不校验证书")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_sync)

    # -- deliveries
    p = sub.add_parser("deliveries", help="看产物补送：哪些还没送到")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--state", default=None, choices=list(queue_mod.DELIVERY_STATES),
                   help="只看某个状态（pending 待补送 / failed 试够了 / done 已送到）")
    p.add_argument("--job", default=None, metavar="JOB_ID", help="只看某条作业")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_deliveries)

    # -- nodes
    p = sub.add_parser("nodes", help="管理节点清单（存在库里，重启不丢）")
    p.add_argument("action", choices=["ls", "add", "rm"])
    p.add_argument("node_id", nargs="?", help="节点名")
    p.add_argument("url", nargs="?", help="http(s)://host:port")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--token", default="", help="节点口令")
    p.add_argument("--tags", default="", help="标签，逗号分隔")
    p.add_argument("--weight", type=float, default=1.0)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_nodes)

    # -- token
    p = sub.add_parser("token", help="管理访问令牌（只存哈希）")
    p.add_argument("action", choices=["ls", "add", "rm"])
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--perms", default="read",
                   help="权限，逗号分隔：read,submit,cancel,admin")
    p.add_argument("--project", default="", help="限制到某个项目（默认全部）")
    p.add_argument("--label", default="", help="备注名，删的时候好用")
    p.add_argument("--hash", default=None, help="按指纹删")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_token)

    # -- doctor
    p = sub.add_parser("doctor", help="体检：本机能不能当节点、队列能不能写")
    p.add_argument("--db", default="mesh.db")
    p.add_argument("--root", action="append", metavar="NAME=PATH[:ro]")
    p.set_defaults(func=cmd_doctor)

    return parser


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return EXIT_OK
    try:
        return args.func(args)
    except CliError as exc:
        _err(str(exc))
        return EXIT_USAGE
    except KeyboardInterrupt:
        return EXIT_OK
    except BrokenPipeError:                    # `mesh jobs | head`
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
