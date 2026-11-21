"""控制面：把作业派给合适的节点，把结果收回来。

调度循环（:meth:`Hub.tick`）一轮做四件事：

1. **回收**：租约过期的作业回队列（节点掉线不会卡死整个项目）
2. **心跳**：向每个节点要一份能力快照；连不上的立刻从调度池里摘掉
3. **派发**：按优先级遍历就绪作业，用 :func:`capability.score` 挑节点
4. **收尾**：成功 → ``complete``（解锁下游）；失败 → ``fail``（按 attempts 决定重试）

为什么是"先挑节点、再领作业"
----------------------------
反过来的话，队列会先把作业标成 running，然后发现没有节点能跑它 —— 这条作业
就占着租约空等，直到超时被回收。节点一多，整个队列会被这类作业堵死。
所以 :meth:`Hub.tick` 对每条就绪作业先问注册表"谁能跑"，能跑才 :meth:`Queue.lease`。

派发失败（节点连不上）走 :meth:`Queue.unlease`：退回队列，**不消耗重试次数**。
"""

from __future__ import annotations

import html
import json
import threading
import time

from . import capability, queue as queue_mod, task, wire

__all__ = ["HubError", "Hub", "NodeRef"]

DEFAULT_POLL = 1.0
DEFAULT_DISPATCH_TIMEOUT = 1800.0
MAX_READY_SCAN = 200
LEASE_MARGIN = 60.0          # 租约在派发超时之外额外留的余量（秒）


class HubError(Exception):
    """控制面拒绝：计划有冲突、节点未注册等。"""


class NodeRef(object):
    """一个已注册节点的接入信息。"""

    __slots__ = ("node_id", "url", "token", "tags", "weight", "failures", "last_error")

    def __init__(self, node_id, url, token="", tags=None, weight=1.0):
        self.node_id = node_id
        self.url = url.rstrip("/")
        self.token = token
        self.tags = list(tags or [])
        self.weight = float(weight)
        self.failures = 0
        self.last_error = ""

    def to_dict(self):
        return {"node_id": self.node_id, "url": self.url, "tags": self.tags,
                "weight": self.weight, "failures": self.failures,
                "last_error": self.last_error}

    def __repr__(self):
        return "NodeRef(%s, %s)" % (self.node_id, self.url)


class Hub(object):
    """控制面。线程安全（内部一把可重入锁）。"""

    def __init__(self, db_path, token="", ttl=capability.STALE_TTL,
                 poll=DEFAULT_POLL, dispatch_timeout=DEFAULT_DISPATCH_TIMEOUT,
                 lease=None):
        # 租约必须比派发超时更长。否则一个还在节点上正常跑的长作业会被
        # `reap` 判定成掉线作业、退回队列、再由别的节点重跑一遍 —— 同一份
        # 改动被执行两次，正是租约机制本该防住的事。默认按派发超时再加余量。
        if lease is None:
            lease = float(dispatch_timeout) + LEASE_MARGIN
        self.queue = queue_mod.Queue(db_path, lease=lease)
        self.registry = capability.Registry(ttl=ttl)
        self.token = token
        self.poll = float(poll)
        self.dispatch_timeout = float(dispatch_timeout)
        self._refs = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 节点管理
    # ------------------------------------------------------------------
    def add_node(self, node_id, url, token="", tags=None, weight=1.0):
        if not node_id or not url:
            raise HubError("节点需要 node_id 和 url")
        with self._lock:
            ref = NodeRef(node_id, url, token, tags, weight)
            self._refs[node_id] = ref
        return ref

    def remove_node(self, node_id):
        with self._lock:
            ref = self._refs.pop(node_id, None)
        self.registry.remove(node_id)
        return ref is not None

    def nodes(self):
        with self._lock:
            return [r.to_dict() for r in sorted(self._refs.values(),
                                                key=lambda r: r.node_id)]

    def client_for(self, node_id):
        with self._lock:
            ref = self._refs.get(node_id)
        if ref is None:
            raise HubError("节点未注册：%s" % node_id)
        return wire.Client(ref.url, ref.token, timeout=self.dispatch_timeout)

    def refresh(self, now=None):
        """拉一遍所有节点的心跳。连不上的立刻移出调度池。"""
        now = now or time.time()
        live, dead = [], []
        for ref in self.nodes_refs():
            try:
                raw = self.client_for(ref.node_id).cap()
                cap = capability.Capability.from_dict(raw)
                if ref.tags:
                    cap.tags = sorted(set(cap.tags) | set(ref.tags))
                cap.last_seen = now
                self.registry.register(cap)
                ref.failures = 0
                ref.last_error = ""
                live.append(ref.node_id)
            except Exception as exc:
                ref.failures += 1
                ref.last_error = str(exc)
                self.registry.remove(ref.node_id)
                dead.append(ref.node_id)
        return {"live": live, "dead": dead}

    def nodes_refs(self):
        with self._lock:
            return [self._refs[k] for k in sorted(self._refs)]

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def submit(self, plan, allow_conflicts=False):
        """把一份计划推进队列。返回作业 id 列表。

        默认**拒绝有写冲突的计划** —— 两个没排序的作业写同一片区域，
        并行跑就是静默的数据损坏，宁可在提交时挡下来。
        """
        if isinstance(plan, dict):
            plan = task.Plan.from_dict(plan)
        plan.validate()

        conflicts = plan.conflicts()
        if conflicts and not allow_conflicts:
            detail = "; ".join("%s 与 %s 都写 %s" % (a, b, ga)
                               for a, b, ga, _gb in conflicts[:5])
            raise HubError("计划里有未排序的写冲突（%d 处）：%s" % (len(conflicts), detail))

        jobs = plan.to_jobs()
        self.queue.add_many(jobs)
        return [j.id for j in jobs]

    # ------------------------------------------------------------------
    # 调度
    # ------------------------------------------------------------------
    def tick(self, now=None):
        """跑一轮调度。返回这一轮发生了什么。"""
        now = now or time.time()
        report = {"reaped": [], "dispatched": [], "completed": [],
                  "failed": [], "unleased": [], "blocked": [], "nodes": {}}

        report["reaped"] = self.queue.reap(now=now)
        report["nodes"] = self.refresh(now=now)

        # 本轮已经派给各节点的条数。能力快照是上一轮心跳拍的，一个节点明明
        # 只有 1 个槽位，光看快照会一轮里塞给它 10 条 —— 后面 9 条在节点那边
        # 撞上"槽位已满"，又多绕一圈队列。这里按轮记账，快照过期也不会超发。
        busy = {}

        for job in self.queue.ready(now=now)[:MAX_READY_SCAN]:
            cap, why = self._pick_node(job.req, now, busy)
            if cap is None:
                report["blocked"].append({"job": job.id, "why": why})
                continue

            leased = self.queue.lease(job.id, cap.node_id, now=now)
            if leased is None:
                continue                      # 被别的调度者抢走了

            busy[cap.node_id] = busy.get(cap.node_id, 0) + 1
            outcome = self._dispatch(leased, cap)
            if outcome is None:
                # 节点连不上：退回队列，不消耗重试次数
                busy[cap.node_id] -= 1
                self.queue.unlease(leased.id, reason="节点 %s 不可达" % cap.node_id, now=now)
                report["unleased"].append(leased.id)
                continue

            report["dispatched"].append({"job": leased.id, "node": cap.node_id})
            if outcome.get("ok"):
                self.queue.complete(
                    leased.id, result=outcome,
                    feedback=(outcome.get("stdout") or "")[:1000])
                report["completed"].append(leased.id)
            else:
                error = outcome.get("error") or outcome.get("stderr") or "节点返回失败"
                state = self.queue.fail(leased.id, error=error[:2000], retryable=True)
                report["failed"].append({"job": leased.id, "state": state,
                                         "error": error[:200]})
        return report

    def _pick_node(self, req, now, busy):
        """挑一个本轮还有余量的节点。返回 ``(cap, None)`` 或 ``(None, 原因列表)``。"""
        for cand, _score in self.registry.candidates(req, now=now):
            if cand.slots_free - busy.get(cand.node_id, 0) > 0:
                return cand, None

        why = self.registry.explain(req, now=now)
        for node_id in sorted(busy):
            cap = self.registry.get(node_id)
            if cap is None or busy[node_id] < cap.slots_free:
                continue
            why.append("%s: 本轮已派 %d 条，槽位用满（%d/%d）"
                       % (node_id, busy[node_id], cap.slots_used, cap.slots_total))
        return None, why

    def _dispatch(self, job, cap):
        """把作业发给节点。传输失败返回 None（区别于节点报告的业务失败）。"""
        try:
            client = self.client_for(cap.node_id)
        except HubError:
            return None
        try:
            return client.run_job(job.to_dict(), lease=self.queue.lease_secs)
        except wire.WireError as exc:
            with self._lock:
                ref = self._refs.get(cap.node_id)
                if ref is not None:
                    ref.failures += 1
                    ref.last_error = str(exc)
            return None

    def run_until_idle(self, timeout=600.0, poll=None):
        """反复 tick 直到没有 queued/running 作业，或超时。"""
        poll = self.poll if poll is None else float(poll)
        deadline = time.time() + float(timeout)
        rounds = 0
        while True:
            report = self.tick()
            rounds += 1
            stats = self.queue.stats()
            pending = stats.get(queue_mod.QUEUED, 0) + stats.get(queue_mod.RUNNING, 0)
            if pending == 0:
                return {"rounds": rounds, "stats": stats, "last": report}
            if time.time() >= deadline:
                return {"rounds": rounds, "stats": stats, "last": report,
                        "timeout": True}
            time.sleep(poll)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def status(self):
        caps = {c.node_id: c for c in self.registry.list(include_stale=True)}
        nodes = []
        for ref in self.nodes_refs():
            cap = caps.get(ref.node_id)
            entry = ref.to_dict()
            entry["online"] = cap is not None and not cap.is_stale()
            if cap is not None:
                entry.update({
                    "slots_total": cap.slots_total, "slots_used": cap.slots_used,
                    "cpu_count": cap.cpu_count, "load1": round(cap.load1, 2),
                    "tags": sorted(set(cap.tags) | set(ref.tags)),
                    "roots": cap.root_names(),
                })
            nodes.append(entry)
        return {
            "nodes": nodes,
            "jobs": self.queue.stats(),
            "projects": sorted({j.project for j in self.queue.list()}),
        }

    def jobs(self, project=None, state=None, limit=None):
        return self.queue.list(project=project, state=state, limit=limit)

    # ------------------------------------------------------------------
    # 对外接口：手机浏览器 / curl 看全程
    # ------------------------------------------------------------------
    def build_server(self, host="127.0.0.1", port=0):
        """把控制面挂成一个 HTTP 服务：``POST /rpc/<方法>`` 给脚本，
        ``GET /`` 给人看。"""
        srv = wire.Server(host=host, port=port, token=self.token, node_id="hub")
        srv.add("ping", lambda payload: {"pong": time.time()})
        srv.add("status", lambda payload: self.status())
        srv.add("jobs", lambda payload: [
            j.to_dict() for j in self.jobs(
                project=payload.get("project"), state=payload.get("state"),
                limit=payload.get("limit"))])
        srv.add("submit", lambda payload: {
            "ids": self.submit(payload.get("plan") or payload,
                               allow_conflicts=bool(payload.get("allow_conflicts")))})
        srv.add_get("/status", lambda query: self.status())
        srv.add_get("/", self.dashboard)
        return srv

    def dashboard(self, query=None):
        """自包含状态页：没有外部资源，手机浏览器打开就能看。"""
        query = query or {}
        try:
            refresh = max(1, min(60, int(query.get("refresh") or 5)))
        except (TypeError, ValueError):
            refresh = 5

        st = self.status()
        out = [
            "<!doctype html><html lang=\"zh\"><head><meta charset=\"utf-8\">",
            "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",
            "<meta http-equiv=\"refresh\" content=\"%d\">" % refresh,
            "<title>mesh</title><style>",
            "body{font:14px/1.5 -apple-system,system-ui,sans-serif;margin:0;padding:16px;",
            "background:#0f1115;color:#e6e6e6}h1{font-size:18px;margin:0 0 4px}",
            "h2{font-size:14px;margin:24px 0 8px;color:#8b93a1;font-weight:600}",
            "table{width:100%;border-collapse:collapse;font-size:13px}",
            "th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #23262d}",
            "th{color:#8b93a1;font-weight:500}code{color:#7ec699}",
            ".on{color:#7ec699}.off{color:#e06c75}.muted{color:#8b93a1}",
            "</style></head><body>",
            "<h1>mesh <span class=\"muted\">%s</span></h1>" % html.escape(str(self.queue.path)),
            "<div class=\"muted\">每 %d 秒刷新 · 作业 %s</div>" % (
                refresh, html.escape(json.dumps(st["jobs"], ensure_ascii=False, sort_keys=True))),
            "<h2>节点</h2>",
        ]
        if st["nodes"]:
            out.append("<table><tr><th>节点</th><th>状态</th><th>槽位</th>"
                       "<th>负载</th><th>标签</th></tr>")
            for n in st["nodes"]:
                out.append(
                    "<tr><td>%s</td><td class=\"%s\">%s</td><td>%s/%s</td>"
                    "<td>%s</td><td>%s</td></tr>" % (
                        html.escape(str(n["node_id"])),
                        "on" if n["online"] else "off",
                        "在线" if n["online"] else "离线",
                        n.get("slots_used", 0), n.get("slots_total", 0),
                        n.get("load1", "-"), html.escape(",".join(n.get("tags") or []))))
            out.append("</table>")
        else:
            out.append("<div class=\"muted\">还没有节点接入</div>")

        out.append("<h2>作业</h2>")
        rows = self.jobs(limit=50)
        if rows:
            out.append("<table><tr><th>id</th><th>项目</th><th>状态</th>"
                       "<th>尝试</th><th>标题</th></tr>")
            for j in rows:
                out.append("<tr><td><code>%s</code></td><td>%s</td><td>%s</td>"
                           "<td>%d/%d</td><td>%s</td></tr>" % (
                               html.escape(j.id), html.escape(j.project),
                               html.escape(j.state), j.attempts, j.max_attempts,
                               html.escape(j.title or j.cmd or "")))
            out.append("</table>")
        else:
            out.append("<div class=\"muted\">队列是空的</div>")
        out.append("</body></html>")
        return "\n".join(out)

    def close(self):
        self.queue.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __repr__(self):
        return "Hub(%d nodes, %s)" % (len(self._refs), self.queue.stats())
