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
import os
import re
import tempfile
import threading
import time
import uuid

from . import auth as auth_mod
from . import capability, delta, queue as queue_mod, task, wire

__all__ = ["HubError", "Hub", "NodeRef"]

DEFAULT_POLL = 1.0
DEFAULT_DISPATCH_TIMEOUT = 1800.0
MAX_READY_SCAN = 200
LEASE_MARGIN = 60.0          # 租约在派发超时之外额外留的余量（秒）
DEFAULT_LEADER_TTL = 15.0    # 调度权租约：一个 tick 之内必须能续上
MIRROR_CHUNK = 1 << 20
MAX_SIG_BLOCKS_PULL = 96     # 拉产物时签名表最多这么多块（每块 ≈88 字节）

# 每个 RPC 方法需要什么权限。没列出来的方法一律按 admin 处理 —— 加了新方法
# 忘了写在这里，是"默认更严"，不是"默认放行"。
RPC_ACTION = {
    "ping": "read",
    "status": "read",
    "jobs": "read",
    "submit": "submit",
    "cancel": "cancel",
    "retry": "cancel",
}
GET_ACTION = {"/": "read", "/status": "read"}


def _block_for(size):
    """挑差分块大小。

    ``delta.choose_block_size`` 用的是 rsync 的 sqrt 经验值，但那是按 16 字节
    二进制强校验算的；这里每块要带 64 个字符的 sha256 hex（JSON 里 ≈88 字节），
    同样的块数下签名表重 5 倍 —— 小文件上签名表比文件本身还大，差分反而更贵。
    所以再按"签名表最多 MAX_SIG_BLOCKS_PULL 块"兜一次底。
    """
    guess = delta.choose_block_size(size)
    if size > 0:
        need = max(1, size // MAX_SIG_BLOCKS_PULL)
        block = delta.MIN_BLOCK_SIZE
        while block < need and block < delta.MAX_BLOCK_SIZE:
            block <<= 1
        guess = max(guess, block)
    return max(delta.MIN_BLOCK_SIZE, min(delta.MAX_BLOCK_SIZE, guess))


def _mirror_path(base, rel):
    """把节点报上来的相对路径落到镜像目录里，**不许逃出去**。

    ``changed`` 是节点算出来的，正常情况下不会带 ``..``；但节点是不可信的一方，
    一个改过的节点随手回一句 ``../../.ssh/authorized_keys`` 就能往控制面上写文件。
    所以这里按段拼接并核对最终路径仍在 base 之内。
    """
    raw = str(rel or "").replace("\\", "/")
    if raw.startswith("/") or ":" in raw:
        # 绝对路径/带冒号：节点报这种路径本身就是异常信号，直接拒
        raise HubError("产物路径必须是相对路径：%r" % (raw,))
    rel = raw.strip("/")
    if not rel:
        raise HubError("产物路径不合法：%r" % (raw,))
    parts = [p for p in rel.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise HubError("产物路径不合法（含 ..）：%r" % (rel,))
    target = os.path.abspath(os.path.join(base, *parts))
    root = os.path.abspath(base) + os.sep
    if not (target + os.sep).startswith(root):
        raise HubError("产物路径越界：%r" % (rel,))
    return target


def _safe_name(name):
    """把节点名 / root 名变成能当目录名用的东西。

    保留汉字：中文节点名是最常见的用法，剥掉就成了 ``unnamed``，
    两个中文名字还会撞到同一个目录里去。
    """
    cleaned = re.sub(r"[^\w.-]", "_", str(name or ""), flags=re.UNICODE)
    return cleaned.strip("._")[:64] or "unnamed"


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
                 lease=None, mirror=None, auth=None, leader_ttl=DEFAULT_LEADER_TTL,
                 ca=None, tls_insecure=False, load_nodes=True):
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
        # 产物回收的落地目录；None 表示不收（只看 stdout 和变更清单）
        self.mirror = os.path.abspath(mirror) if mirror else None
        self.ca = ca
        self.tls_insecure = bool(tls_insecure)
        # 令牌：给了 --token 就是一把管理员口令；多租户由 auth 决定
        self.auth = auth if auth is not None else auth_mod.Auth(
            admin_tokens=[token] if token else [])
        self.holder = "hub-%d-%s" % (os.getpid(), uuid.uuid4().hex[:8])
        self.leader_ttl = float(leader_ttl)
        self._leader = False
        self._refs = {}
        self._lock = threading.RLock()
        if load_nodes:
            self._load_nodes()

    # ------------------------------------------------------------------
    # 节点管理
    # ------------------------------------------------------------------
    def add_node(self, node_id, url, token="", tags=None, weight=1.0, persist=True):
        """接入一个节点。默认写进库里，控制面重启后自动恢复。"""
        if not node_id or not url:
            raise HubError("节点需要 node_id 和 url")
        with self._lock:
            ref = NodeRef(node_id, url, token, tags, weight)
            self._refs[node_id] = ref
        if persist:
            self.queue.save_node(node_id, ref.url, token, ref.tags, ref.weight)
        return ref

    def remove_node(self, node_id, persist=True):
        with self._lock:
            ref = self._refs.pop(node_id, None)
        self.registry.remove(node_id)
        if persist:
            self.queue.delete_node(node_id)
        return ref is not None

    def _load_nodes(self):
        """从库里恢复节点清单（不重复回写）。"""
        for row in self.queue.load_nodes():
            ref = NodeRef(row["node_id"], row["url"], row["token"],
                          row["tags"], row["weight"])
            self._refs[row["node_id"]] = ref
        return len(self._refs)

    def nodes(self):
        with self._lock:
            return [r.to_dict() for r in sorted(self._refs.values(),
                                                key=lambda r: r.node_id)]

    def client_for(self, node_id):
        with self._lock:
            ref = self._refs.get(node_id)
        if ref is None:
            raise HubError("节点未注册：%s" % node_id)
        return wire.Client(ref.url, ref.token, timeout=self.dispatch_timeout,
                           ca=self.ca, insecure=self.tls_insecure)

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
                  "failed": [], "unleased": [], "blocked": [], "nodes": {},
                  "standby": None}

        # 同一份队列可以被多个控制面盯着，但只有一个在派发。抢不到调度权就
        # 什么都不做 —— 两个调度器同时决策会互相把作业从对方手里抢走。
        if not self.ensure_leadership(now=now):
            report["standby"] = self.queue.lock_holder(queue_mod.LEADER_LOCK)
            return report

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
                if self.mirror:
                    # 产物在 complete 之前收：收的结果要跟着作业一起落库
                    outcome["artifacts"] = self.collect(leased, cap, outcome)
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

    # ------------------------------------------------------------------
    # 调度权
    # ------------------------------------------------------------------
    def ensure_leadership(self, now=None):
        """续上（或抢下）调度权。返回自己是否在派发。"""
        if self._leader and self.queue.renew_lock(
                queue_mod.LEADER_LOCK, self.holder, self.leader_ttl, now=now):
            return True
        self._leader = self.queue.acquire_lock(
            queue_mod.LEADER_LOCK, self.holder, self.leader_ttl, now=now)
        return self._leader

    def release_leadership(self):
        if self._leader:
            self.queue.release_lock(queue_mod.LEADER_LOCK, self.holder)
            self._leader = False

    @property
    def is_leader(self):
        return self._leader

    # ------------------------------------------------------------------
    # 产物回收（只取增量）
    # ------------------------------------------------------------------
    def _root_for(self, job, cap):
        """作业落在这个节点的哪个 root 上。"""
        want = (job.req or {}).get("root")
        if want:
            return want
        names = cap.root_names() if cap is not None else []
        return names[0] if len(names) == 1 else ""

    def collect(self, job, cap, outcome):
        """把作业改过的文件收回来：本地已有旧版就只传差分。

        这是 ``delta`` 真正的用处：改一个 10MB 文件里的 200 字节，
        传的是 200 字节加几条块引用，不是 10MB。
        """
        root = self._root_for(job, cap)
        node_id = cap.node_id
        report = {"node": node_id, "root": root, "files": [], "errors": [],
                  "sent": 0, "size": 0}
        if not root:
            report["errors"].append("作业没指定 root，无法判断产物落在哪")
            return report

        base = os.path.join(self.mirror, _safe_name(node_id), _safe_name(root))
        try:
            os.makedirs(base, exist_ok=True)
        except OSError as exc:
            report["errors"].append("建不了目录 %s：%s" % (base, exc))
            return report

        try:
            client = self.client_for(node_id)
        except HubError as exc:
            report["errors"].append(str(exc))
            return report

        for rel in outcome.get("changed") or []:
            addr = "mesh://%s/%s/%s" % (node_id, root, rel)
            try:
                local = _mirror_path(base, rel)
                item = self._pull_file(client, addr, local, rel)
            except (wire.WireError, delta.DeltaError, OSError, HubError) as exc:
                report["errors"].append("%s：%s" % (rel, exc))
                continue
            report["files"].append(item)
            report["sent"] += item["sent"]
            report["size"] += item["size"]

        for rel in outcome.get("deleted") or []:
            try:
                local = _mirror_path(base, rel)
                if os.path.isfile(local):
                    os.remove(local)
                report["files"].append({"path": rel, "mode": "delete",
                                        "sent": 0, "size": 0})
            except (OSError, HubError) as exc:
                report["errors"].append("%s：%s" % (rel, exc))

        report["saved"] = max(0, report["size"] - report["sent"])
        report["ratio"] = (round(1.0 - report["sent"] / float(report["size"]), 4)
                           if report["size"] else 0.0)
        return report

    def _pull_file(self, client, addr, local, rel):
        """一个文件：把**本地旧文件的签名表**发过去，换回一份差分。

        rsync 的方向是这么定的：签名表由接收方（这里）从自己手上的旧版本算，
        发送方（节点）拿它在自己的新版本上滑窗比对。反过来问节点要签名表是错的 ——
        节点只有新版本，算出来的签名跟本地旧文件对不上，永远退化成整取。
        """
        old = b""
        if os.path.isfile(local):
            with open(local, "rb") as f:
                old = f.read()

        mode, sent_up, sent_down = "full", 0, 0
        if old:
            block = _block_for(len(old))
            sig = [list(item) for item in delta.block_signature(old, block)]
            up = len(json.dumps(sig).encode("utf-8"))
            packed = client.mkdelta(addr, delta.sha256_hex(old), block, sig)
            down = len(json.dumps(packed["delta"]).encode("utf-8"))
            new_size = int((packed.get("delta") or {}).get("new_size") or 0)
            if up + down < new_size:
                new = delta.patch(old, packed["delta"])
                sent_up, sent_down, mode = up, down, "delta"
            else:
                # 小文件/大改：差分的额外开销比整取还贵，直接整取
                new = client.read(addr)
                sent_down = len(new)
        else:
            new = client.read(addr)
            sent_down = len(new)
        sent = sent_up + sent_down

        parent = os.path.dirname(local)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # 先写临时文件再改名：中途断了不会留下半个文件
        tmp = local + ".part"
        with open(tmp, "wb") as f:
            f.write(new)
        os.replace(tmp, local)
        return {"path": rel, "mode": mode, "sent": sent, "size": len(new),
                "sent_up": sent_up, "sent_down": sent_down}

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
            if report.get("standby") is not None:
                # 别人在派发，干等没意义
                return {"rounds": rounds, "stats": stats, "last": report,
                        "standby": report["standby"]}
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
            "leader": self._leader,
            "mirror": self.mirror or "",
            "auth": {"enabled": self.auth.enabled,
                     "grants": len(self.auth.grants),
                     "admin_tokens": len(self.auth.admin_hashes)},
        }

    def jobs(self, project=None, state=None, limit=None):
        return self.queue.list(project=project, state=state, limit=limit)

    # ------------------------------------------------------------------
    # 对外接口：手机浏览器 / curl 看全程
    # ------------------------------------------------------------------
    def _auth_hook(self, token, name, payload):
        """每个请求的权限判定。异常一律当拒绝（见 wire._hook_denied）。"""
        if name in GET_ACTION:
            return self.auth.allow(token, GET_ACTION[name], payload.get("project"))
        action = RPC_ACTION.get(name, "admin")
        project = None
        if name == "submit":
            plan = (payload or {}).get("plan") or payload or {}
            project = plan.get("project") if isinstance(plan, dict) else None
        elif isinstance(payload, dict):
            project = payload.get("project")
        return self.auth.allow(token, action, project)

    def build_server(self, host="127.0.0.1", port=0, tls=None):
        """把控制面挂成一个 HTTP 服务：``POST /rpc/<方法>`` 给脚本，
        ``GET /`` 给人看。"""
        srv = wire.Server(host=host, port=port, token=self.token, node_id="hub",
                          auth_hook=self._auth_hook, tls=tls)
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
        self.release_leadership()
        self.queue.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def __repr__(self):
        return "Hub(%d nodes, %s)" % (len(self._refs), self.queue.stats())
