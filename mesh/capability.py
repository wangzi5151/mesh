"""能力注册表：每个节点"能干什么"的可查询清单。

为什么需要它
------------
ai-swarm 只知道节点有几���槽位、用什么 agent；linux-file-bridge 只知道有哪些 root。
两者都回答不了调度真正要问的问题：

* 这台机器装没装 ``aider``？有没有 ``ollama`` 和哪些模型？
* 还剩多少内存和磁盘？现在负载多少？
* 它暴露了哪些 root、哪些是只读的？
* 能不能执行命令？

Mesh 把这些统一成一份**能力清单**，注册到控制面，调度器据此选节点。

零依赖：只用 ``os`` / ``shutil`` / ``subprocess`` / ``/proc``。
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import time

__all__ = [
    "Capability", "Registry", "probe", "probe_tools", "tool_version",
    "score", "DEFAULT_TOOLS", "STALE_TTL",
]

# 默认探测这些工具（存在才记版本）
DEFAULT_TOOLS = (
    "git", "python3", "node", "npm", "aider", "claude", "codex",
    "ollama", "curl", "tar", "gzip", "openssl", "sqlite3", "ffmpeg", "rg", "make",
)

STALE_TTL = 90.0          # 超过这个秒数没心跳就算失联
VERSION_TIMEOUT = 2.0


# --------------------------------------------------------------------------
# 本机探测
# --------------------------------------------------------------------------

def _meminfo():
    """读 /proc/meminfo，返回字节数（非 Linux 或读不到则空字典）。"""
    info = {}
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                key, _, rest = line.partition(":")
                parts = rest.split()
                if parts:
                    try:
                        info[key.strip()] = int(parts[0]) * 1024
                    except ValueError:
                        pass
    except Exception:
        pass
    return info


def _loadavg():
    try:
        return [round(v, 2) for v in os.getloadavg()]
    except Exception:
        return []


def tool_version(name, timeout=VERSION_TIMEOUT):
    """返回工具的版本字符串；没装返回 ``None``；装了但问不出来返回 ``"?"``。"""
    path = shutil.which(name)
    if not path:
        return None
    for flag in ("--version", "-V", "-v"):
        try:
            proc = subprocess.run(
                [path, flag],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                timeout=timeout,
            )
        except Exception:
            continue
        out = (proc.stdout or b"").decode("utf-8", "replace").strip()
        if out:
            return out.splitlines()[0][:120]
    return "?"


def probe_tools(names=None, with_versions=True):
    """探测一批工具，返回 ``{名字: 版本或 "?"}``，未安装的不出现。"""
    result = {}
    for name in (names if names is not None else DEFAULT_TOOLS):
        if not shutil.which(name):
            continue
        result[name] = tool_version(name) if with_versions else "?"
    return result


def _disk(path):
    try:
        usage = shutil.disk_usage(path)
        return {"total": usage.total, "free": usage.free}
    except Exception:
        return {"total": 0, "free": 0}


def probe(node_id, name=None, tags=None, roots=None, tools=None,
          slots_total=1, slots_used=0, agents=None, models=None,
          exec_enabled=False, exec_allow_shell=False, with_versions=True,
          disk_path=None):
    """采集本机能力。

    ``roots`` 是 ``[{name, path, mode}, ...]``；会补上 ``exists`` 字段。
    """
    mem = _meminfo()
    root_list = []
    for item in (roots or []):
        entry = dict(item)
        entry["exists"] = os.path.isdir(entry.get("path") or "")
        root_list.append(entry)

    base = disk_path or (root_list[0]["path"] if root_list else os.getcwd())
    disk = _disk(base)

    return Capability(
        node_id=node_id,
        name=name or platform.node() or node_id,
        tags=list(tags or []),
        os_name=platform.system(),
        os_release=platform.release(),
        arch=platform.machine(),
        python=platform.python_version(),
        cpu_count=os.cpu_count() or 1,
        loadavg=_loadavg(),
        mem_total=mem.get("MemTotal", 0),
        mem_avail=mem.get("MemAvailable", 0),
        disk_total=disk["total"],
        disk_free=disk["free"],
        tools=probe_tools(tools, with_versions),
        agents=list(agents or ["mock", "cli", "http"]),
        models=list(models or []),
        roots=root_list,
        slots_total=int(slots_total),
        slots_used=int(slots_used),
        exec_enabled=bool(exec_enabled),
        exec_allow_shell=bool(exec_allow_shell),
        last_seen=time.time(),
    )


# --------------------------------------------------------------------------
# 能力清单
# --------------------------------------------------------------------------

class Capability(object):
    """一台节点的能力快照。可 JSON 往返。"""

    FIELDS = (
        "node_id", "name", "tags", "os_name", "os_release", "arch", "python",
        "cpu_count", "loadavg", "mem_total", "mem_avail", "disk_total", "disk_free",
        "tools", "agents", "models", "roots", "slots_total", "slots_used",
        "exec_enabled", "exec_allow_shell", "last_seen",
    )

    def __init__(self, node_id="", **kw):
        self.node_id = node_id
        self.name = kw.get("name") or node_id
        self.tags = list(kw.get("tags") or [])
        self.os_name = kw.get("os_name") or ""
        self.os_release = kw.get("os_release") or ""
        self.arch = kw.get("arch") or ""
        self.python = kw.get("python") or ""
        self.cpu_count = int(kw.get("cpu_count") or 1)
        self.loadavg = list(kw.get("loadavg") or [])
        self.mem_total = int(kw.get("mem_total") or 0)
        self.mem_avail = int(kw.get("mem_avail") or 0)
        self.disk_total = int(kw.get("disk_total") or 0)
        self.disk_free = int(kw.get("disk_free") or 0)
        self.tools = dict(kw.get("tools") or {})
        self.agents = list(kw.get("agents") or [])
        self.models = list(kw.get("models") or [])
        self.roots = list(kw.get("roots") or [])
        self.slots_total = int(kw.get("slots_total") or 1)
        self.slots_used = int(kw.get("slots_used") or 0)
        self.exec_enabled = bool(kw.get("exec_enabled"))
        self.exec_allow_shell = bool(kw.get("exec_allow_shell"))
        self.last_seen = float(kw.get("last_seen") or 0.0)

    # -- 派生量 ----------------------------------------------------------
    @property
    def slots_free(self):
        return max(0, self.slots_total - self.slots_used)

    @property
    def load1(self):
        return self.loadavg[0] if self.loadavg else 0.0

    def age(self, now=None):
        return (now if now is not None else time.time()) - self.last_seen

    def is_stale(self, ttl=STALE_TTL, now=None):
        return self.age(now) > ttl

    def root_names(self):
        return [r.get("name") for r in self.roots if r.get("name")]

    def root(self, name):
        for r in self.roots:
            if r.get("name") == name:
                return r
        return None

    def has_tools(self, names):
        return all(n in self.tools for n in (names or []))

    # -- 序列化 ----------------------------------------------------------
    def to_dict(self):
        return {k: getattr(self, k) for k in self.FIELDS}

    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise ValueError("能力清单必须是对象")
        node_id = obj.get("node_id") or ""
        if not node_id:
            raise ValueError("能力清单缺少 node_id")
        return cls(node_id, **{k: v for k, v in obj.items() if k != "node_id"})

    def __repr__(self):
        return "Capability(%s, slots=%d/%d, tools=%d)" % (
            self.node_id, self.slots_used, self.slots_total, len(self.tools))


# --------------------------------------------------------------------------
# 调度打分
# --------------------------------------------------------------------------

def score(cap, req=None, now=None):
    """按任务需求给节点打分。

    返回 ``(合格?, 分数, 原因列表)``。分数越高越优先；不合格时分数为 0。
    """
    req = req or {}
    reasons = []

    if cap.is_stale(now=now):
        return False, 0.0, ["节点失联（%.0fs 无心跳）" % cap.age(now)]

    need_tags = req.get("tags") or []
    missing = [t for t in need_tags if t not in cap.tags]
    if missing:
        reasons.append("缺少标签 %s" % ",".join(missing))

    min_cores = int(req.get("min_cores") or 0)
    if min_cores and cap.cpu_count < min_cores:
        reasons.append("CPU 核数不足（%d < %d）" % (cap.cpu_count, min_cores))

    need_tools = req.get("tools") or []
    lack = [t for t in need_tools if t not in cap.tools]
    if lack:
        reasons.append("缺少工具 %s" % ",".join(lack))

    agent = req.get("agent")
    if agent and cap.agents and agent not in cap.agents:
        reasons.append("不支持 agent 类型 %s" % agent)

    min_mem = int(req.get("min_mem_mb") or 0) * 1024 * 1024
    if min_mem and cap.mem_avail and cap.mem_avail < min_mem:
        reasons.append("可用内存不足")

    min_disk = int(req.get("min_disk_mb") or 0) * 1024 * 1024
    if min_disk and cap.disk_free and cap.disk_free < min_disk:
        reasons.append("可用磁盘不足")

    need_root = req.get("root")
    if need_root and not cap.root(need_root):
        reasons.append("没有 root %s" % need_root)

    if reasons:
        return False, 0.0, reasons

    # 打分：空闲槽位越多越好，负载越低越好
    s = 1.0 + 2.0 * cap.slots_free
    if cap.cpu_count:
        s -= min(1.0, cap.load1 / float(cap.cpu_count))
    if cap.mem_avail and cap.mem_total:
        s += 0.5 * (float(cap.mem_avail) / cap.mem_total)
    return True, round(s, 4), []


# --------------------------------------------------------------------------
# 注册表
# --------------------------------------------------------------------------

class Registry(object):
    """内存里的能力注册表。线程安全由调用方保证（控制面单线程调度）。"""

    def __init__(self, ttl=STALE_TTL):
        self.ttl = float(ttl)
        self._nodes = {}

    # -- 增删改查 --------------------------------------------------------
    def register(self, cap):
        if not isinstance(cap, Capability):
            cap = Capability.from_dict(cap)
        if not cap.node_id:
            raise ValueError("能力清单缺少 node_id")
        self._nodes[cap.node_id] = cap
        return cap

    def update(self, node_id, **fields):
        """局部更新（心跳只带变化量时用）。"""
        cap = self._nodes.get(node_id)
        if cap is None:
            raise KeyError("未注册的节点：%s" % node_id)
        for key, value in fields.items():
            if key in Capability.FIELDS and key != "node_id":
                setattr(cap, key, value)
        cap.last_seen = float(fields.get("last_seen") or time.time())
        return cap

    def touch(self, node_id, now=None):
        cap = self._nodes.get(node_id)
        if cap is None:
            raise KeyError("未注册的节点：%s" % node_id)
        cap.last_seen = now if now is not None else time.time()
        return cap

    def get(self, node_id):
        return self._nodes.get(node_id)

    def remove(self, node_id):
        return self._nodes.pop(node_id, None) is not None

    def __len__(self):
        return len(self._nodes)

    def __contains__(self, node_id):
        return node_id in self._nodes

    def list(self, include_stale=False, now=None):
        caps = list(self._nodes.values())
        if not include_stale:
            caps = [c for c in caps if not c.is_stale(self.ttl, now)]
        return sorted(caps, key=lambda c: c.node_id)

    def stale(self, now=None):
        return [c for c in self._nodes.values() if c.is_stale(self.ttl, now)]

    def prune(self, now=None):
        """移除失联节点，返回被移除的 node_id 列表。"""
        gone = [c.node_id for c in self.stale(now)]
        for node_id in gone:
            self._nodes.pop(node_id, None)
        return gone

    # -- 调�� ------------------------------------------------------------
    def candidates(self, req=None, now=None):
        """返回 ``[(cap, score), ...]``，按分数从高到低，只含合格且有空闲槽位的节点。"""
        req = req or {}
        out = []
        for cap in self._nodes.values():
            if cap.slots_free <= 0:
                continue
            ok, s, _reasons = score(cap, req, now)
            if ok:
                out.append((cap, s))
        out.sort(key=lambda pair: (-pair[1], pair[0].node_id))
        return out

    def pick(self, req=None, now=None):
        """挑一个最合适的节点；没有则返回 ``None``。"""
        cands = self.candidates(req, now)
        return cands[0][0] if cands else None

    def explain(self, req=None, now=None):
        """给"为什么没派出去"一个可读的答复。"""
        req = req or {}
        lines = []
        for cap in sorted(self._nodes.values(), key=lambda c: c.node_id):
            if cap.slots_free <= 0:
                lines.append("%s: 槽位已满（%d/%d）" % (cap.node_id, cap.slots_used, cap.slots_total))
                continue
            ok, s, reasons = score(cap, req, now)
            lines.append("%s: %s" % (cap.node_id, ("合格 score=%.2f" % s) if ok else "；".join(reasons)))
        return lines

    # -- 序列化 ----------------------------------------------------------
    def to_dict(self):
        return {"ttl": self.ttl, "nodes": [c.to_dict() for c in self._nodes.values()]}

    @classmethod
    def from_dict(cls, obj):
        reg = cls(ttl=(obj or {}).get("ttl") or STALE_TTL)
        for item in (obj or {}).get("nodes") or []:
            reg.register(Capability.from_dict(item))
        return reg
