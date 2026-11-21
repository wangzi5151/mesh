"""任务规格、DAG 计划，以及**写作用域强制**。

为什么需要它
------------
ai-swarm 的 DAG 里，每个任务只有 ``read`` / ``write`` 两个 glob 列表，但**没人检查**。
多个 agent 并行跑的时候，两个任务同时改同一个文件就是静默的数据损坏 —— 没有报错，
只有一份被覆盖的代码。

Mesh 把这件事变成硬约束：

1. 计划阶段：:meth:`Plan.conflicts` 找出**没被依赖排序、却写同一片区域**的任务对。
2. 执行阶段：节点 agent 在把改动落盘前调 :func:`check_scope`，越界就整批拒绝。

这是"多台机器上的 agent 并行干活"能安全成立的前提。

路径匹配
--------
用 :func:`glob_match`，支持 ``**`` 跨目录（``fnmatch`` 做不到这点）：:

    glob_match("src/**/*.py", "src/a/b/c.py")   -> True
    glob_match("src/*.py",    "src/a/b.py")     -> False
"""

from __future__ import annotations

import fnmatch

from . import address

__all__ = [
    "PlanError", "TaskSpec", "Plan", "SyncTarget",
    "glob_match", "globs_may_overlap", "check_scope", "in_scope", "static_prefix",
    "merge_sync",
]

MAX_DEPTH = 64          # glob 递归深度上限，防病态模式


class PlanError(Exception):
    """计划不合法（重名、悬空依赖、有环、空计划）。"""


# --------------------------------------------------------------------------
# 路径匹配
# --------------------------------------------------------------------------

def _split(path):
    return [s for s in (path or "").split("/") if s not in ("", ".")]


def _match_segs(pat, segs, depth=0):
    if depth > MAX_DEPTH:
        return False
    if not pat:
        return not segs
    head = pat[0]
    if head == "**":
        # ** 吃�� 0 到 n 段
        for i in range(len(segs) + 1):
            if _match_segs(pat[1:], segs[i:], depth + 1):
                return True
        return False
    if not segs:
        return False
    if not fnmatch.fnmatchcase(segs[0], head):
        return False
    return _match_segs(pat[1:], segs[1:], depth + 1)


def glob_match(pattern, path):
    """glob 匹配，支持 ``**`` 跨目录。"""
    if not pattern:
        return False
    return _match_segs(_split(pattern), _split(path))


def static_prefix(pattern):
    """模式里第一个通配符之前的固定目录段。"""
    out = []
    for seg in _split(pattern):
        if any(ch in seg for ch in "*?["):
            break
        out.append(seg)
    return out


def globs_may_overlap(a, b):
    """两个写 glob 是否**可能**写到同一路径。

    保守判断：宁可误报也不漏报 —— 漏报意味着静默的数据损坏。
    两个都不含通配符时，只有完全相同才算重叠。
    """
    if not a or not b:
        return False
    if a == b:
        return True
    a_wild = any(ch in a for ch in "*?[")
    b_wild = any(ch in b for ch in "*?[")
    if not a_wild and not b_wild:
        return False
    pa, pb = static_prefix(a), static_prefix(b)
    n = min(len(pa), len(pb))
    return pa[:n] == pb[:n]


# --------------------------------------------------------------------------
# 作用域强制
# --------------------------------------------------------------------------

def in_scope(job, path, mode="write"):
    """``path`` 是否落在作业的写（或读）作用域内。"""
    if mode == "write":
        patterns = job.write or []
    else:
        patterns = list(job.write or []) + list(job.read or [])
    return any(glob_match(p, path) for p in patterns)


def check_scope(job, changed_paths, mode="write"):
    """返回**越界**的路径列表。空列表表示全部合规。

    注意这是**事后**判定：节点跑完作业、比对快照，再把整条作业判为失败。
    越界写出去的文件不会自动回滚（agent 是黑盒进程，回滚不了），
    所以 ``op_job`` 会把越界路径全列出来写进 ``error``，由控制面记在
    ``feedback`` 里让下一次 attempt 看到 —— 想真正防住，就把 ``write``
    收紧到最小，并且作业给 1 次尝试。
    """
    if mode == "write":
        patterns = job.write or []
    else:
        patterns = list(job.write or []) + list(job.read or [])
    if not patterns:
        return list(changed_paths)
    return [p for p in changed_paths if not any(glob_match(g, p) for g in patterns)]


# --------------------------------------------------------------------------
# 分发目标
# --------------------------------------------------------------------------
class SyncTarget(object):
    """这条作业改过的文件，除了收回控制面，还要同步到哪个节点的哪个 root。

    产物回收（``hub.collect``）只把文件从节点收回控制面；"A 机器改完自动到
    B 机器"必须显式说出来，所以任务里有 ``sync`` 字段。两种写法::

        "sync": ["mesh://笔记本/work"]                        # 全部改动
        "sync": [{"to": "mesh://树莓派/store", "paths": ["docs/**"]}]

    ``paths`` 是相对路径的 glob 过滤器（:func:`glob_match`），留空表示全部。
    字符串形式里带路径就当过滤器：``mesh://笔记本/work/docs`` 只同步 ``docs/`` 下面。
    """

    __slots__ = ("node", "env", "root", "paths")

    def __init__(self, node, env="", root="", paths=None):
        self.node = node
        self.env = env
        self.root = root
        self.paths = list(paths or [])

    # -- 构造 ------------------------------------------------------------
    @classmethod
    def from_any(cls, obj):
        """字符串 / 对象 / 已解析的目标 → :class:`SyncTarget`（总是返回新对象）。"""
        if isinstance(obj, SyncTarget):
            return cls(obj.node, obj.env, obj.root, list(obj.paths))
        if isinstance(obj, str):
            return cls._from_str(obj)
        if isinstance(obj, dict):
            text = obj.get("to") or obj.get("addr") or ""
            if not text:
                raise PlanError("分发目标缺少 to：%r" % (obj,))
            target = cls._from_str(text)
            if obj.get("paths") is not None:
                target.paths = _clean_paths(obj.get("paths"))
            return target
        raise PlanError("分发目标必须是字符串或对象，收到 %s" % type(obj).__name__)

    @classmethod
    def _from_str(cls, text):
        try:
            addr = address.parse(text)
        except address.AddressError as exc:
            raise PlanError("分发目标地址不合法：%s" % exc)
        if addr.is_wildcard:
            # 通配等于"所有节点"，分发到所有节点没法说清谁该收，挡在计划阶段
            raise PlanError("分发目标不能是通配节点：%r" % (text,))
        if not addr.root:
            raise PlanError("分发目标必须写到 root（mesh://节点/root）：%r" % (text,))
        paths = []
        if addr.path:
            paths = [addr.path, addr.path + "/**"]
        return cls(addr.node, addr.env, addr.root, paths)

    # -- 序列化 ----------------------------------------------------------
    def to_dict(self):
        out = {"to": str(self)}
        if self.paths:
            out["paths"] = list(self.paths)
        return out

    def key(self):
        return (self.node, self.env, self.root)

    def accepts(self, rel):
        """这个相对路径要不要同步过去。"""
        if not self.paths:
            return True
        return any(glob_match(p, rel) for p in self.paths)

    def __str__(self):
        out = address.SCHEME + self.node
        if self.env:
            out += "/" + self.env + ":" + self.root
        else:
            out += "/" + self.root
        return out

    def __repr__(self):
        return "SyncTarget(%s, paths=%s)" % (str(self), self.paths)


def _clean_paths(paths):
    """过滤器列表：允许单个字符串，逐个校验非空。"""
    if isinstance(paths, str):
        paths = [paths]
    if not isinstance(paths, (list, tuple)):
        raise PlanError("分发目标的 paths 必须是列表")
    out = []
    for pat in paths:
        if not isinstance(pat, str) or not pat.strip():
            raise PlanError("分发目标的 paths 里有空模式：%r" % (pat,))
        out.append(pat.strip().lstrip("/"))
    return out


def merge_sync(*groups):
    """把多组分发目标合并去重。

    同一个 ``节点/root`` 写了两次时合并过滤器：任意一处留空（=全部）就按全部算，
    否则取并集。返回的总是新对象，改它不会影响计划里的原值。
    """
    order, by_key = [], {}
    for group in groups:
        for item in group or []:
            target = SyncTarget.from_any(item)
            key = target.key()
            if key not in by_key:
                by_key[key] = target
                order.append(key)
                continue
            current = by_key[key]
            if not current.paths or not target.paths:
                current.paths = []
            else:
                for pat in target.paths:
                    if pat not in current.paths:
                        current.paths.append(pat)
    return [by_key[k] for k in order]


# --------------------------------------------------------------------------
# 任务规格
# --------------------------------------------------------------------------

class TaskSpec(object):
    """一条任务的定义（还没进队列）。"""

    __slots__ = ("id", "title", "prompt", "read", "write", "deps", "verify",
                 "agent", "cmd", "model", "req", "priority", "max_attempts", "sync")

    def __init__(self, task_id="", **kw):
        self.id = task_id
        self.title = kw.get("title") or ""
        self.prompt = kw.get("prompt") or ""
        self.read = list(kw.get("read") or [])
        self.write = list(kw.get("write") or [])
        self.deps = list(kw.get("deps") or [])
        self.verify = kw.get("verify") or ""
        self.agent = kw.get("agent") or ""
        self.cmd = kw.get("cmd") or ""
        self.model = kw.get("model") or ""
        self.req = dict(kw.get("req") or {})
        self.priority = int(kw.get("priority") or 0)
        # 0 表示"用队列的默认值"。会写坏东西的任务该给 1 次，别让它重复踩。
        self.max_attempts = int(kw.get("max_attempts") or 0)
        # 这条作业的产物要同步到哪些节点的哪些 root（收回来之后再推出去）
        self.sync = merge_sync(kw.get("sync"))

    def to_dict(self):
        return {
            "id": self.id, "title": self.title, "prompt": self.prompt,
            "read": self.read, "write": self.write, "deps": self.deps,
            "verify": self.verify, "agent": self.agent, "cmd": self.cmd,
            "model": self.model, "req": self.req, "priority": self.priority,
            "max_attempts": self.max_attempts,
            "sync": [t.to_dict() for t in self.sync],
        }

    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise PlanError("任务必须是对象")
        task_id = obj.get("id") or ""
        if not task_id:
            raise PlanError("任务缺少 id")
        return cls(task_id, **{k: v for k, v in obj.items() if k != "id"})

    def to_job(self, project, default_sync=None):
        """转成队列里的 Job。``default_sync`` 是计划级的分发默认值。"""
        from .queue import Job
        return Job(
            self.id, project, title=self.title, prompt=self.prompt,
            read=self.read, write=self.write, deps=self.deps,
            priority=self.priority, req=self.req, agent=self.agent,
            cmd=self.cmd, model=self.model, verify=self.verify,
            max_attempts=self.max_attempts,
            sync=[t.to_dict() for t in merge_sync(default_sync, self.sync)],
        )

    def __repr__(self):
        return "TaskSpec(%s, deps=%s, write=%s)" % (self.id, self.deps, self.write)


# --------------------------------------------------------------------------
# 计划
# --------------------------------------------------------------------------

class Plan(object):
    """一组任务构成的 DAG。构造后请调 :meth:`validate`。"""

    def __init__(self, project, tasks=None, sync=None):
        self.project = project
        self.tasks = list(tasks or [])
        # 计划级的分发默认值：写一次，所有作业都往这几个 root 推。
        # 和任务自己的 sync 合并（同一目标合并过滤器，见 merge_sync）。
        self.sync = merge_sync(sync)

    # -- 构造 ------------------------------------------------------------
    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise PlanError("计划必须是对象")
        project = obj.get("project") or ""
        if not project:
            raise PlanError("计划缺少 project")
        tasks = [TaskSpec.from_dict(t) for t in (obj.get("tasks") or [])]
        plan = cls(project, tasks, sync=obj.get("sync"))
        plan.validate()
        return plan

    def to_dict(self):
        out = {"project": self.project, "tasks": [t.to_dict() for t in self.tasks]}
        if self.sync:
            out["sync"] = [t.to_dict() for t in self.sync]
        return out

    # -- 校验 ------------------------------------------------------------
    def validate(self):
        """重名 / 悬空依赖 / 有环 / 空计划 —— 任一不满足就抛 PlanError。"""
        if not self.tasks:
            raise PlanError("计划里没有任务")

        seen = set()
        for task in self.tasks:
            if task.id in seen:
                raise PlanError("任务 id 重复：%s" % task.id)
            seen.add(task.id)

        for task in self.tasks:
            if task.id in task.deps:
                raise PlanError("任务 %s 依赖自己" % task.id)
            for dep in task.deps:
                if dep not in seen:
                    raise PlanError("任务 %s 依赖了不存在的 %s" % (task.id, dep))

        # 分发目标在提交前就要能解析：写错了别等到作业跑完、产物要推的时候才发现。
        # （节点在不在线另说 —— 那是运行期的事。）
        try:
            merge_sync(self.sync, *[t.sync for t in self.tasks])
        except PlanError:
            raise
        except Exception as exc:
            raise PlanError("分发目标不合法：%s" % exc)

        self.topo_order()        # 有环会在这里抛
        return self

    def _by_id(self):
        return {t.id: t for t in self.tasks}

    def topo_order(self):
        """拓扑排序（Kahn）。有环抛 PlanError。"""
        by_id = self._by_id()
        indeg = {t.id: 0 for t in self.tasks}
        children = {t.id: [] for t in self.tasks}
        for task in self.tasks:
            for dep in task.deps:
                indeg[task.id] += 1
                children[dep].append(task.id)

        ready = sorted([tid for tid, n in indeg.items() if n == 0])
        order = []
        while ready:
            tid = ready.pop(0)
            order.append(by_id[tid])
            for child in sorted(children[tid]):
                indeg[child] -= 1
                if indeg[child] == 0:
                    ready.append(child)
            ready.sort()

        if len(order) != len(self.tasks):
            stuck = sorted(set(by_id) - {t.id for t in order})
            raise PlanError("计划里有环，涉及：%s" % ", ".join(stuck))
        return order

    def waves(self):
        """按依赖层级分组：同一层可以并行跑。"""
        by_id = self._by_id()
        depth = {}

        def calc(tid, stack):
            if tid in depth:
                return depth[tid]
            if tid in stack:
                raise PlanError("计划里有环，涉及：%s" % tid)
            stack.add(tid)
            deps = by_id[tid].deps
            depth[tid] = 0 if not deps else 1 + max(calc(d, stack) for d in deps)
            stack.discard(tid)
            return depth[tid]

        for task in self.tasks:
            calc(task.id, set())

        out = {}
        for tid, level in depth.items():
            out.setdefault(level, []).append(by_id[tid])
        return [sorted(out[lvl], key=lambda t: t.id) for lvl in sorted(out)]

    # -- 安全检查 --------------------------------------------------------
    def _ancestors(self, task_id):
        """task_id 的所有祖先（含自身）。"""
        by_id = self._by_id()
        seen = set()
        stack = [task_id]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(by_id[cur].deps)
        return seen

    def conflicts(self):
        """找出**没被依赖排序、却可能写同一片区域**的任务对。

        返回 ``[(id_a, id_b, glob_a, glob_b), ...]``。这是并行执行前的安全检查：
        有冲突就必须加依赖边，否则两个 agent 会互相覆盖。
        """
        found = []
        tasks = sorted(self.tasks, key=lambda t: t.id)
        for i, a in enumerate(tasks):
            for b in tasks[i + 1:]:
                if a.id in self._ancestors(b.id) or b.id in self._ancestors(a.id):
                    continue                      # 已��先后顺序，安全
                for ga in a.write:
                    for gb in b.write:
                        if globs_may_overlap(ga, gb):
                            found.append((a.id, b.id, ga, gb))
                            break
                    else:
                        continue
                    break
        return found

    # -- 输出 ------------------------------------------------------------
    def to_jobs(self):
        """转成 Job 列表（按拓扑序）。计划级 sync 在这里并进每条作业。"""
        return [t.to_job(self.project, default_sync=self.sync)
                for t in self.topo_order()]

    def sync_targets(self):
        """计划里出现过的所有分发目标（去重，用于 plan 预览）。"""
        return merge_sync(self.sync, *[t.sync for t in self.tasks])

    def stats(self):
        return {
            "project": self.project,
            "tasks": len(self.tasks),
            "waves": len(self.waves()),
            "edges": sum(len(t.deps) for t in self.tasks),
            "conflicts": len(self.conflicts()),
            "sync": len(self.sync_targets()),
        }

    def __len__(self):
        return len(self.tasks)

    def __repr__(self):
        return "Plan(%s, %d tasks)" % (self.project, len(self.tasks))
