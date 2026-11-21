"""节点 agent：把一台机器的文件、能力、算力暴露成 Mesh 端点。

安全模型（三层）
----------------
1. **路径围栏**：每个 root 是一个命名目录，所有访问都经 :func:`jail` 解析。
   用 ``realpath`` 解析后再验证包含关系，所以**符号链接逃逸也会被拦下**。
2. **root 模式**：``ro`` 的 root 直接拒绝任何写操作。
3. **写作用域**：作业执行前后对比改动文件，用 :func:`task.check_scope` 校验。
   越界就报错并列出越界路径 —— 这是防止 agent 乱改文件的关键一层。

令牌
----
所有 ``/rpc/*`` 都要 ``Authorization: Bearer <token>``；``/health`` 不需要，
方便控制面探活。
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
import threading
import time

from . import address, capability, delta, queue, task, wire

__all__ = ["AgentError", "Node", "jail", "serve"]

MAX_READ = 8 << 20          # 单次 read 上限 8MB
MAX_SIG_BLOCKS = 200000     # 签名表最大块数（防内存爆）
MAX_EXEC_OUT = 1 << 18      # 命令输出截断到 256KB
READ_CHUNK = 1 << 20
SKIP_DIRS = (".git", "__pycache__", "node_modules", ".venv")   # 作用域扫描跳过


class AgentError(Exception):
    """节点侧拒绝：路径越界、只读、参数非法等。"""


def jail(base, target):
    """把 ``target`` 关进 ``base`` 里。越界抛 :class:`AgentError`。

    两边都先 ``realpath``，所以 ``..`` 和符号链接都逃不出去。
    """
    base = os.path.realpath(base)
    target = os.path.realpath(target)
    if target == base:
        return target
    if not target.startswith(base + os.sep):
        raise AgentError("路径越界：%s 不在 %s 之内" % (target, base))
    return target


def _sha256_file(path, chunk=READ_CHUNK):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def _parse_addr(addr):
    """把地址统一解析成 MeshAddress，并把地址层错误收敛成 AgentError。

    调用方只该 catch ``AgentError`` 一种异常 —— 否则每加一层校验，
    调用方就得多 catch 一种类型。
    """
    if isinstance(addr, address.MeshAddress):
        return addr
    try:
        return address.parse(addr)
    except address.AddressError as exc:
        raise AgentError("地址不合法：%s" % exc)


class Node(object):
    """一台机器在 Mesh 里的化身。"""

    def __init__(self, node_id, roots=None, token="", name=None, tags=None,
                 slots_total=1, agents=None, models=None, tool_names=None,
                 exec_enabled=False, exec_allow_shell=False, probe_versions=False,
                 runner=None):
        self.node_id = node_id
        self.token = token
        self.name = name or platform.node() or node_id
        self.tags = list(tags or [])
        self.slots_total = int(slots_total)
        self.slots_used = 0
        self.agents = list(agents or ["mock", "cli", "http"])
        self.models = list(models or [])
        self.tool_names = list(tool_names or capability.DEFAULT_TOOLS)
        self.exec_enabled = bool(exec_enabled)
        self.exec_allow_shell = bool(exec_allow_shell)
        self.probe_versions = bool(probe_versions)
        self.runner = runner

        self.roots = {}
        self._cap = None
        self._lock = threading.RLock()
        self.started_at = time.time()

        for item in (roots or []):
            self.add_root(item["name"], item["path"], item.get("mode", "rw"))

    # ------------------------------------------------------------------
    # root 管理
    # ------------------------------------------------------------------
    def add_root(self, name, path, mode="rw"):
        if not name:
            raise AgentError("root 必须有名字")
        if mode not in ("rw", "ro"):
            raise AgentError("root 模式只能是 rw 或 ro，收到 %r" % (mode,))
        abs_path = os.path.realpath(os.path.expanduser(path))
        if not os.path.isdir(abs_path):
            raise AgentError("root 目录不存在：%s" % abs_path)
        with self._lock:
            self.roots[name] = {"path": abs_path, "mode": mode}
            self._cap = None
        return abs_path

    def roots_info(self):
        return [{"name": n, "path": r["path"], "mode": r["mode"]}
                for n, r in sorted(self.roots.items())]

    # ------------------------------------------------------------------
    # 路径解析
    # ------------------------------------------------------------------
    def resolve(self, addr, mode="read"):
        """``mesh://node/env:root/path`` → 本机绝对路径（已过围栏）。"""
        addr = _parse_addr(addr)
        if not addr.root:
            raise AgentError("地址缺少 root：%s" % addr)
        root = self.roots.get(addr.root)
        if root is None:
            raise AgentError("本节点没有 root %r（可用：%s）"
                             % (addr.root, ", ".join(sorted(self.roots)) or "无"))
        if mode == "write" and root["mode"] != "rw":
            raise AgentError("root %r 是只读的" % addr.root)

        base = root["path"]
        if not addr.path:
            return jail(base, base)

        # 先关父目录，再拼文件名 —— 这样"写一个新文件"也能正确校验
        parent = os.path.dirname(addr.path)
        leaf = os.path.basename(addr.path)
        if parent:
            parent = jail(base, os.path.join(base, parent))
        else:
            parent = base
        return jail(base, os.path.join(parent, leaf))

    # ------------------------------------------------------------------
    # 能力
    # ------------------------------------------------------------------
    def capability(self):
        """返回本节点能力快照（静态部分缓存，动态部分每次刷新）。"""
        with self._lock:
            if self._cap is None:
                self._cap = capability.probe(
                    self.node_id, name=self.name, tags=self.tags,
                    roots=self.roots_info(), tools=self.tool_names,
                    slots_total=self.slots_total, slots_used=self.slots_used,
                    agents=self.agents, models=self.models,
                    exec_enabled=self.exec_enabled,
                    exec_allow_shell=self.exec_allow_shell,
                    with_versions=self.probe_versions,
                )
            cap = self._cap

        cap.loadavg = capability._loadavg()
        mem = capability._meminfo()
        if mem:
            cap.mem_total = mem.get("MemTotal", cap.mem_total)
            cap.mem_avail = mem.get("MemAvailable", cap.mem_avail)
        base = next(iter(self.roots.values()))["path"] if self.roots else os.getcwd()
        usage = capability._disk(base)
        cap.disk_total, cap.disk_free = usage["total"], usage["free"]
        cap.slots_used = self.slots_used
        cap.last_seen = time.time()
        return cap

    # ------------------------------------------------------------------
    # 槽位
    # ------------------------------------------------------------------
    def acquire_slot(self):
        with self._lock:
            if self.slots_used >= self.slots_total:
                return False
            self.slots_used += 1
            return True

    def release_slot(self):
        with self._lock:
            if self.slots_used > 0:
                self.slots_used -= 1

    # ------------------------------------------------------------------
    # 文件操作
    # ------------------------------------------------------------------
    def op_ls(self, payload):
        path = self.resolve(payload.get("addr") or "", "read")
        if not os.path.isdir(path):
            raise AgentError("不是目录：%s" % path)
        limit = int(payload.get("limit") or 1000)
        entries = []
        with os.scandir(path) as it:
            for entry in it:
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                entries.append({
                    "name": entry.name,
                    "dir": entry.is_dir(follow_symlinks=False),
                    "link": entry.is_symlink(),
                    "size": st.st_size,
                    "mtime": st.st_mtime,
                })
                if len(entries) >= limit:
                    break
        entries.sort(key=lambda e: (not e["dir"], e["name"]))
        return {"entries": entries, "count": len(entries), "truncated": len(entries) >= limit}

    def op_stat(self, payload):
        path = self.resolve(payload.get("addr") or "", "read")
        if not os.path.exists(path):
            return {"exists": False}
        st = os.stat(path)
        out = {
            "exists": True,
            "dir": os.path.isdir(path),
            "size": st.st_size,
            "mtime": st.st_mtime,
            "mode": oct(st.st_mode & 0o777),
        }
        if payload.get("sha") and os.path.isfile(path):
            out["sha"] = _sha256_file(path)
        return out

    def op_read(self, payload):
        path = self.resolve(payload.get("addr") or "", "read")
        if not os.path.isfile(path):
            raise AgentError("不是文件：%s" % path)
        offset = max(0, int(payload.get("offset") or 0))
        limit = int(payload.get("limit") or -1)
        if limit < 0 or limit > MAX_READ:
            limit = MAX_READ
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(limit)
        return {"data": wire.b64e(data), "size": len(data),
                "eof": len(data) < limit, "total": os.path.getsize(path)}

    def op_write(self, payload):
        path = self.resolve(payload.get("addr") or "", "write")
        data = wire.b64d(payload.get("data") or "")

        # 可选的写作用��校验（纵深防御：root 之外再加一层）
        scope = payload.get("scope") or []
        if scope:
            rel = os.path.relpath(path, self._root_path_for(payload.get("addr")))
            rel = rel.replace(os.sep, "/")
            if not any(task.glob_match(g, rel) for g in scope):
                raise AgentError("写入 %s 超出作业作用域 %s" % (rel, scope))

        expected = payload.get("expected_sha")
        if expected and os.path.isfile(path):
            actual = _sha256_file(path)
            if actual != expected:
                raise AgentError("文件已被改动：期望 %s…，实际 %s…"
                                 % (expected[:12], actual[:12]))

        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".mesh-write-tmp"
        try:
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return {"sha": hashlib.sha256(data).hexdigest(), "size": len(data)}

    def _root_path_for(self, addr):
        addr = _parse_addr(addr)
        root = self.roots.get(addr.root)
        return root["path"] if root else os.getcwd()

    # ------------------------------------------------------------------
    # 差分端点（rsync 流程）
    # ------------------------------------------------------------------
    def op_sig(self, payload):
        """接收方要旧文件的块校验表。"""
        path = self.resolve(payload.get("addr") or "", "read")
        block_size = int(payload.get("block_size") or delta.DEFAULT_BLOCK_SIZE)
        try:
            delta._check_block_size(block_size)
        except delta.DeltaError as exc:
            raise AgentError(str(exc))
        if not os.path.isfile(path):
            return {"sig": [], "old_sha": "", "size": 0}
        size = os.path.getsize(path)
        if size // block_size > MAX_SIG_BLOCKS:
            raise AgentError("文件太大，签名表会超过 %d 块" % MAX_SIG_BLOCKS)
        with open(path, "rb") as f:
            data = f.read()
        return {
            "sig": [list(item) for item in delta.block_signature(data, block_size)],
            "old_sha": hashlib.sha256(data).hexdigest(),
            "size": size,
        }

    def op_delta(self, payload):
        """发送方：只有签名表，产出从接收��旧版到本节点当前版��差分。"""
        path = self.resolve(payload.get("addr") or "", "read")
        if not os.path.isfile(path):
            raise AgentError("不是文件：%s" % path)
        block_size = int(payload.get("block_size") or delta.DEFAULT_BLOCK_SIZE)
        sig = payload.get("sig") or []
        with open(path, "rb") as f:
            new = f.read()
        try:
            d = delta.diff_with_sig(sig, new, block_size,
                                    old_sha=payload.get("old_sha") or "")
        except delta.DeltaError as exc:
            raise AgentError("生成差分失败：%s" % exc)
        return {"delta": d.to_dict(), "estimate": delta.estimate(d)}

    def op_patch(self, payload):
        """接收方：用本地旧文件 + 差分重建。"""
        path = self.resolve(payload.get("addr") or "", "write")
        if not os.path.isfile(path):
            raise AgentError("基线文件不存在：%s" % path)
        try:
            d = delta.Delta.from_dict(payload.get("delta") or {})
            sha = delta.patch_file(path, d, path)
        except delta.DeltaError as exc:
            raise AgentError("应用差分失败：%s" % exc)
        return {"sha": sha, "size": os.path.getsize(path)}

    # ------------------------------------------------------------------
    # 命令执行
    # ------------------------------------------------------------------
    def op_exec(self, payload):
        if not self.exec_enabled:
            raise AgentError("本节点未开启命令执行（exec_enabled=False）")
        cmd = payload.get("cmd") or ""
        if not cmd:
            raise AgentError("缺少 cmd")
        cwd = payload.get("cwd") or ""
        if cwd:
            cwd = self.resolve(cwd, "read")
        else:
            cwd = next(iter(self.roots.values()))["path"] if self.roots else os.getcwd()
        timeout = max(0.1, min(float(payload.get("timeout") or 60), 3600.0))
        try:
            proc = subprocess.run(cmd, shell=True, cwd=cwd, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AgentError("命令超时（%.0fs）" % timeout)
        return {
            "rc": proc.returncode,
            "stdout": proc.stdout[:MAX_EXEC_OUT].decode("utf-8", "replace"),
            "stderr": proc.stderr[:MAX_EXEC_OUT].decode("utf-8", "replace"),
            "truncated": len(proc.stdout) > MAX_EXEC_OUT or len(proc.stderr) > MAX_EXEC_OUT,
        }

    # ------------------------------------------------------------------
    # 作业执行
    # ------------------------------------------------------------------
    def _job_root_name(self, job):
        name = (job.req or {}).get("root")
        if name:
            if name not in self.roots:
                raise AgentError("作业要求的 root %r 不存在（可用：%s）"
                                 % (name, ", ".join(sorted(self.roots)) or "无"))
            return name
        if len(self.roots) == 1:
            return next(iter(self.roots))
        raise AgentError("作业未指定 root，而本节点有多个 root：%s"
                         % ", ".join(sorted(self.roots)))

    def _snapshot(self, job, max_files=20000):
        """root 内每个文件的 ``(mtime_ns, size)`` —— 作业前后的比对基线。

        为什么不用"记下开始时间、事后找 mtime >= 开始时间"这种朴素做法：
        文件系统的 mtime 和 :func:`time.time` 不是同一个精度、甚至会小幅偏移
        （实测能差几毫秒），同一瞬间写出的文件就会被判成"没改过"。
        写作用域校验靠这个清单把关，漏检等于闸门失效，所以改成自己比自己。
        """
        root_name = self._job_root_name(job)
        base = self.roots[root_name]["path"]
        snap = {}
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for name in filenames:
                if len(snap) >= max_files:
                    return snap
                full = os.path.join(dirpath, name)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                rel = os.path.relpath(full, base).replace(os.sep, "/")
                snap[rel] = (st.st_mtime_ns, st.st_size)
        return snap

    def _changed_since(self, job, baseline, max_files=20000):
        """相对 ``baseline`` 快照被新增或改动过的文件（相对 root 路径）。

        注意：**扫整个 root**，不是只扫写作用域目录。否则 agent 写到作用域之外
        的文件根本不会被发现 —— 那样作用域校验就形同虚设。
        代价是每次作业后要走一遍目录树，用 ``max_files`` 兜底防爆。
        """
        root_name = self._job_root_name(job)
        base = self.roots[root_name]["path"]

        changed = []
        seen = 0
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for name in filenames:
                seen += 1
                if seen > max_files:
                    return sorted(changed)
                full = os.path.join(dirpath, name)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                rel = os.path.relpath(full, base).replace(os.sep, "/")
                if baseline.get(rel) != (st.st_mtime_ns, st.st_size):
                    changed.append(rel)
        return sorted(changed)

    def _run_cmd(self, job, timeout=1800.0):
        """跑 ``job.cmd``。

        提示词通过**环境变量和 stdin** 传入，不做字符串拼接 —— 避免提示词里
        的内容被当成 shell 语法执行。
        """
        root_name = self._job_root_name(job)
        cwd = self.roots[root_name]["path"]
        env = dict(os.environ)
        env.update({
            "MESH_JOB_ID": job.id,
            "MESH_PROJECT": job.project,
            "MESH_PROMPT": job.prompt,
            "MESH_FEEDBACK": job.feedback,
            "MESH_READ": ",".join(job.read),
            "MESH_WRITE": ",".join(job.write),
            "MESH_ROOT": cwd,
        })
        try:
            proc = subprocess.run(
                job.cmd, shell=True, cwd=cwd, env=env,
                input=(job.prompt or "").encode("utf-8"),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AgentError("作业超时（%.0fs）" % timeout)
        return {
            "rc": proc.returncode,
            "stdout": proc.stdout[:MAX_EXEC_OUT].decode("utf-8", "replace"),
            "stderr": proc.stderr[:MAX_EXEC_OUT].decode("utf-8", "replace"),
        }

    def op_job(self, payload):
        """跑一条作业，并在落盘后强制校验写作用域。"""
        job = queue.Job.from_dict(payload.get("job") or {})
        if not self.acquire_slot():
            raise AgentError("槽位已满（%d/%d）" % (self.slots_used, self.slots_total))
        baseline = self._snapshot(job)
        try:
            if self.runner is not None:
                out = self.runner(job) or {}
            elif job.cmd:
                out = self._run_cmd(job, timeout=float(payload.get("timeout") or 1800))
            else:
                raise AgentError("作业既没有 cmd，节点也没有配置 runner")

            changed = self._changed_since(job, baseline)
            violations = task.check_scope(job, changed)

            out = dict(out)
            out["changed"] = changed
            out["violations"] = violations
            # runner 自己报的 ok 优先：自定义执行器完全可以不用 rc 表达失败
            # （比如大模型回复"这题我做不了"），一律按 rc==0 判成功会把
            # 失败静默成一个绿色的作业。
            verdict = out.get("ok")
            if verdict is None:
                verdict = out.get("rc", 0) == 0
            out["ok"] = bool(verdict) and not violations
            if violations:
                out["error"] = ("越界写入 %d 个文件（作用域 %s）：%s"
                                "（已写出的文件不会自动回滚）"
                                % (len(violations), job.write, ", ".join(violations[:10])))
            return out
        finally:
            self.release_slot()

    # ------------------------------------------------------------------
    # 服务装配
    # ------------------------------------------------------------------
    def build_server(self, host="127.0.0.1", port=0):
        srv = wire.Server(host=host, port=port, token=self.token, node_id=self.node_id)
        srv.add("cap", lambda payload: self.capability().to_dict())
        srv.add("ls", self.op_ls)
        srv.add("stat", self.op_stat)
        srv.add("read", self.op_read)
        srv.add("write", self.op_write)
        srv.add("sig", self.op_sig)
        srv.add("delta", self.op_delta)
        srv.add("patch", self.op_patch)
        srv.add("exec", self.op_exec)
        srv.add("job", self.op_job)
        return srv

    def __repr__(self):
        return "Node(%s, roots=%s, slots=%d/%d)" % (
            self.node_id, ",".join(sorted(self.roots)) or "-",
            self.slots_used, self.slots_total)


def serve(node, host="127.0.0.1", port=8765):
    """起一个节点服务并阻塞。返回 Server（已启动）。"""
    srv = node.build_server(host=host, port=port)
    srv.start()
    return srv
