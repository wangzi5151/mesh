"""持久作业队列：SQLite WAL + 租约 + 崩溃恢复。

为什么需要它
------------
ai-swarm 的调度状态是「内存字典 + 每 3 秒整体落盘 JSON」。hub 崩了就丢最近 3 秒，
重启后还得把 running 任务手工重置。在"手机后台跑一整晚"的场景里这不够硬。

Mesh 的控制面必须能：断电重启接着跑、多节点并发安全、历史可查。
所以用 SQLite（标准库自带）做真相源：

* ``journal_mode=WAL`` —— 读写不互斥，崩溃不丢已提交事务
* ``BEGIN IMMEDIATE`` —— 领任务先拿写锁，两个节点绝不会抢到同一个任务
* 租约（lease）—— 节点掉线，任务到期自动回队列，不卡住整个项目
* 租约到期回收不额外惩罚重试次数（``reap`` 不改 ``attempts``），
  但已发生的那次尝试仍计数，所以重试总数始终有上界

状态机
------
::

    add ──────────────► queued ──lease──► running ──complete──► done
                          ▲                 │
                          │  fail(可重试)    │  fail(重试用尽)
                          └─────────────────┤
                          ▲                 └──────────────► failed ──► 下游 blocked
                          │  reap(租约到期，不扣重试)
                          └───────────────────────────────────────────┘

``blocked`` 表示"上游永久失败，等人工处理"；``retry`` 可以把 failed 拉回 queued
并把下游 blocked 一并解锁。

注意：``queued`` 只表示"可以排队"；是否真的能派发由 :meth:`Queue.ready` 判断
（要求所有依赖都已 done）。这样插入 DAG 时不必关心拓扑顺序。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

__all__ = [
    "QueueError", "Job", "Queue",
    "BLOCKED", "QUEUED", "RUNNING", "DONE", "FAILED", "CANCELED",
    "STATES", "TERMINAL", "DEFAULT_LEASE", "DEFAULT_MAX_ATTEMPTS",
]

# -- 状态 ----------------------------------------------------------------
BLOCKED = "blocked"
QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELED = "canceled"

STATES = (BLOCKED, QUEUED, RUNNING, DONE, FAILED, CANCELED)
TERMINAL = (DONE, FAILED, CANCELED)

DEFAULT_LEASE = 180.0          # 租约秒数
DEFAULT_MAX_ATTEMPTS = 2       # agent 最多试几次


class QueueError(Exception):
    """队列层的错误（非法状态迁移、缺字段等）。"""


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    project         TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    prompt          TEXT NOT NULL DEFAULT '',
    read_json       TEXT NOT NULL DEFAULT '[]',
    write_json      TEXT NOT NULL DEFAULT '[]',
    priority        INTEGER NOT NULL DEFAULT 0,
    req_json        TEXT NOT NULL DEFAULT '{}',
    agent           TEXT NOT NULL DEFAULT '',
    cmd             TEXT NOT NULL DEFAULT '',
    model           TEXT NOT NULL DEFAULT '',
    verify          TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT 'queued',
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 2,
    lease_owner     TEXT NOT NULL DEFAULT '',
    lease_expires   REAL NOT NULL DEFAULT 0,
    feedback        TEXT NOT NULL DEFAULT '',
    result_json     TEXT NOT NULL DEFAULT '',
    error           TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    started_at      REAL NOT NULL DEFAULT 0,
    finished_at     REAL NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_state    ON jobs(state, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_project  ON jobs(project, state);
CREATE INDEX IF NOT EXISTS idx_jobs_owner    ON jobs(lease_owner, state);
CREATE INDEX IF NOT EXISTS idx_jobs_expires  ON jobs(state, lease_expires);

-- 依赖边：job_id 依赖 dep_id（父）
CREATE TABLE IF NOT EXISTS deps (
    job_id  TEXT NOT NULL,
    dep_id  TEXT NOT NULL,
    PRIMARY KEY (job_id, dep_id)
);

CREATE INDEX IF NOT EXISTS idx_deps_dep ON deps(dep_id);

CREATE TABLE IF NOT EXISTS events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL NOT NULL,
    job_id       TEXT NOT NULL DEFAULT '',
    project      TEXT NOT NULL DEFAULT '',
    kind         TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_ts  ON events(ts);
"""


def _loads(text, default):
    if not text:
        return default
    try:
        value = json.loads(text)
    except (ValueError, TypeError):
        return default
    return value if isinstance(value, type(default)) else default


class Job(object):
    """一条作业。``deps`` 是父作业 id 列表；``read``/``write`` 是 glob 列表。"""

    __slots__ = (
        "id", "project", "title", "prompt", "read", "write", "priority", "req",
        "agent", "cmd", "model", "verify", "state", "attempts", "max_attempts",
        "lease_owner", "lease_expires", "feedback", "result", "error",
        "created_at", "updated_at", "started_at", "finished_at", "deps",
    )

    def __init__(self, job_id="", project="", **kw):
        self.id = job_id
        self.project = project
        self.title = kw.get("title") or ""
        self.prompt = kw.get("prompt") or ""
        self.read = list(kw.get("read") or [])
        self.write = list(kw.get("write") or [])
        self.priority = int(kw.get("priority") or 0)
        self.req = dict(kw.get("req") or {})
        self.agent = kw.get("agent") or ""
        self.cmd = kw.get("cmd") or ""
        self.model = kw.get("model") or ""
        self.verify = kw.get("verify") or ""
        self.state = kw.get("state") or QUEUED
        self.attempts = int(kw.get("attempts") or 0)
        self.max_attempts = int(kw.get("max_attempts") or DEFAULT_MAX_ATTEMPTS)
        self.lease_owner = kw.get("lease_owner") or ""
        self.lease_expires = float(kw.get("lease_expires") or 0.0)
        self.feedback = kw.get("feedback") or ""
        self.result = kw.get("result")
        self.error = kw.get("error") or ""
        self.created_at = float(kw.get("created_at") or 0.0)
        self.updated_at = float(kw.get("updated_at") or 0.0)
        self.started_at = float(kw.get("started_at") or 0.0)
        self.finished_at = float(kw.get("finished_at") or 0.0)
        self.deps = list(kw.get("deps") or [])

    # -- 派生 ------------------------------------------------------------
    @property
    def is_terminal(self):
        return self.state in TERMINAL

    def scope(self):
        """这个作业允许改的路径集合（write 并上 read）。"""
        return set(self.write) | set(self.read)

    def to_dict(self):
        return {
            "id": self.id, "project": self.project, "title": self.title,
            "prompt": self.prompt, "read": self.read, "write": self.write,
            "deps": self.deps, "priority": self.priority, "req": self.req,
            "agent": self.agent, "cmd": self.cmd, "model": self.model,
            "verify": self.verify, "state": self.state,
            "attempts": self.attempts, "max_attempts": self.max_attempts,
            "lease_owner": self.lease_owner, "lease_expires": self.lease_expires,
            "feedback": self.feedback, "result": self.result, "error": self.error,
            "created_at": self.created_at, "updated_at": self.updated_at,
            "started_at": self.started_at, "finished_at": self.finished_at,
        }

    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise QueueError("作业必须是对象")
        job_id = obj.get("id") or ""
        if not job_id:
            raise QueueError("作业缺少 id")
        data = {k: v for k, v in obj.items() if k not in ("id", "project")}
        return cls(job_id, obj.get("project") or "", **data)

    @classmethod
    def from_row(cls, row, deps=None):
        return cls(
            row["id"], row["project"],
            title=row["title"], prompt=row["prompt"],
            read=_loads(row["read_json"], []),
            write=_loads(row["write_json"], []),
            priority=row["priority"],
            req=_loads(row["req_json"], {}),
            agent=row["agent"], cmd=row["cmd"], model=row["model"],
            verify=row["verify"], state=row["state"],
            attempts=row["attempts"], max_attempts=row["max_attempts"],
            lease_owner=row["lease_owner"], lease_expires=row["lease_expires"],
            feedback=row["feedback"],
            result=_loads(row["result_json"], {}) if row["result_json"] else None,
            error=row["error"],
            created_at=row["created_at"], updated_at=row["updated_at"],
            started_at=row["started_at"], finished_at=row["finished_at"],
            deps=deps or [],
        )

    def __repr__(self):
        return "Job(%s, %s/%s, %s, %d/%d)" % (
            self.id, self.project, self.state, self.title[:20],
            self.attempts, self.max_attempts)


class Queue(object):
    """线程安全的持久队列。同一个文件只应有一个写者（控制面进程）。"""

    def __init__(self, path, lease=DEFAULT_LEASE, max_attempts=DEFAULT_MAX_ATTEMPTS):
        self.path = path
        # 注意：租约秒数必须叫 lease_secs —— 叫 lease 会被实例属性遮住
        # 下面的 lease() 方法，调度器就永远派不出作业（曾经真踩过）。
        self.lease_secs = float(lease)
        self.max_attempts = int(max_attempts)
        self._lock = threading.RLock()

        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            if parent:
                os.makedirs(parent, exist_ok=True)

        self._db = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False, timeout=30.0)
        self._db.row_factory = sqlite3.Row
        self._setup()

    # -- 生命周期 --------------------------------------------------------
    def _setup(self):
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.execute("PRAGMA busy_timeout=30000")
            self._db.executescript(SCHEMA)

    def close(self):
        with self._lock:
            try:
                self._db.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # -- 写：入队 --------------------------------------------------------
    def add(self, job):
        """插入一条作业。返回落库后的 Job（含 created_at）。"""
        if not isinstance(job, Job):
            job = Job.from_dict(job)
        if not job.id:
            raise QueueError("作业缺少 id")
        if not job.project:
            raise QueueError("作业缺少 project")
        if job.state not in STATES:
            raise QueueError("非法状态：%r" % (job.state,))

        now = time.time()
        job.created_at = job.created_at or now
        job.updated_at = now
        if not job.max_attempts:
            job.max_attempts = self.max_attempts

        with self._lock:
            try:
                self._db.execute(
                    """INSERT INTO jobs (
                        id, project, title, prompt, read_json, write_json, priority,
                        req_json, agent, cmd, model, verify, state, attempts,
                        max_attempts, lease_owner, lease_expires, feedback,
                        result_json, error, created_at, updated_at, started_at, finished_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job.id, job.project, job.title, job.prompt,
                        json.dumps(job.read, ensure_ascii=False),
                        json.dumps(job.write, ensure_ascii=False),
                        job.priority,
                        json.dumps(job.req, ensure_ascii=False),
                        job.agent, job.cmd, job.model, job.verify, job.state,
                        job.attempts, job.max_attempts, job.lease_owner,
                        job.lease_expires, job.feedback,
                        json.dumps(job.result, ensure_ascii=False) if job.result is not None else "",
                        job.error, job.created_at, job.updated_at,
                        job.started_at, job.finished_at,
                    ))
                for dep in job.deps:
                    self._db.execute(
                        "INSERT OR IGNORE INTO deps (job_id, dep_id) VALUES (?,?)",
                        (job.id, dep))
            except sqlite3.IntegrityError as exc:
                raise QueueError("作业已存在或依赖非法：%s" % exc)
        self._event("add", job.id, job.project, {"state": job.state})
        return job

    def add_many(self, jobs):
        """批量入队。按给定顺序插入，依赖可以先于父作业出现。"""
        added = []
        for job in jobs:
            added.append(self.add(job))
        return added

    # -- 读 --------------------------------------------------------------
    def get(self, job_id):
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            deps = [r["dep_id"] for r in self._db.execute(
                "SELECT dep_id FROM deps WHERE job_id=? ORDER BY dep_id", (job_id,))]
        return Job.from_row(row, deps)

    def list(self, project=None, state=None, limit=None):
        sql = "SELECT * FROM jobs WHERE 1=1"
        args = []
        if project:
            sql += " AND project=?"
            args.append(project)
        if state:
            sql += " AND state=?"
            args.append(state)
        sql += " ORDER BY priority DESC, created_at ASC"
        if limit:
            sql += " LIMIT ?"
            args.append(int(limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
            deps = self._deps_map([r["id"] for r in rows])
        return [Job.from_row(r, deps.get(r["id"], [])) for r in rows]

    def ready(self, project=None, now=None):
        """可以立刻派发的作业：state=queued 且所有依赖都已 done。"""
        sql = """
            SELECT j.* FROM jobs j
            WHERE j.state = 'queued'
              AND NOT EXISTS (
                  SELECT 1 FROM deps d
                  JOIN jobs p ON p.id = d.dep_id
                  WHERE d.job_id = j.id AND p.state <> 'done'
              )
        """
        args = []
        if project:
            sql += " AND j.project = ?"
            args.append(project)
        sql += " ORDER BY j.priority DESC, j.created_at ASC"
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
            deps = self._deps_map([r["id"] for r in rows])
        return [Job.from_row(r, deps.get(r["id"], [])) for r in rows]

    def dependents(self, job_id):
        """直接依赖 job_id 的作业（子）。"""
        with self._lock:
            return [r["job_id"] for r in self._db.execute(
                "SELECT job_id FROM deps WHERE dep_id=? ORDER BY job_id", (job_id,))]

    def _deps_map(self, ids):
        if not ids:
            return {}
        marks = ",".join("?" * len(ids))
        out = {}
        for row in self._db.execute(
                "SELECT job_id, dep_id FROM deps WHERE job_id IN (%s) ORDER BY dep_id" % marks,
                list(ids)):
            out.setdefault(row["job_id"], []).append(row["dep_id"])
        return out

    def __len__(self):
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # ------------------------------------------------------------------
    # 事件日志
    # ------------------------------------------------------------------
    def _event(self, kind, job_id="", project="", payload=None):
        payload = payload or {}
        ts = time.time()
        with self._lock:
            self._db.execute(
                "INSERT INTO events (ts, job_id, project, kind, payload_json) VALUES (?,?,?,?,?)",
                (ts, job_id, project, kind, json.dumps(payload, ensure_ascii=False)))

    # ------------------------------------------------------------------
    # 租约与调度
    # ------------------------------------------------------------------
    def _claim(self, row, owner, now):
        """把已选中的作业置为 running 并加租约（调用方须持有写事务）。"""
        job_id = row["id"]
        lease_expires = now + self.lease_secs
        self._db.execute(
            "UPDATE jobs SET state=?, lease_owner=?, lease_expires=?, started_at=?, attempts=attempts+1, updated_at=? WHERE id=?",
            (RUNNING, owner, lease_expires, now, now, job_id))
        return job_id, lease_expires

    def lease_one(self, owner, project=None, now=None):
        """尝试领一个就绪作业（state=queued 且依赖已完成）。

        成功返回 Job（state 已改为 running），否则返回 None。
        """
        now = now or time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            # 选取第一条满足条件的作业
            sql = """
                SELECT j.* FROM jobs j
                WHERE j.state = 'queued'
                  AND NOT EXISTS (
                      SELECT 1 FROM deps d
                      JOIN jobs p ON p.id = d.dep_id
                      WHERE d.job_id = j.id AND p.state <> 'done'
                  )
            """
            args = []
            if project:
                sql += " AND j.project = ?"
                args.append(project)
            sql += " ORDER BY j.priority DESC, j.created_at ASC LIMIT 1"
            row = self._db.execute(sql, args).fetchone()
            if row is None:
                self._db.execute("COMMIT")
                return None
            job_id, lease_expires = self._claim(row, owner, now)
            self._db.execute("COMMIT")
            # 读取完整作业对象（包括 deps）
            job = self.get(job_id)
            self._event("lease", job_id, job.project, {"owner": owner, "expires": lease_expires})
            return job

    def lease(self, job_id, owner, now=None):
        """领**指定**的作业。作业不在 queued、或依赖未完成，返回 None。

        调度器需要这个：它得先按能力挑出节点，再决定派哪条作业 ——
        反过来（先领作业再找节点）会让"没有节点能跑"的作业占住租约，
        把整个队列卡死。
        """
        now = now or time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute("""
                SELECT j.* FROM jobs j
                WHERE j.id = ?
                  AND j.state = 'queued'
                  AND NOT EXISTS (
                      SELECT 1 FROM deps d
                      JOIN jobs p ON p.id = d.dep_id
                      WHERE d.job_id = j.id AND p.state <> 'done'
                  )
            """, (job_id,)).fetchone()
            if row is None:
                self._db.execute("COMMIT")
                return None
            claimed, lease_expires = self._claim(row, owner, now)
            self._db.execute("COMMIT")
            job = self.get(claimed)
            self._event("lease", claimed, job.project,
                        {"owner": owner, "expires": lease_expires})
            return job

    def complete(self, job_id, result=None, feedback=None):
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute(
                "UPDATE jobs SET state=?, result_json=?, feedback=?, finished_at=?, lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                (DONE, json.dumps(result, ensure_ascii=False) if result is not None else "",
                 feedback or "", now, now, job_id))
            self._db.execute("COMMIT")
        self._event("complete", job_id, "", {"result": result, "feedback": feedback})

    def fail(self, job_id, error=None, feedback=None, retryable=True):
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute("SELECT attempts, max_attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                self._db.execute("COMMIT")
                raise QueueError("作业不存在：%s" % job_id)
            attempts, max_attempts = row["attempts"], row["max_attempts"]
            if retryable and attempts < max_attempts:
                # 回到 queued：attempts 已在 lease 时计过，这里不再加。
                # 失败原因写进 feedback，下一次 attempt 会带着它重跑。
                self._db.execute(
                    "UPDATE jobs SET state=?, error=?, feedback=?, lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                    (QUEUED, error or "", feedback or error or "", now, job_id))
                new_state = QUEUED
            else:
                self._db.execute(
                    "UPDATE jobs SET state=?, error=?, feedback=?, lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                    (FAILED, error or "", feedback or "", now, job_id))
                new_state = FAILED
            self._db.execute("COMMIT")
        self._event("fail", job_id, "", {"error": error, "state": new_state})
        return new_state

    def cancel(self, job_id, reason=None):
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute(
                "UPDATE jobs SET state=?, feedback=?, lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                (CANCELED, reason or "", now, job_id))
            self._db.execute("COMMIT")
        self._event("cancel", job_id, "", {"reason": reason})

    def reap(self, now=None):
        """把租约过期的 running 作业放回队列（不增加 attempts）。"""
        now = now or time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            cur = self._db.execute(
                "SELECT id FROM jobs WHERE state='running' AND lease_expires <= ?", (now,))
            ids = [r["id"] for r in cur]
            for job_id in ids:
                self._db.execute(
                    "UPDATE jobs SET state='queued', lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                    (now, job_id))
                self._event("reap", job_id, "", {"now": now})
            self._db.execute("COMMIT")
        return ids

    def unlease(self, job_id, reason="", now=None):
        """把一个 running 作业退回队列，**不消耗重试次数**。

        用于派发阶段的基础设施故障（节点连不上、槽位已满）。作业根本没跑起来，
        不该算它失败。但 ``attempts`` 也不回退 —— 否则节点反复掉线时作业会
        无限重试，把队列变成死循环。
        """
        now = now or time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                self._db.execute("COMMIT")
                raise QueueError("作业不存在：%s" % job_id)
            if row["state"] != RUNNING:
                self._db.execute("COMMIT")
                return False
            self._db.execute(
                "UPDATE jobs SET state=?, lease_owner='', lease_expires=0, error=?, updated_at=? WHERE id=?",
                (QUEUED, reason or "", now, job_id))
            self._db.execute("COMMIT")
        self._event("unlease", job_id, "", {"reason": reason})
        return True

    def retry(self, job_id, reset_attempts=False):
        """把 failed / blocked 作业重新放入队列。"""
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                self._db.execute("COMMIT")
                raise QueueError("作业不存在：%s" % job_id)
            if row["state"] not in (FAILED, BLOCKED):
                self._db.execute("COMMIT")
                raise QueueError("只能重试 failed/blocked 作业")
            attempts = 0 if reset_attempts else None
            if reset_attempts:
                self._db.execute(
                    "UPDATE jobs SET state='queued', attempts=0, lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                    (now, job_id))
            else:
                self._db.execute(
                    "UPDATE jobs SET state='queued', lease_owner='', lease_expires=0, updated_at=? WHERE id=?",
                    (now, job_id))
            self._db.execute("COMMIT")
        self._event("retry", job_id, "", {"reset_attempts": reset_attempts})

    def block(self, job_id, reason=None):
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            self._db.execute(
                "UPDATE jobs SET state='blocked', feedback=?, updated_at=? WHERE id=?",
                (reason or "", now, job_id))
            self._db.execute("COMMIT")
        self._event("block", job_id, "", {"reason": reason})

    # ------------------------------------------------------------------
    # 查询帮助函数
    # ------------------------------------------------------------------
    def stats(self):
        """返回每个状态的计数，用于监控面板。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state").fetchall()
        return {r["state"]: r["cnt"] for r in rows}
