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

import hashlib
import json
import os
import sqlite3
import threading
import time

__all__ = [
    "QueueError", "Job", "Queue",
    "BLOCKED", "QUEUED", "RUNNING", "DONE", "FAILED", "CANCELED",
    "STATES", "TERMINAL", "DEFAULT_LEASE", "DEFAULT_MAX_ATTEMPTS",
    "DELIVERY_PENDING", "DELIVERY_DONE", "DELIVERY_FAILED", "DELIVERY_STATES",
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

# 产物补送的状态（和作业状态机无关，别和上面那几个混用）
DELIVERY_PENDING = "pending"     # 等下一次重试（网络问题、目标暂时离线）
DELIVERY_DONE = "done"           # 送到了
DELIVERY_FAILED = "failed"       # 试够了，或者错误重试也没用（只读 root、根不存在）
DELIVERY_STATES = (DELIVERY_PENDING, DELIVERY_DONE, DELIVERY_FAILED)

DEFAULT_LEASE = 180.0          # 租约秒数
DEFAULT_MAX_ATTEMPTS = 2       # agent 最多试几次
LEADER_LOCK = "scheduler"      # 调度权的锁名
DEFAULT_LEADER_TTL = 15.0      # 领导权租约（秒）；一个 tick 之内要能续上


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
    sync_json       TEXT NOT NULL DEFAULT '[]',
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

-- 调度权：同一份队列允许多个控制面盯着，但只有一个在派发
CREATE TABLE IF NOT EXISTS locks (
    name       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires    REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- 节点清单：控制面重启后不用再喂一遍 --nodes-file
CREATE TABLE IF NOT EXISTS nodes (
    node_id   TEXT PRIMARY KEY,
    url       TEXT NOT NULL,
    token     TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    weight    REAL NOT NULL DEFAULT 1.0,
    added_at  REAL NOT NULL
);

-- 令牌：只存 sha256，明文不落库
CREATE TABLE IF NOT EXISTS tokens (
    token_hash TEXT PRIMARY KEY,
    label      TEXT NOT NULL DEFAULT '',
    project    TEXT NOT NULL DEFAULT '',
    perms_json TEXT NOT NULL DEFAULT '[]',
    created_at REAL NOT NULL
);

-- 补送表：作业已经 done，但产物没送到某个目标节点。
-- 和作业分开记是关键 —— 作业本身成功了，重跑它会把同一份改动做两次；
-- 而"没送到"是另一件事，该单独重试、单独有终态。
CREATE TABLE IF NOT EXISTS deliveries (
    id          TEXT PRIMARY KEY,
    job_id      TEXT NOT NULL,
    project     TEXT NOT NULL DEFAULT '',
    to_addr     TEXT NOT NULL,
    node        TEXT NOT NULL,
    root        TEXT NOT NULL,
    paths_json  TEXT NOT NULL DEFAULT '[]',
    state       TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    next_at     REAL NOT NULL DEFAULT 0,
    last_error  TEXT NOT NULL DEFAULT '',
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries(state, next_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_job ON deliveries(job_id);
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
        "created_at", "updated_at", "started_at", "finished_at", "deps", "sync",
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
        # 产物分发目标（原样存计划里的写法，由 hub 解析成 task.SyncTarget）
        self.sync = list(kw.get("sync") or [])

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
            "sync": self.sync,
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
            sync=_loads(row["sync_json"], []),
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
            self._migrate()
        if self.path != ":memory:":
            # 令牌和节点口令都在这个文件里 —— 别让同组用户顺手读走
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass

    def _migrate(self):
        """给 0.2 及更早的库补列。

        ``CREATE TABLE IF NOT EXISTS`` 对已存在的表一句话都不说，所以老库
        不会自动长出新列 —— 一个 0.2 的 ``mesh.db`` 直接跑 0.3 会在 INSERT 上炸。
        这里按 ``PRAGMA table_info`` 补，不重建表、不动数据。
        """
        with self._lock:
            cols = {r["name"] for r in self._db.execute("PRAGMA table_info(jobs)")}
            if "sync_json" not in cols:
                self._db.execute(
                    "ALTER TABLE jobs ADD COLUMN sync_json TEXT NOT NULL DEFAULT '[]'")
        return True

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
                        result_json, error, created_at, updated_at, started_at,
                        finished_at, sync_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
                        json.dumps(job.sync, ensure_ascii=False),
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

    def update_result(self, job_id, result):
        """只换结果，不动状态（重推产物之后记账用）。

        ``complete`` 会把作业状态和结束时间一起写掉，重推不该改这些 ——
        作业早就成功了，这次只是"产物又送了一遍"。
        """
        now = time.time()
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            cur = self._db.execute(
                "UPDATE jobs SET result_json=?, updated_at=? WHERE id=?",
                (json.dumps(result, ensure_ascii=False) if result is not None else "",
                 now, job_id))
            self._db.execute("COMMIT")
            if cur.rowcount == 0:
                raise QueueError("作业不存在：%s" % job_id)
        self._event("result", job_id, "", {"result": result})
        return True

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
    # 调度权（多控制面）
    # ------------------------------------------------------------------
    def acquire_lock(self, name, holder, ttl=DEFAULT_LEADER_TTL, now=None):
        """抢一把带租约的锁。抢到（或本来就是我持有）返回 True。

        用这个让"多个控制面盯同一份队列"变安全：只有一个在派发，其余的待命。
        租约过期（进程被杀、机器断电）后别人自然能接手，不需要手工清理。
        """
        now = now or time.time()
        expires = now + float(ttl)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                row = self._db.execute(
                    "SELECT holder, expires FROM locks WHERE name=?", (name,)).fetchone()
                if row is None:
                    self._db.execute(
                        "INSERT INTO locks (name, holder, expires, updated_at) VALUES (?,?,?,?)",
                        (name, holder, expires, now))
                    ok = True
                elif row["holder"] == holder or row["expires"] <= now:
                    self._db.execute(
                        "UPDATE locks SET holder=?, expires=?, updated_at=? WHERE name=?",
                        (holder, expires, now, name))
                    ok = True
                else:
                    ok = False
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        return ok

    def renew_lock(self, name, holder, ttl=DEFAULT_LEADER_TTL, now=None):
        """续租。已经不是我持有的（被别人抢走了）返回 False。"""
        now = now or time.time()
        with self._lock:
            cur = self._db.execute(
                "UPDATE locks SET expires=?, updated_at=? WHERE name=? AND holder=?",
                (now + float(ttl), now, name, holder))
            return cur.rowcount > 0

    def release_lock(self, name, holder):
        with self._lock:
            cur = self._db.execute(
                "DELETE FROM locks WHERE name=? AND holder=?", (name, holder))
            return cur.rowcount > 0

    def lock_holder(self, name, now=None):
        """谁现在持有锁（空字符串表示没人）。"""
        now = now if now is not None else time.time()
        with self._lock:
            row = self._db.execute(
                "SELECT holder, expires FROM locks WHERE name=?", (name,)).fetchone()
        if row is None or row["expires"] <= now:
            return ""
        return row["holder"]

    # ------------------------------------------------------------------
    # 节点清单
    # ------------------------------------------------------------------
    def save_node(self, node_id, url, token="", tags=None, weight=1.0):
        now = time.time()
        with self._lock:
            self._db.execute(
                """INSERT INTO nodes (node_id, url, token, tags_json, weight, added_at)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(node_id) DO UPDATE SET
                       url=excluded.url, token=excluded.token,
                       tags_json=excluded.tags_json, weight=excluded.weight""",
                (node_id, url, token or "",
                 json.dumps(list(tags or []), ensure_ascii=False),
                 float(weight), now))

    def load_nodes(self):
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM nodes ORDER BY node_id").fetchall()
        return [{
            "node_id": r["node_id"], "url": r["url"], "token": r["token"],
            "tags": _loads(r["tags_json"], []), "weight": r["weight"],
        } for r in rows]

    def delete_node(self, node_id):
        with self._lock:
            cur = self._db.execute("DELETE FROM nodes WHERE node_id=?", (node_id,))
            return cur.rowcount > 0

    def clear_nodes(self):
        with self._lock:
            cur = self._db.execute("DELETE FROM nodes")
            return cur.rowcount

    # ------------------------------------------------------------------
    # 令牌（只存哈希）
    # ------------------------------------------------------------------
    def add_token(self, token, perms, project="", label=""):
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock:
            self._db.execute(
                """INSERT INTO tokens (token_hash, label, project, perms_json, created_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(token_hash) DO UPDATE SET
                       label=excluded.label, project=excluded.project,
                       perms_json=excluded.perms_json""",
                (digest, label or "", project or "",
                 json.dumps(sorted(set(perms or [])), ensure_ascii=False), time.time()))
        return digest

    def load_tokens(self):
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM tokens ORDER BY created_at").fetchall()
        return [{
            "token_hash": r["token_hash"], "label": r["label"],
            "project": r["project"], "perms": _loads(r["perms_json"], []),
        } for r in rows]

    def delete_token(self, label=None, token_hash=None):
        with self._lock:
            if token_hash:
                cur = self._db.execute(
                    "DELETE FROM tokens WHERE token_hash=?", (token_hash,))
            elif label:
                cur = self._db.execute("DELETE FROM tokens WHERE label=?", (label,))
            else:
                raise QueueError("要删令牌就得给 label 或 token_hash")
            return cur.rowcount > 0

    # ------------------------------------------------------------------
    # 产物补送表
    # ------------------------------------------------------------------
    def record_delivery(self, delivery_id, job_id, project, to_addr, node, root,
                        paths=None, state=DELIVERY_PENDING, error="",
                        attempts=0, next_at=0.0, now=None):
        """记下一个"作业已 done、产物送没送到"的行。同一个 id 重复写就更新。

        写入即覆盖：重推（``mesh sync``）会把 attempts 和 next_at 一起清掉，
        所以人工介入之后补送的下一次重试是立刻发生的，不用等退避。
        """
        now = now or time.time()
        with self._lock:
            self._db.execute(
                """INSERT INTO deliveries (
                       id, job_id, project, to_addr, node, root, paths_json,
                       state, attempts, next_at, last_error, created_at, updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                       to_addr=excluded.to_addr, node=excluded.node, root=excluded.root,
                       paths_json=excluded.paths_json, state=excluded.state,
                       attempts=excluded.attempts, next_at=excluded.next_at,
                       last_error=excluded.last_error, updated_at=excluded.updated_at""",
                (delivery_id, job_id, project or "", to_addr, node, root,
                 json.dumps(list(paths or []), ensure_ascii=False), state,
                 int(attempts), float(next_at), error or "", now, now))
        return delivery_id

    def due_deliveries(self, now=None, limit=10):
        """到点该补送的行（按 next_at 先后）。"""
        now = now or time.time()
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM deliveries WHERE state=? AND next_at <= ? "
                "ORDER BY next_at ASC, id ASC LIMIT ?",
                (DELIVERY_PENDING, now, int(limit))).fetchall()
        return [self._delivery_row(r) for r in rows]

    def update_delivery(self, delivery_id, state=None, error=None,
                        attempts=None, next_at=None, now=None):
        now = now or time.time()
        sets, args = ["updated_at=?"], [now]
        for column, value in (("state", state), ("last_error", error),
                              ("attempts", attempts), ("next_at", next_at)):
            if value is None:
                continue
            sets.append("%s=?" % column)
            args.append(float(value) if column == "next_at" else
                        int(value) if column == "attempts" else value)
        args.append(delivery_id)
        with self._lock:
            cur = self._db.execute(
                "UPDATE deliveries SET %s WHERE id=?" % ", ".join(sets), args)
        return cur.rowcount > 0

    def deliveries(self, job_id=None, state=None, limit=None):
        sql = "SELECT * FROM deliveries WHERE 1=1"
        args = []
        if job_id:
            sql += " AND job_id=?"
            args.append(job_id)
        if state:
            sql += " AND state=?"
            args.append(state)
        sql += " ORDER BY created_at ASC, id ASC"
        if limit:
            sql += " LIMIT ?"
            args.append(int(limit))
        with self._lock:
            rows = self._db.execute(sql, args).fetchall()
        return [self._delivery_row(r) for r in rows]

    def forget_deliveries(self, job_id):
        """删掉一条作业的补送记录（换目标重推之前先清干净）。"""
        with self._lock:
            cur = self._db.execute("DELETE FROM deliveries WHERE job_id=?", (job_id,))
        return cur.rowcount

    def delivery_stats(self):
        with self._lock:
            rows = self._db.execute(
                "SELECT state, COUNT(*) AS cnt FROM deliveries GROUP BY state").fetchall()
        return {r["state"]: r["cnt"] for r in rows}

    @staticmethod
    def _delivery_row(row):
        return {
            "id": row["id"], "job_id": row["job_id"], "project": row["project"],
            "to": row["to_addr"], "node": row["node"], "root": row["root"],
            "paths": _loads(row["paths_json"], []), "state": row["state"],
            "attempts": row["attempts"], "next_at": row["next_at"],
            "error": row["last_error"], "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------
    # 查询帮助函数
    # ------------------------------------------------------------------
    def stats(self):
        """返回每个状态的计数，用于监控面板。"""
        with self._lock:
            rows = self._db.execute(
                "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state").fetchall()
        return {r["state"]: r["cnt"] for r in rows}
