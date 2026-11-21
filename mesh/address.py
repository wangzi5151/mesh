"""统一寻址：``mesh://<节点>/<环境>:<根>/<路径>``。

为什么需要它
------------
两个地基仓库各有一套坐标，互不相通：

* linux-file-bridge 用 ``env:root/path`` —— 有"环境"和"根"，没有"节点"。
* ai-swarm 用 ``(project, task)`` —— 有任务，没有路径。

Mesh 里一个节点同时是**文件系统 + 能力清单 + 算力槽位**，所以需要一个三维坐标：
哪个节点、哪个环境、哪个根、哪条路径。

语法
----
::

    mesh://node-a/ubuntu:project/src/main.py     # 完整形式
    mesh://node-a/project/src/main.py            # 省略环境 → 默认环境
    mesh://node-a/project                        # 根本身
    mesh://node-a                                # 节点本身
    mesh://*/ubuntu:project/README.md            # 通配：所有节点

``local`` 是保留节点���，指"本进程所在的那台机器"。
"""

from __future__ import annotations

import re

__all__ = [
    "AddressError", "MeshAddress", "LOCAL_NODE", "WILDCARD_NODE",
    "parse", "normalize_path", "is_valid_name", "matches",
]

SCHEME = "mesh://"
LOCAL_NODE = "local"
WILDCARD_NODE = "*"

# 名字会出现在 mesh:// 地址、镜像目录名和库里。允许 Unicode 词字符 ——
# 中文节点名（"手机"、"笔记本"）是最自然的用法，把它们挡在外面纯属自找麻烦。
# 前半段限制仍然有效：不能以 . 或 - 开头，不能含 / : 这些会破坏地址结构的字符。
_NAME_RE = re.compile(r"^[\w][\w.-]{0,63}$", re.UNICODE)
MAX_PATH = 4096


class AddressError(Exception):
    """地址不合法。"""


def is_valid_name(value):
    """节点/环境/根的名字：词字符开头，可含 ``._-``，最长 64（支持中文）。"""
    return bool(value) and bool(_NAME_RE.match(value))


def _check_name(value, what):
    if value == WILDCARD_NODE and what == "节点":
        return value
    if not is_valid_name(value):
        raise AddressError(
            "%s名不合法：%r（需字母、数字或汉字开头，可含 . _ -，最长 64 字符）"
            % (what, value))
    return value


def normalize_path(path):
    """规范化相对路径：去掉多余斜杠���拒绝逃逸。

    拒绝：绝对路径、``..`` 段、NUL、反斜杠、超长。
    """
    if path is None:
        return ""
    if not isinstance(path, str):
        raise AddressError("路径必须是字符串，收到 %r" % (type(path).__name__,))
    if "\x00" in path:
        raise AddressError("路径含 NUL 字节")
    if "\\" in path:
        raise AddressError("路径含反斜杠（请用 /）：%r" % path)
    if len(path) > MAX_PATH:
        raise AddressError("路径过长（%d > %d）" % (len(path), MAX_PATH))

    parts = []
    for seg in path.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            raise AddressError("路径含 .. 段，拒绝逃逸：%r" % path)
        parts.append(seg)
    return "/".join(parts)


class MeshAddress(object):
    """一个网格坐标。不可变（用 ``_replace`` 派生新对象）。"""

    __slots__ = ("node", "env", "root", "path")

    def __init__(self, node, env="", root="", path=""):
        self.node = _check_name(node, "节点")
        self.env = _check_name(env, "环境") if env else ""
        self.root = _check_name(root, "根") if root else ""
        self.path = normalize_path(path)

    # -- 构造 ------------------------------------------------------------
    def _replace(self, **kw):
        return MeshAddress(
            kw.get("node", self.node),
            kw.get("env", self.env),
            kw.get("root", self.root),
            kw.get("path", self.path),
        )

    @property
    def is_wildcard(self):
        return self.node == WILDCARD_NODE

    @property
    def is_local(self):
        return self.node == LOCAL_NODE

    @property
    def has_root(self):
        return bool(self.root)

    # -- 序列化 ----------------------------------------------------------
    def __str__(self):
        out = SCHEME + self.node
        if self.env:
            out += "/" + self.env + ":" + self.root
        elif self.root:
            out += "/" + self.root
        if self.path:
            out += "/" + self.path
        return out

    def __repr__(self):
        return "MeshAddress(%r)" % str(self)

    def to_dict(self):
        return {"node": self.node, "env": self.env, "root": self.root, "path": self.path}

    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise AddressError("地址必须是对象")
        return cls(obj.get("node") or "", obj.get("env") or "",
                   obj.get("root") or "", obj.get("path") or "")

    def __eq__(self, other):
        if not isinstance(other, MeshAddress):
            return NotImplemented
        return (self.node, self.env, self.root, self.path) == \
               (other.node, other.env, other.root, other.path)

    def __hash__(self):
        return hash((self.node, self.env, self.root, self.path))


def parse(text):
    """解析地址字符串。``mesh://`` 前缀可省略。"""
    if not isinstance(text, str):
        raise AddressError("地址必须是字符串")
    raw = text.strip()
    if not raw:
        raise AddressError("地址为空")
    if raw.lower().startswith(SCHEME):
        raw = raw[len(SCHEME):]
    elif "://" in raw:
        raise AddressError("只支持 mesh:// 协议：%r" % text)

    raw = raw.rstrip("/")
    if not raw:
        raise AddressError("地址缺少节点名：%r" % text)

    node, _, rest = raw.partition("/")
    if not node:
        raise AddressError("地址缺少节点名：%r" % text)

    env = ""
    root = ""
    path = ""
    if rest:
        head, _, tail = rest.partition("/")
        if ":" in head:
            env, _, root = head.partition(":")
            if not env:
                raise AddressError("环境名为空：%r" % text)
            if not root:
                raise AddressError("根名为空：%r" % text)
        else:
            root = head
        path = tail
    return MeshAddress(node, env, root, path)


def matches(pattern, addr):
    """通配匹配：``pattern`` 的节点位可以是 ``*``。

    其余三段（环境/根/路径）必须完全相等；``pattern`` 中留空的段视为"不限"。
    """
    if isinstance(pattern, str):
        pattern = parse(pattern)
    if isinstance(addr, str):
        addr = parse(addr)
    if pattern.node != WILDCARD_NODE and pattern.node != addr.node:
        return False
    if pattern.env and pattern.env != addr.env:
        return False
    if pattern.root and pattern.root != addr.root:
        return False
    if pattern.path and pattern.path != addr.path:
        return False
    return True
