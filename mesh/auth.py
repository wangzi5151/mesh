"""令牌与权限。

为什么不是"一个口令走天下"
--------------------------
一台控制面后面通常有好几个项目（写作、代码、孩子作业），每个项目该有自己的一把钥匙：
给孩子作业那把钥匙不该能提交改代码仓库的作业，也不该能取消别人的作业。

所以令牌是 **令牌 → (项目, 权限集)** 的映射，不是全局开关。
数据库里只存 sha256，明文只在你创建它的那一秒出现过。

权限
----
========  ==========================================
``read``  看状态、列作业、看面板
``submit``提交计划
``cancel``取消 / 重试作业
``admin`` 管理令牌、接入节点（隐含上面全部）
========  ==========================================

项目为空字符串表示"所有项目"。没有配置任何令牌时，控制面处于**开发模式**：
谁都能连（局域网自用场景），但 ``status`` 和面板会明确告诉你是开着门的。
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

__all__ = [
    "PERMS", "IMPLIED", "normalize", "hash_token", "new_token",
    "Grant", "Auth", "ANY_PROJECT",
]

PERMS = ("read", "submit", "cancel", "admin")
ANY_PROJECT = ""

# admin 蕴含其它权限；反过来不行
IMPLIED = {
    "admin": frozenset(["read", "submit", "cancel", "admin"]),
}


def normalize(perms):
    """把权限列表补全成 frozenset（``admin`` 自动带上其它权限）。"""
    out = set()
    for item in perms or []:
        name = str(item).strip()
        if not name:
            continue
        if name not in PERMS:
            raise ValueError("未知权限 %r（可用：%s）" % (name, ", ".join(PERMS)))
        out |= set(IMPLIED.get(name, (name,)))
    return frozenset(out)


def hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token(nbytes=24):
    """生成一把够长的令牌（默认 192 bit，URL 安全字符）。"""
    return secrets.token_urlsafe(nbytes)


class Grant(object):
    """一份授权：谁（hash）、能对哪个项目、做什么。"""

    __slots__ = ("token_hash", "project", "perms", "label")

    def __init__(self, token_hash, project=ANY_PROJECT, perms=(), label=""):
        self.token_hash = token_hash
        self.project = project or ANY_PROJECT
        self.perms = normalize(perms)
        self.label = label or ""

    def allows(self, action, project=None):
        if action not in self.perms:
            return False
        if self.project == ANY_PROJECT:
            return True
        return project is None or project == self.project

    def to_dict(self):
        return {"token_hash": self.token_hash, "project": self.project,
                "perms": sorted(self.perms), "label": self.label}

    def __repr__(self):
        return "Grant(%s, project=%r, perms=%s)" % (
            self.label or self.token_hash[:8], self.project, sorted(self.perms))


class Auth(object):
    """一组授权 + 可选的若干"管理员令牌"。

    ``admin_tokens`` 是给 ``mesh hub --token`` 那种老用法留的：一把口令就是管理员。
    """

    def __init__(self, grants=None, admin_tokens=(), enabled=None):
        self.grants = {}
        self.admin_hashes = set()
        for item in grants or []:
            grant = item if isinstance(item, Grant) else Grant(**item)
            self.grants[grant.token_hash] = grant
        for token in admin_tokens or ():
            if token:
                self.admin_hashes.add(hash_token(token))
        # enabled=None 表示"有令牌就开、没令牌就放行"，方便开发时直接跑
        self.enabled = bool(self.grants or self.admin_hashes) if enabled is None else bool(enabled)

    # -- 构造 ------------------------------------------------------------
    @classmethod
    def from_store(cls, store, admin_tokens=()):
        """从队列库里的 tokens 表读出来。"""
        grants = [Grant(row["token_hash"], row["project"], row["perms"], row["label"])
                  for row in store.load_tokens()]
        return cls(grants, admin_tokens)

    def add(self, token, perms, project=ANY_PROJECT, label=""):
        """把一把明文令牌加进来（调用方负责同时写进库里）。"""
        grant = Grant(hash_token(token), project, perms, label)
        self.grants[grant.token_hash] = grant
        self.enabled = True
        return grant

    def remove(self, token_hash):
        return self.grants.pop(token_hash, None) is not None

    # -- 判定 ------------------------------------------------------------
    def allow(self, token, action, project=None):
        """返回 ``(放行?, 原因)``。原因只在拒绝时有意义。"""
        if not self.enabled:
            return True, "未配置令牌（开发模式）"
        if not token:
            return False, "缺少令牌"
        digest = hash_token(token)
        if digest in self.admin_hashes:
            return True, "管理员令牌"
        grant = self.grants.get(digest)
        if grant is None:
            return False, "令牌无效"
        if not grant.allows(action, project):
            if action not in grant.perms:
                return False, "令牌没有 %s 权限" % action
            return False, "令牌只能用于项目 %s" % grant.project
        return True, ""

    def check(self, token, action, project=None):
        """``allow`` 的布尔版。"""
        return self.allow(token, action, project)[0]

    # -- 展示 ------------------------------------------------------------
    def __len__(self):
        return len(self.grants) + len(self.admin_hashes)

    def describe(self):
        return {
            "enabled": self.enabled,
            "admin_tokens": len(self.admin_hashes),
            "grants": [g.to_dict() for g in
                       sorted(self.grants.values(), key=lambda g: (g.project, g.label))],
        }

    def __repr__(self):
        return "Auth(%d grants, %d admin, %s)" % (
            len(self.grants), len(self.admin_hashes),
            "on" if self.enabled else "off")
