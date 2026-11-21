"""auth.py 的测试：权限展开、项目隔离、令牌只存哈希。"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import auth as A  # noqa: E402
from mesh import queue as Q  # noqa: E402


class TestNormalize(unittest.TestCase):
    def test_plain(self):
        self.assertEqual(A.normalize(["read"]), frozenset(["read"]))

    def test_admin_implies_everything(self):
        self.assertEqual(A.normalize(["admin"]),
                         frozenset(["read", "submit", "cancel", "admin"]))

    def test_duplicates_collapse(self):
        self.assertEqual(A.normalize(["read", "read"]), frozenset(["read"]))

    def test_empty(self):
        self.assertEqual(A.normalize([]), frozenset())
        self.assertEqual(A.normalize(None), frozenset())

    def test_unknown_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            A.normalize(["sudo"])
        self.assertIn("未知权限", str(ctx.exception))


class TestTokens(unittest.TestCase):
    def test_hash_is_stable_and_not_the_token(self):
        digest = A.hash_token("secret")
        self.assertEqual(digest, A.hash_token("secret"))
        self.assertNotEqual(digest, A.hash_token("secret2"))
        self.assertNotIn("secret", digest)
        self.assertEqual(len(digest), 64)

    def test_new_token_is_long_and_random(self):
        a, b = A.new_token(), A.new_token()
        self.assertGreaterEqual(len(a), 32)
        self.assertNotEqual(a, b)


class TestGrant(unittest.TestCase):
    def test_wildcard_project(self):
        grant = A.Grant(A.hash_token("t"), "", ["read"])
        self.assertTrue(grant.allows("read", "任何项目"))
        self.assertTrue(grant.allows("read"))

    def test_scoped_project(self):
        grant = A.Grant(A.hash_token("t"), "blog", ["read", "submit"])
        self.assertTrue(grant.allows("submit", "blog"))
        self.assertFalse(grant.allows("submit", "code"))

    def test_action_must_be_granted(self):
        grant = A.Grant(A.hash_token("t"), "", ["read"])
        self.assertFalse(grant.allows("submit", "blog"))

    def test_admin_grant(self):
        grant = A.Grant(A.hash_token("t"), "", ["admin"])
        for action in ("read", "submit", "cancel", "admin"):
            self.assertTrue(grant.allows(action, "x"))

    def test_to_dict_hides_nothing_it_should_not(self):
        grant = A.Grant(A.hash_token("t"), "blog", ["read"], "手机")
        payload = grant.to_dict()
        self.assertEqual(payload["project"], "blog")
        self.assertEqual(payload["label"], "手机")
        self.assertEqual(payload["perms"], ["read"])


class TestAuth(unittest.TestCase):
    def test_disabled_lets_everyone_in(self):
        auth = A.Auth()
        self.assertFalse(auth.enabled)
        allowed, why = auth.allow("", "admin", None)
        self.assertTrue(allowed)
        self.assertIn("开发模式", why)

    def test_enabled_requires_token(self):
        auth = A.Auth(admin_tokens=["boss"])
        allowed, why = auth.allow("", "read", None)
        self.assertFalse(allowed)
        self.assertIn("缺少令牌", why)

    def test_admin_token_does_anything(self):
        auth = A.Auth(admin_tokens=["boss"])
        for action in ("read", "submit", "cancel", "admin"):
            self.assertTrue(auth.check("boss", action, "任意"))

    def test_wrong_token(self):
        auth = A.Auth(admin_tokens=["boss"])
        allowed, why = auth.allow("nope", "read", None)
        self.assertFalse(allowed)
        self.assertEqual(why, "令牌无效")

    def test_project_isolation(self):
        auth = A.Auth()
        token = "kid-homework"
        auth.add(token, ["read", "submit"], "homework", "孩子作业")
        self.assertTrue(auth.check(token, "submit", "homework"))
        self.assertFalse(auth.check(token, "submit", "code"))

        allowed, why = auth.allow(token, "submit", "code")
        self.assertFalse(allowed)
        self.assertIn("只能用于项目", why)

    def test_missing_action_right(self):
        auth = A.Auth()
        token = "viewer"
        auth.add(token, ["read"], "", "只读")
        self.assertTrue(auth.check(token, "read"))
        self.assertFalse(auth.check(token, "cancel"))
        self.assertIn("没有 cancel 权限", auth.allow(token, "cancel")[1])

    def test_admin_does_not_leak_across_projects(self):
        auth = A.Auth()
        token = "blog-admin"
        auth.add(token, ["admin"], "blog", "博客管理员")
        self.assertTrue(auth.check(token, "admin", "blog"))
        self.assertFalse(auth.check(token, "admin", "code"))

    def test_remove(self):
        auth = A.Auth()
        grant = auth.add("t", ["read"])
        self.assertTrue(auth.remove(grant.token_hash))
        self.assertFalse(auth.remove(grant.token_hash))

    def test_add_enables(self):
        auth = A.Auth()
        self.assertFalse(auth.enabled)
        auth.add("t", ["read"])
        self.assertTrue(auth.enabled)

    def test_len_and_describe(self):
        auth = A.Auth(admin_tokens=["a", "b"])
        auth.add("t", ["read"], "p", "标签")
        self.assertEqual(len(auth), 3)
        payload = auth.describe()
        self.assertTrue(payload["enabled"])
        self.assertEqual(payload["admin_tokens"], 2)
        self.assertEqual(payload["grants"][0]["label"], "标签")


class TestFromStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-auth-")
        self.store = Q.Queue(os.path.join(self.tmp, "q.db"))

    def tearDown(self):
        self.store.close()

    def test_round_trip(self):
        self.store.add_token("s3cret", ["read", "submit"], "blog", "博客")
        auth = A.Auth.from_store(self.store)
        self.assertTrue(auth.enabled)
        self.assertTrue(auth.check("s3cret", "submit", "blog"))
        self.assertFalse(auth.check("s3cret", "read", "别的项目"))

    def test_plaintext_never_stored(self):
        self.store.add_token("s3cret", ["read"], "", "x")
        rows = self.store.load_tokens()
        self.assertNotIn("s3cret", str(rows))
        self.assertEqual(rows[0]["token_hash"], A.hash_token("s3cret"))

    def test_legacy_admin_token_alongside_grants(self):
        self.store.add_token("scoped", ["read"], "blog")
        auth = A.Auth.from_store(self.store, admin_tokens=["boss"])
        self.assertTrue(auth.check("boss", "admin", "任何"))
        self.assertTrue(auth.check("scoped", "read", "blog"))
        self.assertFalse(auth.check("scoped", "admin", "blog"))

    def test_empty_store_is_open(self):
        self.assertFalse(A.Auth.from_store(self.store).enabled)


if __name__ == "__main__":
    unittest.main(verbosity=2)
