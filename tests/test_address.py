"""address.py 的测试：解析、规范化、往返、通配匹配、错误路径。"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import address as A  # noqa: E402


class TestParse(unittest.TestCase):
    def test_full_form(self):
        a = A.parse("mesh://node-a/ubuntu:project/src/main.py")
        self.assertEqual((a.node, a.env, a.root, a.path),
                         ("node-a", "ubuntu", "project", "src/main.py"))

    def test_env_omitted(self):
        a = A.parse("mesh://node-a/project/src/main.py")
        self.assertEqual((a.node, a.env, a.root, a.path),
                         ("node-a", "", "project", "src/main.py"))

    def test_root_only(self):
        a = A.parse("mesh://node-a/project")
        self.assertEqual((a.node, a.root, a.path), ("node-a", "project", ""))

    def test_node_only(self):
        a = A.parse("mesh://node-a")
        self.assertEqual((a.node, a.root, a.path), ("node-a", "", ""))

    def test_trailing_slash_ignored(self):
        self.assertEqual(A.parse("mesh://node-a/project/").path, "")
        self.assertEqual(A.parse("mesh://node-a/project/src/").path, "src")

    def test_scheme_optional(self):
        self.assertEqual(A.parse("node-a/project/x.py"), A.parse("mesh://node-a/project/x.py"))

    def test_wildcard_and_local(self):
        self.assertTrue(A.parse("mesh://*/project/x.py").is_wildcard)
        self.assertTrue(A.parse("mesh://local/project").is_local)

    def test_round_trip(self):
        for text in ("mesh://node-a/ubuntu:project/src/main.py",
                     "mesh://node-a/project",
                     "mesh://node-a",
                     "mesh://*/ubuntu:project/README.md"):
            self.assertEqual(str(A.parse(text)), text, "往返失败：%s" % text)

    def test_dots_in_names(self):
        a = A.parse("mesh://phone.1/ubuntu-20.04:my_root/a_b/c-d.py")
        self.assertEqual((a.node, a.env, a.root, a.path),
                         ("phone.1", "ubuntu-20.04", "my_root", "a_b/c-d.py"))


class TestNormalizePath(unittest.TestCase):
    def test_strips_redundant_slashes_and_dots(self):
        self.assertEqual(A.normalize_path("//a//b/./c//"), "a/b/c")
        self.assertEqual(A.normalize_path(""), "")
        self.assertEqual(A.normalize_path(None), "")
        self.assertEqual(A.normalize_path("."), "")

    def test_rejects_escape(self):
        for bad in ("../etc/passwd", "a/../../b", "..", "a/.."):
            with self.assertRaises(A.AddressError, msg="应当拒绝 %r" % bad):
                A.normalize_path(bad)

    def test_rejects_bad_chars(self):
        with self.assertRaises(A.AddressError):
            A.normalize_path("a\x00b")
        with self.assertRaises(A.AddressError):
            A.normalize_path("a\\b")
        with self.assertRaises(A.AddressError):
            A.normalize_path("x" * (A.MAX_PATH + 1))

    def test_rejects_non_string(self):
        with self.assertRaises(A.AddressError):
            A.normalize_path(123)


class TestErrors(unittest.TestCase):
    def test_empty(self):
        for bad in ("", "   ", "mesh://", "mesh:///"):
            with self.assertRaises(A.AddressError, msg="应当拒绝 %r" % bad):
                A.parse(bad)

    def test_wrong_scheme(self):
        with self.assertRaises(A.AddressError):
            A.parse("http://node-a/x")

    def test_bad_names(self):
        for bad in ("mesh://-bad/project", "mesh://no de/project",
                    "mesh://" + "x" * 65 + "/project"):
            with self.assertRaises(A.AddressError, msg="应当拒绝 %r" % bad):
                A.parse(bad)

    def test_empty_env_or_root(self):
        with self.assertRaises(A.AddressError):
            A.parse("mesh://node-a/:project")
        with self.assertRaises(A.AddressError):
            A.parse("mesh://node-a/ubuntu:")

    def test_escape_in_address(self):
        with self.assertRaises(A.AddressError):
            A.parse("mesh://node-a/project/../../etc/passwd")


class TestMatches(unittest.TestCase):
    def test_exact(self):
        self.assertTrue(A.matches("mesh://n1/p/x.py", "mesh://n1/p/x.py"))
        self.assertFalse(A.matches("mesh://n1/p/x.py", "mesh://n2/p/x.py"))
        self.assertFalse(A.matches("mesh://n1/p/x.py", "mesh://n1/p/y.py"))

    def test_wildcard_node(self):
        self.assertTrue(A.matches("mesh://*/p/x.py", "mesh://n1/p/x.py"))
        self.assertTrue(A.matches("mesh://*/p/x.py", "mesh://n2/p/x.py"))

    def test_empty_segments_are_wildcards(self):
        self.assertTrue(A.matches("mesh://n1", "mesh://n1/p/x.py"))
        self.assertTrue(A.matches("mesh://n1/p", "mesh://n1/p/x.py"))
        self.assertFalse(A.matches("mesh://n1/q", "mesh://n1/p/x.py"))

    def test_accepts_objects(self):
        pat = A.parse("mesh://*/p")
        self.assertTrue(A.matches(pat, A.parse("mesh://n1/p/x.py")))


class TestValueSemantics(unittest.TestCase):
    def test_equality_and_hash(self):
        a = A.parse("mesh://n1/p/x.py")
        b = A.parse("mesh://n1/p/x.py")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertEqual(len({a, b}), 1)
        self.assertNotEqual(a, A.parse("mesh://n1/p/y.py"))

    def test_dict_round_trip(self):
        a = A.parse("mesh://n1/ubuntu:project/src/x.py")
        self.assertEqual(A.MeshAddress.from_dict(a.to_dict()), a)

    def test_replace(self):
        a = A.parse("mesh://n1/ubuntu:project/src/x.py")
        b = a._replace(path="other/y.py")
        self.assertEqual(b.path, "other/y.py")
        self.assertEqual(a.path, "src/x.py", "_replace 不应改动原对象")

    def test_repr(self):
        self.assertIn("mesh://n1", repr(A.parse("mesh://n1/p")))


class TestNames(unittest.TestCase):
    def test_chinese_node_name(self):
        a = A.parse("mesh://手机/work/sub/a.txt")
        self.assertEqual((a.node, a.root, a.path), ("手机", "work", "sub/a.txt"))
        self.assertEqual(A.parse(str(a)), a, "中文名必须能往返")

    def test_chinese_env_and_root(self):
        a = A.parse("mesh://笔记本/乌班图:项目/src/a.py")
        self.assertEqual((a.node, a.env, a.root), ("笔记本", "乌班图", "项目"))

    def test_valid_names(self):
        for name in ("n1", "手机", "my-laptop", "a_b.c", "节点1"):
            self.assertTrue(A.is_valid_name(name), name)

    def test_invalid_names(self):
        for name in ("", "a/b", "x:y", "..", "-x", ".x", "我的 手机", "a" * 65):
            self.assertFalse(A.is_valid_name(name), name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
