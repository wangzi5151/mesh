#!/usr/bin/env python3
"""确认 mesh/ 里没有偷偷引入第三方包。

「运行时零依赖」是这个项目的立身之本（要能在手机 proot / Termux 里直接跑），
所以这条必须在 CI 里当硬门槛。

用法：
    python3 tools/check_stdlib_only.py            # 违规则退出码 1
"""

from __future__ import annotations

import ast
import os
import sys

# Python 3.10+ 有 sys.stdlib_module_names；3.8/3.9 用这份兜底清单。
FALLBACK_STDLIB = {
    "abc", "aifc", "argparse", "array", "ast", "asynchat", "asyncio", "asyncore",
    "atexit", "base64", "binascii", "binhex", "bisect", "builtins", "bz2",
    "calendar", "cmath", "cmd", "code", "codecs", "codeop", "collections",
    "colorsys", "compileall", "concurrent", "configparser", "contextlib",
    "contextvars", "copy", "copyreg", "crypt", "csv", "ctypes", "curses",
    "dataclasses", "datetime", "dbm", "decimal", "difflib", "dis", "distutils",
    "doctest", "dummy_threading", "email", "encodings", "enum", "errno",
    "faulthandler", "fcntl", "filecmp", "fileinput", "fnmatch", "formatter",
    "fractions", "ftplib", "functools", "gc", "getopt", "getpass", "gettext",
    "glob", "grp", "gzip", "hashlib", "heapq", "hmac", "html", "http", "idlelib",
    "imaplib", "imghdr", "imp", "importlib", "inspect", "io", "ipaddress",
    "itertools", "json", "keyword", "lib2to3", "linecache", "locale", "logging",
    "lzma", "mailbox", "mailcap", "marshal", "math", "mimetypes", "mmap",
    "modulefinder", "multiprocessing", "netrc", "nis", "nntplib", "numbers",
    "operator", "optparse", "os", "ossaudiodev", "parser", "pathlib", "pdb",
    "pickle", "pickletools", "pipes", "pkgutil", "platform", "plistlib", "poplib",
    "posix", "posixpath", "pprint", "profile", "pstats", "pty", "pwd",
    "py_compile", "pyclbr", "pydoc", "pydoc_data", "pyexpat", "queue", "quopri",
    "random", "re", "readline", "reprlib", "resource", "rlcompleter", "runpy",
    "sched", "secrets", "select", "selectors", "shelve", "shlex", "shutil",
    "signal", "site", "smtpd", "smtplib", "sndfile", "socket", "socketserver",
    "spwd", "sqlite3", "sre_compile", "sre_constants", "sre_parse", "ssl",
    "stat", "statistics", "string", "stringprep", "struct", "subprocess",
    "sunau", "symtable", "sys", "sysconfig", "syslog", "tabnanny", "tarfile",
    "telnetlib", "tempfile", "termios", "test", "textwrap", "threading",
    "time", "timeit", "tkinter", "token", "tokenize", "tomllib", "trace",
    "traceback", "tracemalloc", "tty", "turtle", "turtledemo", "types",
    "typing", "unicodedata", "unittest", "urllib", "uu", "uuid", "venv",
    "warnings", "wave", "weakref", "webbrowser", "winreg", "winsound",
    "wsgiref", "xml", "xmlrpc", "zipapp", "zipfile", "zipimport", "zlib",
    "_thread", "__future__",
}


def stdlib_names():
    names = set(getattr(sys, "stdlib_module_names", ()) or ())
    if names:
        return names | {"__future__"}
    return set(FALLBACK_STDLIB)


def top_level_imports(path):
    """返回该文件里所有顶层导入的模块名（含 from X import 的 X）。"""
    with open(path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:            # 相对导入（from . import x）一定是本项目
                continue
            if node.module:
                out.add(node.module.split(".")[0])
    return out


def main(argv):
    root = argv[1] if len(argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pkg = os.path.join(root, "mesh")
    if not os.path.isdir(pkg):
        print("找不到包目录：%s" % pkg)
        return 2

    allowed = stdlib_names() | {"mesh"}
    offenders = []
    checked = 0

    for dirpath, dirnames, filenames in os.walk(pkg):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in sorted(filenames):
            if not name.endswith(".py"):
                continue
            full = os.path.join(dirpath, name)
            checked += 1
            try:
                imports = top_level_imports(full)
            except SyntaxError as exc:
                offenders.append((full, "<语法错误 %s>" % exc))
                continue
            for mod in sorted(imports):
                if mod not in allowed:
                    offenders.append((full, mod))

    if offenders:
        print("发现第三方依赖（本项目要求运行时零依赖）：\n")
        for path, mod in offenders:
            print("  %-46s %s" % (os.path.relpath(path, root), mod))
        print("\n共 %d 处。要么去掉，要么在 README 里说清它不再是零依赖工具。" % len(offenders))
        return 1

    print("OK：%d 个文件全部只用标准库" % checked)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
