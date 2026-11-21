"""零依赖的 JSON-RPC over HTTP：节点与控制面之间唯一的传输层。

为什么不用 WebSocket / gRPC
---------------------------
目标是「手机 proot 里直接跑」，所以只用标准库：``http.server`` + ``urllib``。
长任务不需要服务端推送（用轮询 + 租约代替），文件块用 base64 装在 JSON 里。
简单、可调试（curl 就能测）、跨平台无坑。

线协议
------
::

    POST /rpc/<method>
    Authorization: Bearer <token>
    Content-Type: application/json

    {"a": 1}

    -> 200 {"ok": true, "r": {...}}
    -> 200 {"ok": false, "e": "错误信息", "t": "错误类型"}

    GET /health -> {"ok": true, "r": {"name": ..., "proto": 1}}

约定：成功放 ``r``，失败放 ``e``；HTTP 状态码始终 200（除非鉴权失败 401），
这样错误信息不会被中间层吞掉。

二进制
------
JSON 装不下 bytes，统一走 :func:`b64e` / :func:`b64d`（标准 base64）。
"""

from __future__ import annotations

import base64
import hmac
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__all__ = ["WireError", "Client", "Server", "b64e", "b64d", "NAME", "PROTO"]

NAME = "mesh"
PROTO = 1
DEFAULT_TIMEOUT = 30.0
MAX_BODY = 64 << 20          # 单请求体上限 64MB


class WireError(Exception):
    """传输层或远端业务错误。``kind`` 区分网络错误和远端拒绝。"""

    def __init__(self, message, kind="remote", status=None):
        super(WireError, self).__init__(message)
        self.kind = kind
        self.status = status


def b64e(data):
    if isinstance(data, (bytearray, memoryview)):
        data = bytes(data)
    return base64.b64encode(data).decode("ascii")


def b64d(text):
    return base64.b64decode((text or "").encode("ascii"))


# --------------------------------------------------------------------------
# 客户端
# --------------------------------------------------------------------------

class Client(object):
    """调一个节点。线程安全（每次请求独立连接）。"""

    def __init__(self, base_url, token="", timeout=DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = float(timeout)

    # -- 底层 ------------------------------------------------------------
    def call(self, method, payload=None):
        url = "%s/rpc/%s" % (self.base_url, urllib.parse.quote(method))
        body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            raise WireError("HTTP %d %s" % (exc.code, detail or exc.reason),
                            kind="http", status=exc.code)
        except urllib.error.URLError as exc:
            raise WireError("连不上 %s：%s" % (self.base_url, exc.reason), kind="network")
        except Exception as exc:
            raise WireError("请求失败：%s" % exc, kind="network")

        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            raise WireError("响应不是 JSON：%r" % raw[:200], kind="protocol")

        if not isinstance(data, dict):
            raise WireError("响应格式错误", kind="protocol")
        if not data.get("ok"):
            raise WireError(data.get("e") or "远端返回失败", kind="remote")
        # 不能写成 `data.get("r") or {}`：那样空列表、0、False 会被悄悄换成 {}，
        # 调用方分不清"什么都没有"和"一个空对象"。
        if "r" in data:
            return data["r"]
        return {}

    def health(self):
        url = self.base_url + "/health"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data.get("r") or {}
        except Exception as exc:
            raise WireError("健康检查失败：%s" % exc, kind="network")

    # -- 便捷方法 --------------------------------------------------------
    def cap(self):
        return self.call("cap")

    def ls(self, addr, limit=1000):
        return self.call("ls", {"addr": addr, "limit": limit})

    def stat(self, addr):
        return self.call("stat", {"addr": addr})

    def read(self, addr, offset=0, limit=None):
        r = self.call("read", {"addr": addr, "offset": offset,
                               "limit": limit if limit is not None else -1})
        return b64d(r.get("data") or "")

    def write(self, addr, data, expected_sha=None):
        payload = {"addr": addr, "data": b64e(data)}
        if expected_sha:
            payload["expected_sha"] = expected_sha
        return self.call("write", payload)

    def sig(self, addr, block_size):
        return self.call("sig", {"addr": addr, "block_size": block_size})

    def mkdelta(self, addr, old_sha, block_size, sig):
        return self.call("delta", {"addr": addr, "old_sha": old_sha,
                                   "block_size": block_size, "sig": sig})

    def apply_delta(self, addr, old_sha, block_size, delta):
        return self.call("patch", {"addr": addr, "old_sha": old_sha,
                                   "block_size": block_size, "delta": delta})

    def exec_(self, cmd, cwd="", timeout=60, scope=None):
        return self.call("exec", {"cmd": cmd, "cwd": cwd,
                                  "timeout": timeout, "scope": scope or []})

    def run_job(self, job_dict, lease=180):
        return self.call("job", {"job": job_dict, "lease": lease})

    def __repr__(self):
        return "Client(%s)" % self.base_url


# --------------------------------------------------------------------------
# 服务端
# --------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mesh/%d" % PROTO

    # 默认实现会往 stderr 打日志，太吵 —— 交给上层的 hooks
    def log_message(self, fmt, *args):
        hook = getattr(self.server, "log_hook", None)
        if hook:
            hook(fmt % args)

    # -- 响应 ------------------------------------------------------------
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._send_raw(code, body, "application/json; charset=utf-8")

    def _send_html(self, code, text):
        self._send_raw(code, text.encode("utf-8"), "text/html; charset=utf-8")

    def _send_raw(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _auth_ok(self, query=""):
        token = getattr(self.server, "token", "")
        if not token:
            return True
        header = self.headers.get("Authorization") or ""
        if header.startswith("Bearer "):
            if hmac.compare_digest(header[7:].strip(), token):
                return True
        # 浏览器里打不开自定义头，所以 GET 页面允许 ?token= —— 手机上看状态
        # 只能这么带。只对 GET 生效，POST 一律要求 Bearer。
        if query:
            given = dict(urllib.parse.parse_qsl(query)).get("token") or ""
            if given and hmac.compare_digest(given, token):
                return True
        return False

    # -- 路由 ------------------------------------------------------------
    def do_GET(self):
        path, _, query = self.path.partition("?")
        if path == "/health":
            self._send(200, {"ok": True, "r": {"name": NAME, "proto": PROTO,
                                               "node": getattr(self.server, "node_id", "")}})
            return

        get_routes = getattr(self.server, "get_routes", None) or {}
        handler = get_routes.get(path)
        if handler is None:
            self._send(404, {"ok": False, "e": "未知路径", "t": "NotFound"})
            return
        if not self._auth_ok(query):
            self._send(401, {"ok": False, "e": "令牌无效", "t": "Unauthorized"})
            return
        try:
            result = handler(dict(urllib.parse.parse_qsl(query)))
        except Exception as exc:
            self._send(200, {"ok": False, "e": str(exc) or exc.__class__.__name__,
                             "t": exc.__class__.__name__})
            return
        if isinstance(result, str):
            self._send_html(200, result)
        else:
            self._send(200, {"ok": True, "r": result})

    def do_POST(self):
        if not self._auth_ok():
            self._send(401, {"ok": False, "e": "令牌无效", "t": "Unauthorized"})
            return

        path = self.path.split("?")[0]
        if not path.startswith("/rpc/"):
            self._send(404, {"ok": False, "e": "未知路径", "t": "NotFound"})
            return
        method = urllib.parse.unquote(path[len("/rpc/"):])

        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length > MAX_BODY:
            self._send(200, {"ok": False, "e": "请求体过大", "t": "TooLarge"})
            return
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception as exc:
            self._send(200, {"ok": False, "e": "请求 JSON 解析失败：%s" % exc, "t": "BadJSON"})
            return
        if not isinstance(payload, dict):
            self._send(200, {"ok": False, "e": "请求必须是对象", "t": "BadJSON"})
            return

        routes = getattr(self.server, "routes", {})
        handler = routes.get(method)
        if handler is None:
            self._send(200, {"ok": False, "e": "未知方法：%s" % method, "t": "NoMethod"})
            return

        try:
            result = handler(payload)
        except Exception as exc:
            self._send(200, {"ok": False, "e": str(exc) or exc.__class__.__name__,
                             "t": exc.__class__.__name__})
            return
        self._send(200, {"ok": True, "r": result if result is not None else {}})


class Server(object):
    """把 ``{方法名: 处理函数}`` 表（POST ``/rpc/<方法>``）和
    ``{路径: 处理函数}`` 表（GET，给人看的页面/JSON）暴露成 HTTP 服务。"""

    def __init__(self, host="127.0.0.1", port=0, token="", node_id=""):
        self.host = host
        self.port = port
        self.token = token
        self.node_id = node_id
        self.routes = {}
        self.get_routes = {}
        self._httpd = None
        self._thread = None

    # -- 注册 ------------------------------------------------------------
    def route(self, name):
        def deco(fn):
            self.routes[name] = fn
            return fn
        return deco

    def add(self, name, fn):
        self.routes[name] = fn
        return fn

    def add_get(self, path, fn):
        """注册一个 GET 路由。处理函数收到 query 字典；返回 str 视为 HTML。"""
        if not path.startswith("/"):
            path = "/" + path
        self.get_routes[path] = fn
        return fn

    # -- 生命周期 --------------------------------------------------------
    def start(self):
        self._httpd = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._httpd.daemon_threads = True
        self._httpd.routes = self.routes
        self._httpd.get_routes = self.get_routes
        self._httpd.token = self.token
        self._httpd.node_id = self.node_id
        self._httpd.log_hook = None
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(target=self._httpd.serve_forever, name="mesh-http")
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self):
        if self._httpd is not None:
            try:
                self._httpd.shutdown()
                self._httpd.server_close()
            except Exception:
                pass
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def url(self):
        host = self.host if self.host not in ("0.0.0.0", "") else "127.0.0.1"
        return "http://%s:%d" % (host, self.port)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
        return False

    def __repr__(self):
        return "Server(%s, %d routes, %d get)" % (
            self.url, len(self.routes), len(self.get_routes))
