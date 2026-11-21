"""块级增量传输（rsync 式差分）。

为什么需要它
------------
内容寻址存储（CAS）只能做到"同一个文件不传两次"。只要文件改了一个字节，
整文件就要重传 —— 在手机流量下这是致命的。

这里实现真正的差分：接收方把旧文件的**块校验表**发过来，发送方只回
"哪些块能复用 + 哪些是新字节"，接收方在本地把新文件拼回去。

算法
----
1. 接收方按 ``block_size`` 切分旧文件，每块算一个弱校验（Adler-32）和一个
   强校验（sha256），得到签名表。
2. 发送方在**新文件**上滑动一个等宽窗口，用弱校验 O(1) 滚动比对；弱校验命中
   才去算强���验（避免对每个位置都做 sha256）。
3. 命中 ⇒ 输出 ``("c", 块号)``；未命中 ⇒ 把窗口最左字节吐进字面量，窗口右移。
4. 接收方按指令流重建：``c`` 从旧文件对应偏移拷块，``l`` 直接写字节。

弱校验滚动公式（窗口长度 L，丢左字节 x1、补右字节 x_{L+1}）::

    a' = a - x1 + x_{L+1}
    b' = b - L*x1 + a' - 1

收缩（只丢左字节，窗口变 L-1）::

    a' = a - x1
    b' = b - L*x1 - 1

边界
----
v0.1 的 ``diff`` 在内存里完成（``max_bytes`` 默认 64MB，够覆盖 32MB 单文件上限）。
``patch_file`` 是流式的：拷块用 ``seek`` + 分片读，字面量直接写，内存占用与文件大小无关。
超过 64MB 的流式差分见 ROADMAP 的 P1。
"""

from __future__ import annotations

import base64
import hashlib
import os
import zlib

__all__ = [
    "DeltaError", "WeakSum", "Delta",
    "weak_checksum", "block_signature", "choose_block_size",
    "diff", "patch", "diff_ops", "patch_ops",
    "diff_ops_with_sig", "diff_with_sig",
    "diff_files", "patch_file", "estimate", "encode_ops", "decode_ops",
    "sha256_hex", "DEFAULT_BLOCK_SIZE", "MIN_BLOCK_SIZE", "MAX_BLOCK_SIZE",
]

MOD = 65521                      # Adler-32 的模数（小于 2^16 的最大素数）
DEFAULT_BLOCK_SIZE = 4096
MIN_BLOCK_SIZE = 64
MAX_BLOCK_SIZE = 1 << 20         # 1 MiB
DEFAULT_MAX_BYTES = 64 << 20     # 内存差分上限 64MB
CHUNK = 1 << 20
DELTA_VERSION = 1


class DeltaError(Exception):
    """差分/重建过程中的错误（参数非法、基线不匹配、校验失败）。"""


# --------------------------------------------------------------------------
# 校验
# --------------------------------------------------------------------------

def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def _check_block_size(block_size):
    if isinstance(block_size, bool) or not isinstance(block_size, int):
        raise DeltaError("block_size 必须是整数，收到 %r" % (block_size,))
    if not MIN_BLOCK_SIZE <= block_size <= MAX_BLOCK_SIZE:
        raise DeltaError(
            "block_size 必须在 %d~%d 之间，收到 %d" % (MIN_BLOCK_SIZE, MAX_BLOCK_SIZE, block_size))
    return block_size


def choose_block_size(size):
    """按文件大小挑块大小：约 sqrt(size)，取 2 的幂，夹在合法区间内。

    rsync 的经验值。太小 ⇒ 签名表爆炸；太大 ⇒ 命中率下降。
    """
    if size <= 0:
        return MIN_BLOCK_SIZE
    guess = 1
    while guess * guess < size:
        guess <<= 1
    return max(MIN_BLOCK_SIZE, min(MAX_BLOCK_SIZE, guess))


def weak_checksum(data):
    """一段数据的 Adler-32 弱校验（返回 32 位整数）。

    直接用标准库 ``zlib.adler32`` —— 它是 C 实现，与本模块的滚动公式
    （``WeakSum``，纯 Python 参考实现）数学上完全等价，测试里交叉验证。
    这是把差分从"每字节一次 Python 循环"降到"每块一次 C 调用"的关键。
    """
    return zlib.adler32(data) & 0xFFFFFFFF


class WeakSum(object):
    """可滚动的 Adler-32 累加器。

    窗口长度由 ``length`` 维护；``roll`` 保持长度不变，``shrink`` 减一。
    所有运算都在模 ``MOD`` 下进行，因此与 ``weak_checksum`` 结果一致。
    """

    __slots__ = ("a", "b", "length")

    def __init__(self, data=None):
        self.a = 1
        self.b = 0
        self.length = 0
        if data:
            self.update(data)

    def update(self, data):
        a, b = self.a, self.b
        for byte in data:
            a = (a + byte) % MOD
            b = (b + a) % MOD
        self.a, self.b = a, b
        self.length += len(data)
        return self

    def add_byte(self, byte):
        self.a = (self.a + byte) % MOD
        self.b = (self.b + self.a) % MOD
        self.length += 1
        return self

    @property
    def value(self):
        return (self.b << 16) | self.a

    def roll(self, out_byte, in_byte):
        """窗口长度不变：丢掉最左字节，补入最右新字节。"""
        n = self.length
        if n <= 0:
            raise DeltaError("窗口为空，无法滑动")
        a = (self.a - out_byte + in_byte) % MOD
        b = (self.b - n * out_byte + a - 1) % MOD
        self.a, self.b = a, b
        return self

    def shrink(self, out_byte):
        """窗口长度减一：丢掉最左字节（用于文件尾部）。"""
        n = self.length
        if n <= 0:
            raise DeltaError("窗口为空，无法收缩")
        a = (self.a - out_byte) % MOD
        b = (self.b - n * out_byte - 1) % MOD
        self.a, self.b = a, b
        self.length = n - 1
        return self

    def __repr__(self):
        return "WeakSum(len=%d, value=%d)" % (self.length, self.value)


def block_signature(data, block_size=DEFAULT_BLOCK_SIZE):
    """把旧文件切成块，返回 ``[(弱校验, 强校验hex, 块长度), ...]``。

    最后一块可能短于 ``block_size``，所以长度必须一起带上 —— 否则一个短块
    可能被误配到一个满宽窗口上。
    """
    _check_block_size(block_size)
    sig = []
    pos = 0
    n = len(data)
    while pos < n:
        chunk = data[pos:pos + block_size]
        sig.append((weak_checksum(chunk), hashlib.sha256(chunk).hexdigest(), len(chunk)))
        pos += block_size
    return sig


# --------------------------------------------------------------------------
# 差分
# --------------------------------------------------------------------------

def diff_ops(old, new, block_size=DEFAULT_BLOCK_SIZE):
    """核心差分：返回指令序列 ``[("c", 块号), ("l", bytes), ...]``。"""
    _check_block_size(block_size)

    # 快路径：内容没变 —— 全部复用
    if old == new:
        return [("c", i) for i in range((len(old) + block_size - 1) // block_size)]

    return diff_ops_with_sig(block_signature(old, block_size), new, block_size)


def diff_ops_with_sig(sig, new, block_size=DEFAULT_BLOCK_SIZE):
    """用旧文件的**签名表**做差分 —— 发送方拿不到旧文件本体时用。

    这正是 rsync 的工作方式：接收方只把块校验发过来，发送方在自己这边的
    新文件上滑动比对。所以"必须先传一遍旧文件"这个前提不成立。

    性能要点：窗口的"整块填充"交给 C 实现的 ``zlib.adler32``，只有真正需要
    滑动时才逐字节滚动。每匹配一块只花一次 C 调用，而不是 block_size 次
    Python 循环 —— 实测把 1.4MB 文件的差分从 3.5s 降到几十毫秒。
    """
    _check_block_size(block_size)
    if not isinstance(sig, (list, tuple)):
        raise DeltaError("签名表必须是列表")

    table = {}
    for idx, entry in enumerate(sig):
        if not isinstance(entry, (list, tuple)) or len(entry) != 3:
            raise DeltaError("签名项必须是 (弱校验, 强校验, 长度)")
        table.setdefault(entry[0], []).append(idx)

    ops = []
    lit = bytearray()
    n = len(new)
    pos = 0
    win = 0
    a = 1
    b = 0
    adler32 = zlib.adler32
    sha256 = hashlib.sha256
    MODL = MOD

    while pos < n:
        # 1) 把窗口撑满
        if win < block_size:
            end = pos + win
            if end >= n:
                pass                                   # 尾部：不再增长
            elif win == 0 and pos + block_size <= n:
                v = adler32(new[pos:pos + block_size]) & 0xFFFFFFFF
                a = v & 0xFFFF
                b = v >> 16
                win = block_size
                continue
            else:
                byte = new[end]
                a = (a + byte) % MODL
                b = (b + a) % MODL
                win += 1
                continue

        # 2) 窗口已满（或已到尾部）—— 尝试匹配
        if win:
            cands = table.get((b << 16) | a)
            hit = -1
            if cands:
                strong = sha256(new[pos:pos + win]).hexdigest()
                for idx in cands:
                    entry = sig[idx]
                    if entry[2] == win and entry[1] == strong:
                        hit = idx
                        break
            if hit >= 0:
                if lit:
                    ops.append(("l", bytes(lit)))
                    lit = bytearray()
                ops.append(("c", hit))
                pos += win
                win = 0
                a = 1
                b = 0
                continue

        # 3) 没匹配上：最左字节进字面量，窗口右移（或收缩）
        out = new[pos]
        lit.append(out)
        if win:
            nxt = pos + win
            if nxt < n:
                inb = new[nxt]
                a = (a - out + inb) % MODL
                b = (b - win * out + a - 1) % MODL
            else:
                a = (a - out) % MODL
                b = (b - win * out - 1) % MODL
                win -= 1
        pos += 1

    if lit:
        ops.append(("l", bytes(lit)))
    return ops


def patch_ops(old, ops, block_size, expected_sha=None):
    """按指令序列重建新内容。"""
    _check_block_size(block_size)
    n_blocks = (len(old) + block_size - 1) // block_size
    out = bytearray()
    for kind, val in ops:
        if kind == "c":
            idx = int(val)
            if idx < 0 or idx >= n_blocks:
                raise DeltaError("块索引越界：%d（旧文件只有 %d 块）" % (idx, n_blocks))
            out += old[idx * block_size:(idx + 1) * block_size]
        elif kind == "l":
            out += val
        else:
            raise DeltaError("未知指令：%r" % (kind,))
    result = bytes(out)
    if expected_sha:
        got = sha256_hex(result)
        if got != expected_sha:
            raise DeltaError("重建结果校验失败：期望 %s…，实际 %s…"
                             % (expected_sha[:12], got[:12]))
    return result


class Delta(object):
    """一份差分：块大小 + 指令流 + 两端哈希。可直接 JSON 上线。"""

    __slots__ = ("block_size", "ops", "old_sha", "new_sha", "new_size")

    def __init__(self, block_size, ops, old_sha="", new_sha="", new_size=0):
        self.block_size = block_size
        self.ops = ops
        self.old_sha = old_sha
        self.new_sha = new_sha
        self.new_size = new_size

    def to_dict(self):
        return {
            "v": DELTA_VERSION,
            "block_size": self.block_size,
            "old_sha": self.old_sha,
            "new_sha": self.new_sha,
            "new_size": self.new_size,
            "ops": encode_ops(self.ops),
        }

    @classmethod
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise DeltaError("delta 必须是对象")
        if int(obj.get("v") or 0) != DELTA_VERSION:
            raise DeltaError("不支持的 delta 版本：%r" % (obj.get("v"),))
        return cls(
            _check_block_size(int(obj.get("block_size") or 0)),
            decode_ops(obj.get("ops") or []),
            str(obj.get("old_sha") or ""),
            str(obj.get("new_sha") or ""),
            int(obj.get("new_size") or 0),
        )

    def __repr__(self):
        return "Delta(block_size=%d, ops=%d, new_size=%d)" % (
            self.block_size, len(self.ops), self.new_size)


def encode_ops(ops):
    """指令流 → JSON 友好的形式（字面量走 base64）。"""
    out = []
    for kind, val in ops:
        if kind == "c":
            out.append(["c", int(val)])
        elif kind == "l":
            out.append(["l", base64.b64encode(val).decode("ascii")])
        else:
            raise DeltaError("未知指令：%r" % (kind,))
    return out


def decode_ops(raw):
    """JSON 形式 → 指令流。"""
    ops = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise DeltaError("指令格式非法：%r" % (item,))
        kind, val = item
        if kind == "c":
            ops.append(("c", int(val)))
        elif kind == "l":
            try:
                ops.append(("l", base64.b64decode(val)))
            except Exception as exc:
                raise DeltaError("字面量 base64 解码失败：%s" % exc)
        else:
            raise DeltaError("未知指令：%r" % (kind,))
    return ops


def diff(old, new, block_size=None, max_bytes=DEFAULT_MAX_BYTES):
    """生成从 ``old`` 到 ``new`` 的差分。

    ``block_size=None`` 时按新文件大小自动挑。
    """
    if len(new) > max_bytes:
        raise DeltaError(
            "文件 %d 字节超过内存差分上限 %d 字节；请改用流式差分（见 ROADMAP P1）"
            % (len(new), max_bytes))
    if block_size is None:
        block_size = choose_block_size(len(old) or len(new))
    _check_block_size(block_size)
    ops = diff_ops(old, new, block_size)
    return Delta(block_size, ops, sha256_hex(old), sha256_hex(new), len(new))


def diff_with_sig(sig, new, block_size, old_sha="", max_bytes=DEFAULT_MAX_BYTES):
    """只凭签名表生成差分 —— 发送方单独用这个。

    ``old_sha`` 是接收方旧文件的哈希，写进 delta 里让接收方自检基线。
    """
    if len(new) > max_bytes:
        raise DeltaError(
            "文件 %d 字节超过内存差分上限 %d 字节" % (len(new), max_bytes))
    _check_block_size(block_size)
    ops = diff_ops_with_sig(sig, new, block_size)
    return Delta(block_size, ops, old_sha, sha256_hex(new), len(new))


def patch(old, delta):
    """用旧内容和差分重建新内容。"""
    if isinstance(delta, dict):
        delta = Delta.from_dict(delta)
    if delta.old_sha:
        got = sha256_hex(old)
        if got != delta.old_sha:
            raise DeltaError(
                "基线不匹配：这份 delta 是针对另一版旧文件生成的"
                "（期�� %s…，实际 %s…）" % (delta.old_sha[:12], got[:12]))
    return patch_ops(old, delta.ops, delta.block_size, delta.new_sha or None)


def estimate(delta):
    """差分收益统计（用于日志/看板展示"省了多少流量"）。"""
    if isinstance(delta, dict):
        delta = Delta.from_dict(delta)
    literal = 0
    copies = 0
    for kind, val in delta.ops:
        if kind == "l":
            literal += len(val)
        else:
            copies += 1
    total = delta.new_size or (literal + copies * delta.block_size)
    copied = max(0, total - literal)
    ratio = (float(literal) / total) if total else 0.0
    return {
        "total_bytes": total,
        "literal_bytes": literal,
        "copied_bytes": copied,
        "copy_blocks": copies,
        "wire_ratio": ratio,
        "saved_ratio": 1.0 - ratio,
    }


# --------------------------------------------------------------------------
# 文件级
# --------------------------------------------------------------------------

def diff_files(old_path, new_path, block_size=None, max_bytes=DEFAULT_MAX_BYTES):
    """两个文件之间的差分。"""
    with open(old_path, "rb") as f:
        old = f.read()
    with open(new_path, "rb") as f:
        new = f.read()
    return diff(old, new, block_size, max_bytes)


def patch_file(old_path, delta, out_path, expected_sha=None):
    """用旧文件 + 差分写出新文件。

    流式：拷块走 ``seek`` + 分片读，字面量直接写，内存占用与文件大小无关。
    先写临时文件、校验通过再原子替换 —— 失败不会留下半截结果。
    """
    if isinstance(delta, dict):
        delta = Delta.from_dict(delta)
    block_size = _check_block_size(delta.block_size)
    want = expected_sha or delta.new_sha

    if delta.old_sha:
        h = hashlib.sha256()
        with open(old_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    break
                h.update(chunk)
        if h.hexdigest() != delta.old_sha:
            raise DeltaError("基线文件不匹配：%s 不是生成这份 delta 时用的��文件" % old_path)

    tmp = out_path + ".mesh-delta-tmp"
    h = hashlib.sha256()
    try:
        with open(old_path, "rb") as src, open(tmp, "wb") as out:
            for kind, val in delta.ops:
                if kind == "c":
                    src.seek(int(val) * block_size)
                    remaining = block_size
                    while remaining > 0:
                        chunk = src.read(min(CHUNK, remaining))
                        if not chunk:
                            break
                        out.write(chunk)
                        h.update(chunk)
                        remaining -= len(chunk)
                elif kind == "l":
                    out.write(val)
                    h.update(val)
                else:
                    raise DeltaError("未知指令：%r" % (kind,))
            out.flush()
            os.fsync(out.fileno())
        got = h.hexdigest()
        if want and got != want:
            raise DeltaError("重建结果校验失败：期望 %s…，实际 %s…"
                             % (want[:12], got[:12]))
        os.replace(tmp, out_path)
        return got
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
