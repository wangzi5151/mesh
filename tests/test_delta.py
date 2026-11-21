"""delta.py 的测试：滚动校验正确性、差分/重建往返、边界与错误路径。"""

from __future__ import annotations

import os
import random
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mesh import delta as D  # noqa: E402


def rand_bytes(n, seed=0):
    rnd = random.Random(seed)
    return bytes(rnd.randrange(256) for _ in range(n))


class TestWeakSum(unittest.TestCase):
    def test_matches_bulk_checksum(self):
        for n in (0, 1, 63, 64, 65, 4096):
            data = rand_bytes(n, seed=n)
            self.assertEqual(D.weak_checksum(data), D.WeakSum(data).value,
                             "长度 %d 时滚动校验与批量校验不一致" % n)

    def test_roll_matches_recompute(self):
        """滑动一步后的校验值，必须等于直接对新窗口重算。"""
        data = rand_bytes(4096, seed=7)
        block = 64
        ws = D.WeakSum(data[:block])
        for i in range(1, 200):
            ws.roll(data[i - 1], data[i - 1 + block])
            want = D.weak_checksum(data[i:i + block])
            self.assertEqual(ws.value, want, "第 %d 步滑动后校验值不对" % i)
            self.assertEqual(ws.length, block)

    def test_shrink_matches_recompute(self):
        data = rand_bytes(4096, seed=11)
        block = 64
        ws = D.WeakSum(data[:block])
        for i in range(1, 40):
            ws.shrink(data[i - 1])
            want = D.weak_checksum(data[i:block])
            self.assertEqual(ws.value, want, "第 %d 步收缩后校验值不对" % i)
            self.assertEqual(ws.length, block - i)

    def test_empty_window_raises(self):
        ws = D.WeakSum()
        with self.assertRaises(D.DeltaError):
            ws.roll(1, 2)
        with self.assertRaises(D.DeltaError):
            ws.shrink(1)


class TestBlockSignature(unittest.TestCase):
    def test_shape_and_tail(self):
        sig = D.block_signature(b"x" * 100, 64)
        self.assertEqual(len(sig), 2)
        self.assertEqual(sig[0][2], 64)
        self.assertEqual(sig[1][2], 36)          # 尾部短块
        self.assertEqual(sig[0][0], D.weak_checksum(b"x" * 64))

    def test_empty(self):
        self.assertEqual(D.block_signature(b"", 64), [])

    def test_bad_block_size(self):
        for bad in (0, -1, 63, (1 << 20) + 1, "64", 64.0, True):
            with self.assertRaises(D.DeltaError, msg="block_size=%r 应当被拒绝" % (bad,)):
                D.block_signature(b"abc", bad)


class TestRoundTrip(unittest.TestCase):
    def _round(self, old, new, block_size=64):
        d = D.diff(old, new, block_size)
        self.assertEqual(d.new_sha, D.sha256_hex(new))
        got = D.patch(old, d)
        self.assertEqual(got, new, "重建结果与目标不一致")
        return d

    def test_identical(self):
        data = rand_bytes(4096, seed=1)
        d = self._round(data, data)
        self.assertEqual(D.estimate(d)["literal_bytes"], 0)
        self.assertTrue(all(k == "c" for k, _ in d.ops))

    def test_empty_old(self):
        new = rand_bytes(500, seed=2)
        d = self._round(b"", new)
        self.assertTrue(all(k == "l" for k, _ in d.ops))

    def test_empty_new(self):
        old = rand_bytes(500, seed=3)
        d = self._round(old, b"")
        self.assertEqual(d.ops, [])

    def test_both_empty(self):
        self._round(b"", b"")

    def test_append(self):
        old = rand_bytes(1000, seed=4)
        new = old + rand_bytes(300, seed=5)
        d = self._round(old, new)
        est = D.estimate(d)
        self.assertGreater(est["copied_bytes"], 0)
        self.assertLess(est["literal_bytes"], 500)

    def test_insert_in_middle(self):
        old = rand_bytes(4096, seed=6)
        new = old[:2000] + rand_bytes(100, seed=7) + old[2000:]
        d = self._round(old, new)
        est = D.estimate(d)
        self.assertLess(est["literal_bytes"], 600, "中间插入只应产生少量字面量")

    def test_delete_in_middle(self):
        old = rand_bytes(4096, seed=8)
        new = old[:2000] + old[2100:]
        self._round(old, new)

    def test_single_byte_change_saves_most(self):
        old = rand_bytes(64 * 1024, seed=9)
        new = bytearray(old)
        new[30000] ^= 0xFF
        d = self._round(old, bytes(new), 1024)
        est = D.estimate(d)
        self.assertLess(est["literal_bytes"], 4096, "改一个字节不该产生大段字面量")
        self.assertGreater(est["saved_ratio"], 0.9)

    def test_repeated_content(self):
        old = b"ABCD" * 1024
        new = b"ABCD" * 1024 + b"ABCD"
        self._round(old, new, 64)

    def test_block_size_boundaries(self):
        old = rand_bytes(300, seed=10)
        for bs in (D.MIN_BLOCK_SIZE, 100, D.MAX_BLOCK_SIZE):
            new = old[:150] + b"new" + old[150:]
            self._round(old, new, bs)

    def test_auto_block_size(self):
        old = rand_bytes(20000, seed=12)
        new = old + rand_bytes(50, seed=13)
        d = D.diff(old, new, None)
        self.assertGreaterEqual(d.block_size, D.MIN_BLOCK_SIZE)
        self.assertEqual(D.patch(old, d), new)

    def test_random_mutations_property(self):
        """随机变异 60 轮，每轮都必须无损往返。"""
        rnd = random.Random(20261121)
        for i in range(60):
            size = rnd.choice([0, 1, 63, 64, 65, 500, 4096, 20000])
            old = rand_bytes(size, seed=1000 + i)
            new = bytearray(old)
            for _ in range(rnd.randrange(0, 4)):
                op = rnd.randrange(3)
                if op == 0 and new:
                    at = rnd.randrange(len(new))
                    del new[at:at + rnd.randrange(1, 50)]
                elif op == 1:
                    at = rnd.randrange(len(new) + 1)
                    new[at:at] = rand_bytes(rnd.randrange(1, 50), seed=rnd.randrange(9999))
                elif new:
                    at = rnd.randrange(len(new))
                    new[at] = rnd.randrange(256)
            new = bytes(new)
            bs = rnd.choice([64, 128, 1024])
            d = D.diff(old, new, bs)
            self.assertEqual(D.patch(old, d), new,
                             "第 %d 轮往返失败（old=%d new=%d bs=%d）" % (i, len(old), len(new), bs))


class TestWire(unittest.TestCase):
    def test_encode_decode_ops(self):
        ops = [("c", 3), ("l", b"\x00\xffhello"), ("c", 0)]
        self.assertEqual(D.decode_ops(D.encode_ops(ops)), ops)

    def test_delta_json_round_trip(self):
        old = rand_bytes(2000, seed=20)
        new = old[:1000] + b"XYZ" + old[1000:]
        d = D.diff(old, new, 128)
        back = D.Delta.from_dict(d.to_dict())
        self.assertEqual(back.block_size, d.block_size)
        self.assertEqual(back.new_sha, d.new_sha)
        self.assertEqual(D.patch(old, back), new)

    def test_bad_version_rejected(self):
        with self.assertRaises(D.DeltaError):
            D.Delta.from_dict({"v": 99, "block_size": 64, "ops": []})

    def test_bad_ops_rejected(self):
        with self.assertRaises(D.DeltaError):
            D.decode_ops([["x", 1]])
        with self.assertRaises(D.DeltaError):
            D.decode_ops([["c"]])
        with self.assertRaises(D.DeltaError):
            D.decode_ops([["l", "!!!not-base64!!!"]])


class TestErrors(unittest.TestCase):
    def test_oversize_rejected(self):
        with self.assertRaises(D.DeltaError):
            D.diff(b"", b"x" * 100, 64, max_bytes=50)

    def test_baseline_mismatch(self):
        old = rand_bytes(500, seed=30)
        d = D.diff(old, old + b"tail", 64)
        with self.assertRaises(D.DeltaError) as ctx:
            D.patch(b"other content", d)
        self.assertIn("基线", str(ctx.exception))

    def test_bad_block_index(self):
        with self.assertRaises(D.DeltaError):
            D.patch_ops(b"x" * 64, [("c", 5)], 64)

    def test_unknown_op(self):
        with self.assertRaises(D.DeltaError):
            D.patch_ops(b"x" * 64, [("z", 1)], 64)

    def test_corrupt_result_detected(self):
        old = rand_bytes(500, seed=31)
        d = D.diff(old, old + b"tail", 64)
        d.ops = [("l", b"wrong")]
        with self.assertRaises(D.DeltaError) as ctx:
            D.patch(old, d)
        self.assertIn("校验失败", str(ctx.exception))


class TestEstimate(unittest.TestCase):
    def test_savings_reported(self):
        old = rand_bytes(100000, seed=40)
        new = bytearray(old)
        new[50000] ^= 0x01
        d = D.diff(old, bytes(new), 1024)
        est = D.estimate(d)
        self.assertEqual(est["total_bytes"], len(new))
        self.assertEqual(est["literal_bytes"] + est["copied_bytes"], est["total_bytes"])
        self.assertGreater(est["saved_ratio"], 0.95)

    def test_estimate_accepts_dict(self):
        old = rand_bytes(1000, seed=41)
        d = D.diff(old, old + b"x", 64)
        self.assertEqual(D.estimate(d.to_dict())["total_bytes"], 1001)


class TestFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mesh-delta-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name, data):
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def test_diff_and_patch_file(self):
        old = rand_bytes(50000, seed=50)
        new = old[:20000] + rand_bytes(500, seed=51) + old[20000:]
        a = self._write("a.bin", old)
        b = self._write("b.bin", new)
        d = D.diff_files(a, b, 1024)
        out = os.path.join(self.tmp, "out.bin")
        got = D.patch_file(a, d, out)
        self.assertEqual(got, D.sha256_hex(new))
        with open(out, "rb") as f:
            self.assertEqual(f.read(), new)

    def test_patch_file_rejects_wrong_base(self):
        a = self._write("a.bin", rand_bytes(1000, seed=52))
        b = self._write("b.bin", rand_bytes(1000, seed=53))
        d = D.diff_files(a, b, 64)
        with self.assertRaises(D.DeltaError):
            D.patch_file(b, d, os.path.join(self.tmp, "out.bin"))

    def test_patch_file_leaves_no_temp_on_failure(self):
        a = self._write("a.bin", rand_bytes(1000, seed=54))
        b = self._write("b.bin", rand_bytes(1000, seed=55))
        d = D.diff_files(a, b, 64)
        d.new_sha = "0" * 64          # 强制校验失败
        out = os.path.join(self.tmp, "out.bin")
        with self.assertRaises(D.DeltaError):
            D.patch_file(a, d, out)
        self.assertFalse(os.path.exists(out))
        self.assertFalse(os.path.exists(out + ".mesh-delta-tmp"))

    def test_empty_file_round_trip(self):
        a = self._write("a.bin", b"")
        b = self._write("b.bin", rand_bytes(300, seed=56))
        d = D.diff_files(a, b, 64)
        out = os.path.join(self.tmp, "out.bin")
        D.patch_file(a, d, out)
        with open(out, "rb") as f:
            self.assertEqual(len(f.read()), 300)


class TestChooseBlockSize(unittest.TestCase):
    def test_monotonic_and_clamped(self):
        self.assertEqual(D.choose_block_size(0), D.MIN_BLOCK_SIZE)
        self.assertEqual(D.choose_block_size(10), D.MIN_BLOCK_SIZE)
        self.assertGreaterEqual(D.choose_block_size(1 << 40), D.MIN_BLOCK_SIZE)
        self.assertLessEqual(D.choose_block_size(1 << 60), D.MAX_BLOCK_SIZE)
        prev = 0
        for size in (100, 1000, 10000, 100000, 1000000):
            cur = D.choose_block_size(size)
            self.assertGreaterEqual(cur, prev)
            prev = cur


class TestSigOnly(unittest.TestCase):
    """rsync 的真实流程：发送方只有签名表，没有旧文件本体。"""

    def test_sender_without_old_file(self):
        old = rand_bytes(40960, seed=70)
        new = old[:20000] + rand_bytes(300, seed=71) + old[20000:]
        bs = 1024

        # 接收方本地算签名表（不需要把旧文件发出去）
        sig = D.block_signature(old, bs)
        old_sha = D.sha256_hex(old)

        # 发送方只有 sig + new，照样能产出差分
        d = D.diff_with_sig(sig, new, bs, old_sha=old_sha)
        self.assertEqual(d.old_sha, old_sha)
        self.assertEqual(d.new_sha, D.sha256_hex(new))

        # 接收方本地重建
        self.assertEqual(D.patch(old, d), new)

        est = D.estimate(d)
        self.assertLess(est["literal_bytes"], 2000, "只有签名表也应能复用旧块")

    def test_sig_only_identical(self):
        old = rand_bytes(4096, seed=72)
        sig = D.block_signature(old, 64)
        d = D.diff_with_sig(sig, old, 64, old_sha=D.sha256_hex(old))
        self.assertTrue(all(k == "c" for k, _ in d.ops))
        self.assertEqual(D.patch(old, d), old)

    def test_bad_sig_shape_rejected(self):
        with self.assertRaises(D.DeltaError):
            D.diff_ops_with_sig([("not", "a", "triple", 1)], b"abc", 64)

    def test_sig_must_be_list(self):
        with self.assertRaises(D.DeltaError):
            D.diff_ops_with_sig({"bad": 1}, b"abc", 64)


if __name__ == "__main__":
    unittest.main(verbosity=2)
