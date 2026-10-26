import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from app.protocol import MessageChannel, ProtocolError  # noqa: E402


class ProtocolTests(unittest.TestCase):
    def pair(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        return a, MessageChannel(b)

    def test_roundtrip(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        tx, rx = MessageChannel(a), MessageChannel(b)
        msg = {"type": "TASK", "task_id": "t1", "n": 5}
        tx.send(msg)
        self.assertEqual(rx.recv(), msg)

    def test_partial_reads_byte_by_byte(self):
        a, rx = self.pair()
        data = b'{"type":"TASK","task_id":"t1"}\n'

        def drip():
            for i in range(len(data)):
                a.send(data[i:i + 1])
                time.sleep(0.001)
        t = threading.Thread(target=drip)
        t.start()
        self.assertEqual(rx.recv()["task_id"], "t1")
        t.join()

    def test_two_messages_in_one_chunk(self):
        a, rx = self.pair()
        a.sendall(b'{"type":"A"}\n{"type":"B"}\n')
        self.assertEqual(rx.recv()["type"], "A")
        self.assertEqual(rx.recv()["type"], "B")

    def test_clean_close_returns_none(self):
        a, rx = self.pair()
        a.close()
        self.assertIsNone(rx.recv())

    def test_close_mid_message_raises(self):
        a, rx = self.pair()
        a.sendall(b'{"type":"TA')
        a.close()
        with self.assertRaises(ProtocolError):
            rx.recv()

    def test_bad_json_raises(self):
        a, rx = self.pair()
        a.sendall(b"this is not json\n")
        with self.assertRaises(ProtocolError):
            rx.recv()

    def test_missing_type_raises(self):
        a, rx = self.pair()
        a.sendall(b'{"x":1}\n')
        with self.assertRaises(ProtocolError):
            rx.recv()

    def test_non_object_and_non_string_type_raise(self):
        for bad in (b"[1,2]\n", b'"str"\n', b"42\n", b"null\n", b'{"type":5}\n'):
            with self.subTest(bad=bad):
                a, rx = self.pair()
                a.sendall(bad)
                with self.assertRaises(ProtocolError):
                    rx.recv()

    def test_invalid_utf8_raises(self):
        a, rx = self.pair()
        a.sendall(b"\xff\xfe\xfd\n")
        with self.assertRaises(ProtocolError):
            rx.recv()

    def test_blank_lines_are_skipped(self):
        a, rx = self.pair()
        a.sendall(b'\n\n  \n{"type":"A"}\n')
        self.assertEqual(rx.recv()["type"], "A")

    def test_overlong_line_rejected(self):
        a, rx = self.pair()
        from app.protocol import MAX_LINE_BYTES

        def flood():
            try:
                a.sendall(b"x" * (MAX_LINE_BYTES + 100000))
            except OSError:
                pass
        t = threading.Thread(target=flood, daemon=True)
        t.start()
        with self.assertRaises(ProtocolError):
            rx.recv()
        rx.close()
        t.join(timeout=5)

    def test_unicode_and_large_message_roundtrip(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        tx, rx = MessageChannel(a), MessageChannel(b)
        msg = {"type": "T", "text": "héllo ✓ \n newline inside string", "blob": "z" * 500000}
        t = threading.Thread(target=tx.send, args=(msg,), daemon=True)
        t.start()
        self.assertEqual(rx.recv(), msg)
        t.join(timeout=5)

    def test_concurrent_senders_never_interleave(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        tx, rx = MessageChannel(a), MessageChannel(b)
        n_threads, per = 4, 50

        def spam(k):
            for i in range(per):
                tx.send({"type": "S", "who": k, "i": i, "pad": "p" * 3000})
        threads = [threading.Thread(target=spam, args=(k,), daemon=True) for k in range(n_threads)]
        for t in threads:
            t.start()
        seen = {}
        for _ in range(n_threads * per):
            m = rx.recv()
            seen.setdefault(m["who"], []).append(m["i"])
        for k in range(n_threads):
            self.assertEqual(seen[k], list(range(per)))   # per-sender order preserved

    def test_encode_decode_helpers(self):
        from app.protocol import decode_line, encode_message, require
        data = encode_message({"type": "X", "a": 1})
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual(data.count(b"\n"), 1)
        self.assertEqual(decode_line(data.strip()), {"type": "X", "a": 1})
        with self.assertRaises(ProtocolError):
            require({"type": "X"}, "a", "b")
        require({"type": "X", "a": 1, "b": 2}, "a", "b")


if __name__ == "__main__":
    unittest.main()
    