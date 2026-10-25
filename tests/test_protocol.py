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


if __name__ == "__main__":
    unittest.main()
    