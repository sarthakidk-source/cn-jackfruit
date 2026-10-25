"""
Newline-delimited JSON (NDJSON) messaging over a TCP socket.

One message = one JSON object on one line, ended by '\n'.
TCP is a byte stream, so a single recv() may return half a message or several
messages at once. MessageChannel keeps a buffer and only returns a message once a
complete line has arrived.
"""
import json
import socket
import threading

MAX_LINE_BYTES = 1 << 20  # reject absurdly long lines (1 MiB)

# Message types
REGISTER = "REGISTER"          # worker -> manager : {"worker_id": ..., "slowdown": ...}
REGISTER_ACK = "REGISTER_ACK"  # manager -> worker
TASK = "TASK"                  # manager -> worker : {"task_id": ..., "n": ..., "work_ms": ...}
RESULT = "RESULT"              # worker -> manager : {"task_id": ..., "status": ..., "output": ...}
ERROR = "ERROR"                # either direction  : {"reason": ...}


class ProtocolError(Exception):
    """The peer sent something that is not a valid message."""


class MessageChannel:
    def __init__(self, sock):
        self.sock = sock
        self._buf = bytearray()
        self._send_lock = threading.Lock()  # one thread sends at a time

    def send(self, msg):
        """Send one message (a dict). json.dumps never emits a raw newline,
        so the trailing '\n' is an unambiguous message boundary."""
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8") + b"\n"
        with self._send_lock:
            self.sock.sendall(data)

    def recv(self):
        """Return the next message as a dict.
        Returns None if the peer closed the connection cleanly.
        Raises ProtocolError for malformed data and OSError (incl. socket.timeout)
        for socket problems."""
        while True:
            nl = self._buf.find(b"\n")
            if nl != -1:
                line = bytes(self._buf[:nl])
                del self._buf[:nl + 1]
                if not line.strip():
                    continue  # skip blank lines
                try:
                    msg = json.loads(line.decode("utf-8"))
                except (UnicodeDecodeError, ValueError) as e:
                    raise ProtocolError("bad JSON: %s" % e)
                if not isinstance(msg, dict) or "type" not in msg:
                    raise ProtocolError("message must be a JSON object with a 'type'")
                return msg
            if len(self._buf) > MAX_LINE_BYTES:
                raise ProtocolError("line too long")
            chunk = self.sock.recv(4096)
            if not chunk:  # peer closed
                if self._buf.strip():
                    raise ProtocolError("connection closed in the middle of a message")
                return None
            self._buf.extend(chunk)

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        