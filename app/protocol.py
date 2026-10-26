"""
Newline-delimited JSON (NDJSON) messaging over a TCP socket.

One message = one JSON object on one line, ended by '\n'.
TCP is a byte stream, so a single recv() may return half a message or several
messages at once. MessageChannel keeps a buffer and only returns a message once a
complete line has arrived.

Message catalogue (every message is a JSON object with a "type" field)
----------------------------------------------------------------------
worker  -> manager   REGISTER      {worker_id, slowdown, version}
manager -> worker    REGISTER_ACK  {worker_id, heartbeat_interval}
manager -> worker    TASK          {task_id, attempt, task_type, n, work_ms,
                                    payload, payload_bytes}
worker  -> manager   RESULT        {task_id, attempt, worker_id, status,
                                    output, payload_crc, elapsed_ms | reason}
worker  -> manager   HEARTBEAT     {worker_id, inflight, completed}
manager -> worker    HEARTBEAT_ACK {}
manager -> worker    SHUTDOWN      {}
client  -> manager   SUBMIT        {tasks: [task spec, ...]}
manager -> client    SUBMIT_ACK    {accepted, duplicates, rejected}
client  -> manager   STATUS_REQ    {task_ids?, workers?}
manager -> client    STATUS        {tasks, workers, summary}
client  -> manager   METRICS_REQ   {}
manager -> client    METRICS       {...full metrics dump...}
either direction     ERROR         {reason}

Note on timeouts: if a socket timeout interrupts send(), the message may be
half written, so the channel must be considered broken and closed. All callers in
this project close the channel on any OSError.
"""
import json
import socket
import threading

PROTOCOL_VERSION = 1
MAX_LINE_BYTES = 4 << 20  # reject absurdly long lines (4 MiB)

# Message types -------------------------------------------------------------
REGISTER = "REGISTER"
REGISTER_ACK = "REGISTER_ACK"
TASK = "TASK"
RESULT = "RESULT"
HEARTBEAT = "HEARTBEAT"
HEARTBEAT_ACK = "HEARTBEAT_ACK"
SHUTDOWN = "SHUTDOWN"
SUBMIT = "SUBMIT"
SUBMIT_ACK = "SUBMIT_ACK"
STATUS_REQ = "STATUS_REQ"
STATUS = "STATUS"
METRICS_REQ = "METRICS_REQ"
METRICS = "METRICS"
ERROR = "ERROR"


class ProtocolError(Exception):
    """The peer sent something that is not a valid message."""


def require(msg, *fields):
    """Raise ProtocolError unless every named field is present in msg."""
    missing = [f for f in fields if f not in msg]
    if missing:
        raise ProtocolError("%s message missing field(s): %s"
                            % (msg.get("type"), ", ".join(missing)))


def decode_line(line):
    """Decode one NDJSON line (bytes) into a message dict, or raise ProtocolError."""
    try:
        msg = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise ProtocolError("bad JSON: %s" % e)
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        raise ProtocolError("message must be a JSON object with a string 'type'")
    return msg


def encode_message(msg):
    """Encode a message dict as one NDJSON line (bytes). json.dumps never emits a
    raw newline, so the trailing newline is an unambiguous message boundary."""
    return json.dumps(msg, separators=(",", ":")).encode("utf-8") + b"\n"


class MessageChannel:
    def __init__(self, sock):
        self.sock = sock
        self._buf = bytearray()
        self._send_lock = threading.Lock()  # one thread sends at a time

    def send(self, msg):
        """Send one message (a dict)."""
        data = encode_message(msg)
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
                return decode_line(line)
            if len(self._buf) > MAX_LINE_BYTES:
                raise ProtocolError("line too long")
            chunk = self.sock.recv(65536)
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
        try:
            self.sock.close()
        except OSError:
            pass
