#!/usr/bin/env python3
"""Command + Ask + OCR queue for Hermes server."""

import json
import os
import re
import time
import base64
import socketserver
import threading
import urllib.request
import urllib.parse
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

from cryptography.fernet import Fernet

# Mirrors claude_watcher.py's _key_file/_get_key/_encrypt/_decrypt/
# _sessions_file exactly (duplicated, not imported -- separate processes).
# See that file for the full rationale/caveats; short version: this is a
# casual "don't read as a plain chat_id->session_id map at a glance"
# measure, not a defense against someone with root on this box.
_JARVIS_ASK_DIR = os.environ.get("JARVIS_ASK_DIR", os.path.dirname(os.path.abspath(__file__)))


def _session_key_file(instance_id):
    return os.path.join(_JARVIS_ASK_DIR, f".session_key_{instance_id}")


def _get_session_key(instance_id):
    path = _session_key_file(instance_id)
    with open(path, "rb") as f:
        return f.read().strip()


def _sessions_enc_file(instance_id):
    # No special case for "andrey" -- mirrors claude_watcher.py's
    # _sessions_file after the 2026-08-04 naming-symmetry fix.
    return os.path.join(_JARVIS_ASK_DIR, f"ask_sessions_{instance_id}.enc")

OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
QUEUE_FILE = "/tmp/hermes_cmd_queue.json"
RESULT_FILE = "/tmp/hermes_cmd_result.json"
# Per-request_id files (not single shared files) -- claude_watcher.py now
# processes multiple .ask calls concurrently (one thread per chat), so a
# single shared queue/result file would let concurrent requests clobber
# each other's state.
ASK_QUEUE_DIR = os.environ.get("JARVIS_ASK_QUEUE_DIR", "/tmp/hermes_ask_queue/")
ASK_RESULT_DIR = os.environ.get("JARVIS_ASK_RESULT_DIR", "/tmp/hermes_ask_result/")
os.makedirs(ASK_QUEUE_DIR, exist_ok=True)
os.makedirs(ASK_RESULT_DIR, exist_ok=True)
# CodexAsk uses a separate queue so Claude and Codex workers can never race
# to claim each other's requests. The HTTP server remains shared because it
# is transport infrastructure, not a model backend.
XASK_QUEUE_DIR = os.environ.get("JARVIS_XASK_QUEUE_DIR", "/tmp/hermes_xask_queue/")
XASK_RESULT_DIR = os.environ.get("JARVIS_XASK_RESULT_DIR", "/tmp/hermes_xask_result/")
XASK_RESET_DIR = os.environ.get("JARVIS_XASK_RESET_DIR", "/tmp/hermes_xask_reset/")
for _path in (XASK_QUEUE_DIR, XASK_RESULT_DIR, XASK_RESET_DIR):
    os.makedirs(_path, exist_ok=True)

# Real tool-call relay (2026-08-11), opposite direction from /ask: a local
# MCP server (mcp_group_tools.py, spawned by `claude -p --mcp-config=...`
# on THIS host) enqueues a tool call here, and the REMOTE userbot host
# (claude_ask.py's tool_call_watcher loop, the only place with a live
# Telethon session) polls /tool_call_pending for it, executes the real
# action, and posts the result to /tool_call_result. Per-request_id files,
# same reasoning as ASK_QUEUE_DIR/ASK_RESULT_DIR -- concurrent tool calls
# (e.g. two different chats both mid-.ask) must not clobber each other.
TOOL_QUEUE_DIR = os.environ.get("JARVIS_TOOL_QUEUE_DIR", "/tmp/hermes_tool_queue/")
TOOL_RESULT_DIR = os.environ.get("JARVIS_TOOL_RESULT_DIR", "/tmp/hermes_tool_result/")
os.makedirs(TOOL_QUEUE_DIR, exist_ok=True)
os.makedirs(TOOL_RESULT_DIR, exist_ok=True)
_TOOL_QUEUE_LOCK = threading.Lock()
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_SAFE_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
MAX_UPLOAD_BYTES = 50 * 1024 * 1024


def _safe_request_id(value):
    return isinstance(value, str) and bool(_SAFE_ID_RE.fullmatch(value))


def _safe_instance_id(value):
    return isinstance(value, str) and bool(_SAFE_INSTANCE_RE.fullmatch(value))


def _pop_pending_tool_call(instance_id: str):
    """Atomically claim one pending tool call for this instance -- under
    ThreadingHTTPServer, two overlapping /tool_call_pending polls (unlikely
    at this scale, but not impossible) must not both grab the same file."""
    with _TOOL_QUEUE_LOCK:
        try:
            names = sorted(os.listdir(TOOL_QUEUE_DIR))
        except FileNotFoundError:
            return None
        for name in names:
            if not name.endswith(".json"):
                continue
            path = os.path.join(TOOL_QUEUE_DIR, name)
            try:
                with open(path) as f:
                    data = json.load(f)
            except Exception:
                continue
            if data.get("instance_id") != instance_id:
                continue
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
            data["request_id"] = name[:-5]
            return data
        return None


def classify_semantic(text: str, condition: str, timeout: float = 25.0) -> str:
    """Tier 1 trigger classifier (Phase 4): a cheap 3-way call ("yes"/"no"/
    "unsure") for conditions that don't reduce to a keyword/link/button
    check. Returns "unsure", not just "no", on genuine ambiguity -- that's
    load-bearing, not decorative: a keyword-prefiltered "verify" trigger
    (see claude_ask.py's _resolve_verified_action) auto-acts on a confident
    yes/no but escalates an "unsure" to a human via confirm buttons, per
    the owner directly (2026-08-09) -- "delete/ignore automatically only
    when actually confident, ask a human only for genuinely doubtful
    cases" was the whole point, not "keyword hit -> always ask".

    Deliberately NOT a direct external API call (tried OpenRouter first,
    even with an Anthropic model on it -- wrong call, per the owner
    directly: the entire point of this project running on `claude -p`
    subscription auth instead of metered API billing is to never pay per
    token anywhere in the pipeline, and OpenRouter bills per token
    regardless of which model you pick on it). Routed through the SAME
    queue/result files claude_watcher.py already polls for /ask, tagged
    mode="classify" -- that mode uses --model haiku (cheapest/fastest,
    still flat-subscription) and no session resume (stateless, matching
    search/translate). This function just blocks synchronously inside its
    own request thread (ThreadingHTTPServer, so it doesn't stall other
    endpoints) waiting for claude_watcher.py to pick it up and answer --
    from claude_ask.py's side, /classify still looks like one plain
    request/response call, same as before."""
    if not condition or not text:
        return "unsure"
    req_id = str(uuid.uuid4())
    prompt = (
        "Условие: " + condition + "\n\nСообщение: " + text[:2000]
    )
    try:
        with open(os.path.join(ASK_QUEUE_DIR, f"{req_id}.json"), "w") as f:
            json.dump({
                "question": prompt, "chat_id": "classify", "request_id": req_id,
                "mode": "classify", "ts": time.time(), "done": False,
            }, f)
    except Exception:
        return "unsure"

    result_path = os.path.join(ASK_RESULT_DIR, f"{req_id}.json")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(result_path):
            try:
                with open(result_path) as f:
                    data = json.load(f)
            except Exception:
                data = None
            if data and data.get("done"):
                try:
                    os.remove(result_path)
                except FileNotFoundError:
                    pass
                answer = (data.get("answer") or "").strip().lower()
                # "нет" and "не уверен" both start with "не" -- order
                # matters here, "да"/"нет" are checked as whole-prefix
                # first, anything else (including a genuine "не уверен",
                # empty, or unparseable) falls through to "unsure" as the
                # safe default rather than silently guessing either way.
                if answer.startswith(("да", "yes")):
                    return "yes"
                if answer.startswith(("нет", "no")):
                    return "no"
                return "unsure"
        time.sleep(0.3)
    return "unsure"


def ocr_image(b64: str, question: str = "Извлеки весь текст с этого изображения.") -> str | None:
    """OCR via OpenRouter vision model"""
    if not OPENROUTER_KEY:
        return "OCR unavailable: OPENROUTER_API_KEY is not configured"
    try:
        data = json.dumps({
            "model": "openai/gpt-4o-mini",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": question},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
                ]
            }],
            "max_tokens": 1000,
        }).encode()
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPENROUTER_KEY}",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            result = json.loads(r.read())
        return result["choices"][0]["message"]["content"]
    except Exception as e:
        return f"OCR error: {e}"


class Queue(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/upload":
            content_type = self.headers.get("Content-Type", "")
            if "multipart" not in content_type:
                self.send_response(400)
                self.end_headers()
                return
            try:
                content_length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                self._json({"status": "error", "message": "Некорректный Content-Length"})
                return
            if content_length < 0 or content_length > MAX_UPLOAD_BYTES:
                self._json({"status": "error", "message": "Файл слишком большой"})
                return
            body_raw = self.rfile.read(content_length)
            # Find filename in Content-Disposition
            fname_match = re.search(rb'filename="([^"]+)"', body_raw)
            raw_fname = fname_match.group(1).decode("utf-8", "replace") if fname_match else "uploaded_file"
            normalized_fname = raw_fname.replace("\\", "/")
            fname = os.path.basename(normalized_fname)
            if (
                not fname or fname in (".", "..") or "\x00" in fname
                or ".." in normalized_fname.split("/")
            ):
                self._json({"status": "error", "message": "Некорректное имя файла"})
                return
            fname = fname[:180]
            # Find the binary part (after \r\n\r\n after the Content-Type line)
            parts = body_raw.split(b"\r\n\r\n", 2)
            if len(parts) >= 3:
                file_data = parts[2].rsplit(b"\r\n--", 1)[0]
            elif len(parts) >= 2:
                file_data = parts[1].rsplit(b"\r\n--", 1)[0]
            else:
                file_data = b""
            # Do not let a client overwrite a predictable /tmp file (or a
            # file uploaded by another request).  The returned path is the
            # only name downstream needs for the subsequent /download call.
            save_path = os.path.join("/tmp", f"jarvis_upload_{uuid.uuid4().hex}_{fname}")
            with open(save_path, "wb") as f:
                f.write(file_data)
            self._json({"status": "ok", "path": save_path, "filename": fname})
            return

        body = self._body()

        if self.path == "/cmd":
            with open(QUEUE_FILE, "w") as f:
                json.dump({"cmd": body.get("cmd", ""), "ts": time.time()}, f)
            self._json({"status": "queued"})

        elif self.path == "/result":
            with open(RESULT_FILE, "w") as f:
                json.dump(body, f)
            self._json({"status": "ok"})

        elif self.path in ("/ask", "/xask"):
            req_id = body.get("request_id", "")
            if not _safe_request_id(req_id):
                self._json({"status": "error", "message": "Некорректный request_id"})
                return
            ask_data = {
                "question": body.get("question", ""),
                "ts": time.time(),
                "done": False,
            }
            for k in (
                "chat_id", "request_id", "message_id", "topic_id", "mode",
                "instance_id", "requester_id",
            ):
                if k in body:
                    ask_data[k] = body[k]
            queue_dir = XASK_QUEUE_DIR if self.path == "/xask" else ASK_QUEUE_DIR
            with open(os.path.join(queue_dir, f"{req_id}.json"), "w") as f:
                json.dump(ask_data, f)
            self._json({"status": "queued"})

        elif self.path == "/tool_call":
            req_id = body.get("request_id", "")
            if not _safe_request_id(req_id):
                self._json({"status": "error", "message": "Некорректный request_id"})
                return
            call_data = {
                "instance_id": body.get("instance_id", ""),
                "chat_id": body.get("chat_id", ""),
                "tool": body.get("tool", ""),
                "args": body.get("args", {}),
                "requester_id": body.get("requester_id"),
                "ts": time.time(),
            }
            with open(os.path.join(TOOL_QUEUE_DIR, f"{req_id}.json"), "w") as f:
                json.dump(call_data, f)
            self._json({"status": "queued"})

        elif self.path == "/tool_call_result":
            req_id = body.get("request_id", "")
            if not _safe_request_id(req_id):
                self._json({"status": "error", "message": "Некорректный request_id"})
                return
            with open(os.path.join(TOOL_RESULT_DIR, f"{req_id}.json"), "w") as f:
                json.dump({"done": True, "result": body.get("result", "")}, f)
            self._json({"status": "ok"})

        elif self.path == "/ocr":
            image_b64 = body.get("image", "")
            question = body.get("question", "Извлеки весь текст с этого изображения.")
            if image_b64:
                text = ocr_image(image_b64, question)
                self._json({"text": text})
            else:
                self._json({"text": None})

        elif self.path == "/classify":
            result = classify_semantic(body.get("text", ""), body.get("condition", ""))
            self._json({"result": result})

        elif self.path == "/xreset":
            chat_id = body.get("chat_id", "")
            instance_id = body.get("instance_id") or "andrey_codex"
            if not chat_id or not _safe_instance_id(instance_id):
                self._json({"status": "error", "message": "Некорректные chat_id или instance_id"})
                return
            reset_id = f"reset_{uuid.uuid4().hex}"
            with open(os.path.join(XASK_RESET_DIR, reset_id + ".json"), "w") as f:
                json.dump({"chat_id": str(chat_id), "instance_id": instance_id}, f)
            self._json({"status": "ok", "message": f"Codex-сессия чата {chat_id} будет сброшена"})

        elif self.path == "/reset":
            chat_id = body.get("chat_id", "")
            if chat_id:
                # claude_watcher.py now uses real Claude Code sessions
                # (--resume) per chat_id, tracked in this file -- clearing
                # the entry here is what makes the next .ask start a
                # genuinely fresh session instead of resuming the old one.
                #
                # One sessions file per userbot instance (mirrors
                # claude_watcher.py's _sessions_file -- kept duplicated here
                # rather than imported, these run as two separate processes):
                # a Telegram chat_id is the PEER's id, not scoped to which
                # account is asking, so two different userbots messaging the
                # same mutual contact would otherwise share -- and a /reset
                # from one would wipe -- the other's session with that
                # contact. Missing/default instance_id keeps the original
                # filename so the existing deployed client needs no change.
                instance_id = body.get("instance_id") or "andrey"
                if not _safe_instance_id(instance_id):
                    self._json({"status": "error", "message": "Некорректный instance_id"})
                    return
                sessions_file = _sessions_enc_file(instance_id)
                cleared = False
                if os.path.exists(sessions_file):
                    try:
                        key = _get_session_key(instance_id)
                        with open(sessions_file, "rb") as f:
                            token = base64.urlsafe_b64encode(f.read())
                        sessions = json.loads(Fernet(key).decrypt(token))
                    except Exception:
                        sessions = {}
                    if sessions.pop(str(chat_id), None) is not None:
                        raw = base64.urlsafe_b64decode(Fernet(key).encrypt(json.dumps(sessions).encode()))
                        with open(sessions_file, "wb") as f:
                            f.write(raw)
                        os.chmod(sessions_file, 0o600)
                        cleared = True
                msg = f"Сессия чата {chat_id} сброшена" if cleared else "Сессия уже пуста"
                self._json({"status": "ok", "message": msg})
            else:
                self._json({"status": "error", "message": "Нет chat_id"})

    def do_GET(self):
        if self.path.startswith("/download"):
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            filepath = params.get("path", [None])[0]
            # Scoped to /tmp -- the only directory this app itself ever
            # writes into (/upload's save_path, claude_watcher.py's WORKDIR
            # for SEND_FILE). Without this, a plain GET with no auth at all
            # could read any file the `mishin` user can read (confirmed
            # live: /etc/passwd, jarvis-ask/.session_key_*, state*.json) --
            # and this endpoint is proxied to the public internet via
            # Tailscale Funnel. realpath() resolves both symlinks and any
            # ../ traversal before the prefix check, so neither can escape
            # the allowed root.
            resolved = os.path.realpath(filepath) if filepath else None
            allowed = resolved and (resolved == "/tmp" or resolved.startswith("/tmp" + os.sep))
            if allowed and os.path.isfile(resolved):
                filepath = resolved
                with open(filepath, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                fname = os.path.basename(filepath)
                # BaseHTTPRequestHandler.send_header encodes header VALUES
                # as latin-1 -- any non-latin1 filename (Cyrillic, emoji,
                # etc, which Jarvis routinely generates) raises
                # UnicodeEncodeError mid-response, which breaks the HTTP
                # response the Tailscale funnel is proxying and surfaces to
                # the caller as a bare 502 with no useful error anywhere
                # (caught live 2026-08-13, jarvis-ask's send_file tool).
                # RFC 5987: plain ASCII fallback filename= plus the real
                # name percent-encoded in filename*=UTF-8''... -- nothing
                # downstream here actually reads this header at all
                # (_send_file_action gets its filename from the request
                # path client-side, not this response), so correctness of
                # the fallback name doesn't matter, only that it's ASCII.
                try:
                    self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                except UnicodeEncodeError:
                    self.send_header(
                        "Content-Disposition",
                        f"attachment; filename=\"file\"; filename*=UTF-8''{urllib.parse.quote(fname)}",
                    )
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._json({"error": "File not found"})
            return

        if self.path == "/cmd":
            if os.path.exists(QUEUE_FILE):
                with open(QUEUE_FILE) as f:
                    cmd = json.load(f)
                os.remove(QUEUE_FILE)
                self._json(cmd)
            else:
                self._json({"cmd": None})

        elif self.path == "/result":
            if os.path.exists(RESULT_FILE):
                with open(RESULT_FILE) as f:
                    result = json.load(f)
                self._json(result)
            else:
                self._json({"stdout": "", "stderr": "", "rc": -1})

        elif self.path.startswith("/ask") or self.path.startswith("/xask"):
            # Always return the file's actual contents, not just when
            # done -- lets a "progress" field (live thought/tool-call
            # updates from claude_watcher.py) flow through to the poller
            # on the other host. The done:true contract is unchanged.
            # Keyed by request_id now (query param) -- concurrent .ask
            # calls each poll their own result file, not a shared one.
            qs = urllib.parse.urlparse(self.path).query
            req_id = urllib.parse.parse_qs(qs).get("request_id", [""])[0]
            result_dir = XASK_RESULT_DIR if self.path.startswith("/xask") else ASK_RESULT_DIR
            result_path = os.path.join(result_dir, f"{req_id}.json") if _safe_request_id(req_id) else None
            if result_path and os.path.exists(result_path):
                try:
                    with open(result_path) as f:
                        data = json.load(f)
                    self._json(data)
                    return
                except Exception:
                    pass
            self._json({"done": False, "answer": None})

        elif self.path.startswith("/tool_call_pending"):
            # Polled by the REMOTE userbot host (claude_ask.py's
            # tool_call_watcher loop, through the funnel) -- pops and
            # returns the oldest pending call for this instance_id, or
            # {"tool": None} if the queue's empty for it.
            qs = urllib.parse.urlparse(self.path).query
            instance_id = urllib.parse.parse_qs(qs).get("instance_id", [""])[0]
            data = _pop_pending_tool_call(instance_id) if instance_id else None
            self._json(data or {"tool": None})

        elif self.path.startswith("/tool_call"):
            # Polled LOCALLY by mcp_group_tools.py (same host as this
            # process, no funnel involved) for the result the remote host
            # posts back via POST /tool_call_result.
            qs = urllib.parse.urlparse(self.path).query
            req_id = urllib.parse.parse_qs(qs).get("request_id", [""])[0]
            result_path = os.path.join(TOOL_RESULT_DIR, f"{req_id}.json") if _safe_request_id(req_id) else None
            if result_path and os.path.exists(result_path):
                try:
                    with open(result_path) as f:
                        data = json.load(f)
                    self._json(data)
                    return
                except Exception:
                    pass
            self._json({"done": False, "result": None})

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def _json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def log_message(self, fmt, *args):
        pass


class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    ThreadingHTTPServer(
        (
            os.environ.get("JARVIS_QUEUE_BIND", "0.0.0.0"),
            int(os.environ.get("JARVIS_QUEUE_PORT", "9092")),
        ),
        Queue,
    ).serve_forever()
