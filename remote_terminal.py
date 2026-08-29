#!/usr/bin/env python3
"""Remote shell access to the DDoS-prone Heroku userbot host via Telegram.

Bridge mechanic (confirmed live 2026-08-03): a message ".terminal <cmd>" sent
by @Test_kopatel_bot to the owner's private chat is picked up by the owner's
own MTProto userbot session (already authorized via a one-time ".owneradd"
reply), which creates a NEW message from the owner's account and edits it in
place as the remote command runs, finishing with a "Время выполнения: Ns"
line once done.

Usage:
    python3 remote_terminal.py "<shell command>" [timeout_seconds]

Prints the final message text and exits with the remote command's exit code
(or 2 if the run timed out / couldn't be confirmed).
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse

BOT_TOKEN = os.environ.get("TESTBOT_BOT_TOKEN", "")
CHAT_ID = int(os.environ.get("TESTBOT_CHAT_ID", "8480261623"))
API = f"https://api.telegram.org/bot{BOT_TOKEN}"

DONE_MARKER = "Время выполнения"
EXIT_CODE_RE = re.compile(r"Код выхода\s+(-?\d+)")


def _call(method, **params):
    if not BOT_TOKEN:
        raise RuntimeError("TESTBOT_BOT_TOKEN is not configured")
    data = urllib.parse.urlencode(params).encode()
    with urllib.request.urlopen(f"{API}/{method}", data=data, timeout=30) as r:
        return json.loads(r.read())


def _get_updates(offset=None, timeout=0):
    kwargs = {"timeout": timeout}
    if offset is not None:
        kwargs["offset"] = offset
    return _call("getUpdates", **kwargs)


def _flush_and_get_offset():
    """Drain any stale queued updates, return the offset for new ones only."""
    r = _get_updates(timeout=0)
    results = r.get("result", [])
    if not results:
        return 0
    max_id = max(u["update_id"] for u in results)
    _get_updates(offset=max_id + 1, timeout=0)
    return max_id + 1


def run_remote(cmd, timeout=60):
    offset = _flush_and_get_offset()

    sent = _call("sendMessage", chat_id=CHAT_ID, text=f".terminal {cmd}")
    if not sent.get("ok"):
        return {"ok": False, "error": f"sendMessage failed: {sent}"}
    sent_date = sent["result"]["date"]

    target_id = None
    latest_text = None
    deadline = time.time() + timeout

    while time.time() < deadline:
        r = _get_updates(offset=offset, timeout=2)
        for u in r.get("result", []):
            offset = u["update_id"] + 1
            msg = u.get("message") or u.get("edited_message")
            if not msg:
                continue
            if target_id is None:
                if (
                    u.get("message")
                    and msg.get("from", {}).get("id") == CHAT_ID
                    and msg.get("date", 0) >= sent_date
                    and msg.get("text", "").startswith("⌨")  # "⌨️ Системная команда ..."
                ):
                    target_id = msg["message_id"]
                    latest_text = msg.get("text", "")
            elif msg.get("message_id") == target_id:
                latest_text = msg.get("text", latest_text)

        if latest_text and DONE_MARKER in latest_text:
            m = EXIT_CODE_RE.search(latest_text)
            code = int(m.group(1)) if m else 0
            return {"ok": True, "text": latest_text, "exit_code": code}

    return {
        "ok": False,
        "error": "timeout waiting for remote result",
        "text": latest_text,
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: remote_terminal.py '<command>' [timeout_seconds]", file=sys.stderr)
        sys.exit(2)
    cmd = sys.argv[1]
    to = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    result = run_remote(cmd, timeout=to)
    print(result.get("text") or result.get("error"))
    sys.exit(result.get("exit_code", 2) if result.get("ok") else 2)
