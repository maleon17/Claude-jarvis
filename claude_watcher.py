#!/usr/bin/env python3
"""Ask watcher (Claude backend) — drop-in replacement for ask_watcher.py.

Queue/result file protocol is UNCHANGED (same paths, same JSON shapes) so
funnel_proxy.py doesn't need any changes. cmd_queue.py needed ONE small,
backward-compatible fix: its GET /ask handler used to hardcode
{"done": false, "answer": null} whenever the result file wasn't done yet,
discarding whatever was actually in the file. That made live progress
impossible to relay cross-host (this watcher runs on lightrag; the userbot
runs on a different host — there's no shared filesystem, only this HTTP
queue). Now it just returns the file's actual contents always; the
done:true contract and everything else is untouched.

Backend swapped: DeepSeek v4 Pro via OpenRouter -> local `claude -p`
(subscription auth, no API key, no OpenRouter key on disk anymore).

Most of the old custom tool surface (terminal, create_file, web_search,
web_extract) is gone: Claude has its own native Bash/Read/Write/WebSearch/
WebFetch tools and uses them directly, autonomously, within a single call —
no marker/recursion dance needed for any of that anymore.

Only the two things that genuinely require a live Telegram/MTProto session
— searching chat history and pulling more of it — still can't be done by
Claude itself, so they stay a marker round-trip resolved by the userbot
module (claude_ask.py), same idea as before but reduced from 8 markers to 2.

Live progress: now streams (--output-format=stream-json) instead of a
single blocking call, and publishes a throttled HTML progress snapshot to
RESULT_FILE (done:false + a "progress" field) as it goes -- the same
thought/tool-call rendering scheme as the Telegram bridge (bridge.py):
a thought (🧠) is the anchor, tool calls (🔧) render under it with their
StdOut/StdErr as separate blocks, and a new thought clears everything. The
userbot module polls this and live-edits the same message, then replaces
it all with a plain 👤/🤖 pair once done.

Design choices made specifically to fix bugs the old system had:
- Real Claude Code sessions now (--resume, one per chat_id, persisted in
  ask_sessions.json), not a one-shot call per message -- the earlier design
  used a fresh stateless call every time plus a hand-rolled flat-file
  history dump, specifically to dodge the old system's "persona quietly
  overridden by accumulated memory over time" bug. Turns out that bug isn't
  actually caused by session persistence itself: Claude Code's auto-memory
  system only gets read/written if the system prompt instructs it to, and
  this persona never mentions memory at all (confirmed empirically -- no
  memory tool calls happen even with the harness's memory_paths exposed).
  The actual fix that matters, kept unconditionally on every single turn
  regardless of resume state, is --system-prompt as a full override (not
  append) of Claude's default prompt, plus --setting-sources local and a
  neutral cwd (/tmp, no CLAUDE.md) -- nothing about session resumption
  bypasses any of that.
- "search"/"translate" stay stateless one-off calls (no session) -- only
  "chat" (plain .ask) is a real ongoing conversation worth resuming.
"""

import base64
import html
import json
import os
import re
import subprocess
import threading
import time

from cryptography.fernet import Fernet, InvalidToken

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", os.path.expanduser("~/.local/bin/claude"))
WORKDIR = os.environ.get("CLAUDE_JARVIS_WORKDIR", "/tmp")  # neutral cwd — no CLAUDE.md/memory to leak into the persona
# Real MCP tools (2026-08-11) replacing the ENTIRE old text-marker surface
# (CREATE_GROUP/INVITE_TO_GROUP/RESOLVE_PERSON, then SEND_MESSAGE/SEND_FILE/
# ADD_CONTACT/BLOCK_USER/LEAVE_CHAT/REGISTER_TRIGGER/DELETE_MESSAGES/
# SEARCH_CHAT/READ_HISTORY/LIST_TRIGGERS in the same pass) -- see
# mcp_telegram_tools.py. Own venv (not the system python3 this process
# itself runs under) because the `mcp` SDK isn't apt-packaged and this
# host's python3 is externally-managed (PEP 668) -- avoids a
# --break-system-packages install shared with every other process on this
# host. Only wired in for mode=="chat" (see call_llm) -- search/translate/
# classify are one-off utility calls with no live Telegram action surface
# worth giving tools for.
MCP_CONFIG_PATH = os.environ.get(
    "CLAUDE_JARVIS_MCP_CONFIG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "mcp_telegram_tools_config.json"),
)
# Per-request_id files, not single shared ones -- multiple .ask calls now run
# concurrently (one thread per request), matching cmd_queue.py's ASK_QUEUE_DIR/
# ASK_RESULT_DIR on the relay side. A single shared file would let concurrent
# requests overwrite each other's queue entry or progress/result.
QUEUE_DIR = os.environ.get("CLAUDE_JARVIS_ASK_QUEUE", "/tmp/jarvisask_ask_queue/")
RESULT_DIR = os.environ.get("CLAUDE_JARVIS_ASK_RESULT", "/tmp/jarvisask_ask_result/")
os.makedirs(QUEUE_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)
SESSIONS_LOCK = threading.Lock()
# A persisted chat session may only have one live `claude --resume` owner.
# The global semaphore caps box-wide load, but does not stop two rapid .ask
# requests for the same userbot/chat from resuming that session together.
CHAT_REQUEST_LOCKS = {}
CHAT_REQUEST_LOCKS_LOCK = threading.Lock()
# Persistent (NOT /tmp) -- real Claude Code sessions now, per chat_id, so an
# actual conversation survives a host reboot instead of getting wiped like
# the old /tmp-based history file did.
#
# One file per userbot INSTANCE (not per chat_id alone): a Telegram chat_id
# for a private chat is the PEER's user id, not scoped to whichever account
# is asking -- if two separate userbots (e.g. the owner's and, later, a
# family member's own instance) both talk to the same mutual contact, they'd
# get the identical chat_id. A single shared "chat_id -> session_id" file
# would silently merge those two people's conversations with that contact
# into one Claude Code session. Keyed by instance_id instead: default/
# missing instance_id (today's only real client, the existing deployed
# claude_ask.py, doesn't send one yet) keeps using the ORIGINAL filename
# unchanged, so none of the owner's already-accumulated sessions move or
# need migrating. Any other instance_id gets its own fresh, separate file.
_SESSIONS_DIR = os.path.dirname(os.path.abspath(__file__))
SESSIONS_FILE = os.path.join(_SESSIONS_DIR, "ask_sessions.json")  # old plaintext path, only for one-time migration below
DEFAULT_INSTANCE = "andrey"

# Each instance's session file is encrypted at rest with its OWN key (not
# shared) -- explicitly a casual/soft boundary, NOT a defense against
# someone with root on this box: two people who both have sudo here can
# always read anything, encrypted or not (dump the key from /proc/<pid>/
# environ, ptrace the running process while it holds plaintext, etc. --
# root beats any local encryption scheme by definition, no cipher fixes
# that). This is purely so `ask_sessions_<id>.json` doesn't read as a plain
# "oh it's just a chat_id/session_id map" at a glance. Real content (the
# actual conversation text) lives in Claude Code's own ~/.claude/projects/
# storage regardless, untouched by any of this.
def _key_file(instance_id: str) -> str:
    return os.path.join(_SESSIONS_DIR, f".session_key_{instance_id or DEFAULT_INSTANCE}")


def _get_key(instance_id: str) -> bytes:
    path = _key_file(instance_id)
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read().strip()
    key = Fernet.generate_key()
    with open(path, "wb") as f:
        f.write(key)
    os.chmod(path, 0o600)
    return key


def _encrypt(instance_id: str, data: dict) -> bytes:
    token = Fernet(_get_key(instance_id)).encrypt(json.dumps(data).encode())
    # Fernet tokens are URL-safe base64 TEXT by design (still readable/
    # copy-pasteable at a glance as "some base64 blob"). Decoding it back to
    # raw bytes before writing to disk is what actually makes the file open
    # as garbage/binary in a normal text editor instead of a suspiciously
    # neat-looking base64 string.
    return base64.urlsafe_b64decode(token)


def _decrypt(instance_id: str, raw: bytes) -> dict:
    token = base64.urlsafe_b64encode(raw)
    plaintext = Fernet(_get_key(instance_id)).decrypt(token)
    return json.loads(plaintext)


def _sessions_file(instance_id: str) -> str:
    # No special case for DEFAULT_INSTANCE -- every instance, including
    # andrey, gets the exact same ask_sessions_<id>.enc naming. Used to
    # special-case andrey onto the bare ask_sessions.enc (legacy from
    # before encryption existed at all), which was never architecturally
    # necessary, just avoided a one-time rename. Fixed 2026-08-04 per the
    # owner: no reason for one instance to look structurally special when
    # nothing about the actual design requires it.
    return os.path.join(_SESSIONS_DIR, f"ask_sessions_{instance_id or DEFAULT_INSTANCE}.enc")


def _migrate_legacy_sessions():
    """One-time, run at import, two legacy shapes to fold in if present:
    1. Pre-encryption plaintext ask_sessions.json -> encrypt into the
       current-scheme path.
    2. Pre-symmetry ask_sessions.enc (the old andrey-only-gets-no-suffix
       special case) -> rename to ask_sessions_andrey.enc. Just a rename,
       not a re-encryption -- the ciphertext doesn't care what its filename
       is, and .session_key_andrey was already named correctly (_key_file
       always used `instance_id or DEFAULT_INSTANCE`, never had the bare-
       filename special case the session file itself did)."""
    target = _sessions_file(DEFAULT_INSTANCE)
    if os.path.exists(target):
        return
    old_enc = os.path.join(_SESSIONS_DIR, "ask_sessions.enc")
    if os.path.exists(old_enc):
        os.rename(old_enc, target)
        print(f"[migrate] ask_sessions.enc -> {target} (naming symmetry fix)", flush=True)
        return
    if not os.path.exists(SESSIONS_FILE):
        return
    try:
        with open(SESSIONS_FILE) as f:
            sessions = json.load(f)
    except Exception as e:
        print(f"[migrate] couldn't read old {SESSIONS_FILE}: {e}", flush=True)
        return
    with open(target, "wb") as f:
        f.write(_encrypt(DEFAULT_INSTANCE, sessions))
    os.chmod(target, 0o600)
    os.rename(SESSIONS_FILE, SESSIONS_FILE + ".migrated_bak")
    print(f"[migrate] {len(sessions)} session(s) -> {target}", flush=True)


_migrate_legacy_sessions()


# Chats where the persona's profanity is suppressed: NO_MATS_RULE is appended
# to the system prompt for these. Comma-separated Telegram chat ids in
# $JARVIS_NOMATS_CHAT_IDS (set by setup.sh / jarvis.env). Empty by default.
NOMATS_CHAT_IDS = {
    c.strip()
    for c in os.environ.get("JARVIS_NOMATS_CHAT_IDS", "").split(",")
    if c.strip()
}

# Every `.ask` call used to run `claude -p` with no CLAUDE_CONFIG_DIR
# override, meaning it always inherited whatever the watcher process's own
# environment had -- the owner's default `~/.claude`. That was invisible
# and harmless while only one instance existed, but it means a second
# instance (e.g. a family member's own separate userbot) would have its
# ENTIRE conversation history land in the exact same ~/.claude/projects/
# bucket, under the exact same Claude account/subscription, as the owner's
# -- not just "unencrypted", genuinely unseparated at the source. Mirrors
# the exact pattern bridge.py already uses for its own multi-tenant
# accounts (see telegram_bridge_setup memory): each non-default instance
# gets its own CLAUDE_CONFIG_DIR under accounts/<instance_id>/, which needs
# its own one-time `claude auth login --claudeai` before it can actually be
# used (not done here -- that's an interactive OAuth step for whoever owns
# that instance to complete themselves). DEFAULT_INSTANCE deliberately
# keeps using the inherited/default config dir unchanged -- zero disruption
# to the owner's already-authenticated setup.
_ACCOUNTS_DIR = os.path.join(_SESSIONS_DIR, "accounts")


def _config_dir(instance_id: str):
    if not instance_id or instance_id == DEFAULT_INSTANCE:
        return None
    path = os.path.join(_ACCOUNTS_DIR, instance_id)
    os.makedirs(path, exist_ok=True)
    return path
PROGRESS_THROTTLE_S = 1.3
INTERNAL_TOOL_RESULT_PREFIX = "[INTERNAL_TOOL_RESULT]"


def _is_internal_tool_result(value):
    return isinstance(value, str) and value.startswith(INTERNAL_TOOL_RESULT_PREFIX)

# ---------------------------------------------------------------------------
# Persona loading. The chat persona is not baked into this file: it lives in
# an editable, git-ignored Markdown file so it can be viewed and rewritten at
# runtime (see the `.persona` userbot command and the /persona endpoint in
# cmd_queue.py) with no code change and no restart.
#
# Lookup order per instance_id:
#   1. $JARVIS_PERSONA_DIR/<instance_id>.md   this instance's own persona
#   2. $JARVIS_PERSONA_DIR/default.md         shared local override
#   3. personas/default.md.example            neutral template shipped in the
#      repo; {{OWNER_NAME}}/{{OWNER_TG_ID}} are filled from env
# The file is re-read whenever its mtime changes, so an edit lands on the
# next .ask without restarting the watcher.
# ---------------------------------------------------------------------------
PERSONA_DIR = os.environ.get(
    "JARVIS_PERSONA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "personas"),
)
_PERSONA_TEMPLATE = os.path.join(PERSONA_DIR, "default.md.example")
_PERSONA_OWNER_NAME = os.environ.get("JARVIS_OWNER_NAME", "владелец")
_PERSONA_OWNER_TG_ID = os.environ.get("JARVIS_OWNER_TG_ID", "")
_PERSONA_FALLBACK = (
    "Ты — Jarvis, дерзкий ИИ-агент с собственным характером. "
    "Отвечай на русском, простым HTML. Никогда не называй себя "
    "языковой моделью и не упоминай компании или модели, на которых работаешь."
)
_persona_cache = {}          # instance_id -> (path, mtime, text)
_persona_lock = threading.Lock()


def _fill_persona_placeholders(text):
    return (text.replace("{{OWNER_NAME}}", _PERSONA_OWNER_NAME)
                .replace("{{OWNER_TG_ID}}", _PERSONA_OWNER_TG_ID))


def persona_file(instance_id):
    """Path to this instance's editable persona file (may not exist yet)."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", instance_id or DEFAULT_INSTANCE)
    return os.path.join(PERSONA_DIR, safe + ".md")


_PERSONA_READ_CAP = 256 * 1024  # generous; a real persona is a few KB


def _read_persona_file(path):
    """Read a persona file without following a symlink -- it must be a regular
    file, never a pointer elsewhere. Returns text, capped at _PERSONA_READ_CAP."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd, "r", encoding="utf-8") as fh:
        return fh.read(_PERSONA_READ_CAP + 1)


def load_persona(instance_id=None):
    instance_id = instance_id or DEFAULT_INSTANCE
    for path in (persona_file(instance_id),
                 os.path.join(PERSONA_DIR, "default.md"),
                 _PERSONA_TEMPLATE):
        try:
            st = os.stat(path)
            stamp = (st.st_mtime_ns, st.st_size)
        except OSError:
            continue
        with _persona_lock:
            cached = _persona_cache.get(instance_id)
            if cached and cached[0] == path and cached[1] == stamp:
                return cached[2]
        try:
            raw = _read_persona_file(path)
        except (OSError, UnicodeError):
            continue
        if len(raw) > _PERSONA_READ_CAP:
            continue  # implausibly large / not a real persona file -- fall through
        # {{OWNER_*}} placeholders are filled for every source, not just the
        # bundled template -- a hand-written persona that keeps them still works.
        text = _fill_persona_placeholders(raw).strip()
        if not text:
            continue  # empty / whitespace-only -> fall through to the next source
        with _persona_lock:
            _persona_cache[instance_id] = (path, stamp, text)
        return text
    return _PERSONA_FALLBACK

NO_MATS_RULE = (
    "\n\nВНИМАНИЕ: в этом чате КАТЕГОРИЧЕСКИ запрещён мат и нецензурная "
    "лексика. Отвечай вежливо и культурно."
)

SYSTEM_PROMPTS = {
    "search": (
        "Ты — Jarvis, поисковый ассистент. Найди информацию по запросу и дай "
        "краткий ответ на русском, используя HTML. Никогда не говори что ты "
        "языковая модель или упоминай компании/модели — ты Jarvis, точка."
    ),
    "translate": (
        "Ты — Jarvis, переводчик. Переведи следующий текст на русский. Только "
        "перевод, без пояснений."
    ),
    # Phase 4 Tier 1 (trigger semantic classification) -- deliberately NOT
    # routed through any external API (OpenRouter etc): the whole reason
    # this project runs on `claude -p` subscription auth instead of metered
    # API billing is to avoid paying per token anywhere in the pipeline.
    # This mode is always called with model="haiku" (see call_llm) and no
    # session resume -- a stateless, cheap, same-subscription classify call.
    # 3-way, not yes/no -- load-bearing for the "verify" trigger gate
    # (claude_ask.py's _resolve_verified_action): a confident да/нет acts
    # automatically, but a genuinely doubtful case must come back as its
    # own distinct answer so the caller can escalate to a human instead of
    # guessing. Collapsing this to a plain yes/no would silently turn every
    # doubtful case into either a false auto-delete or a missed one.
    "classify": (
        "Ты классификатор. Тебе дают условие и текст сообщения. Ответь "
        "СТРОГО одним из трёх вариантов: 'да' (уверенно подходит под "
        "условие), 'нет' (уверенно не подходит) или 'не уверен' "
        "(сомнительный, пограничный случай) -- без пояснений, без "
        "форматирования, без знаков препинания. Используй 'не уверен' "
        "честно, когда действительно есть сомнение, а не только 'да'/'нет' "
        "для перестраховки."
    ),
}
# Model alias used for Tier 1 classify calls -- cheapest/fastest tier,
# never the default (chat/search/translate use whatever model the
# subscription's normally configured for).
CLASSIFY_MODEL = "haiku"

def _load_sessions(instance_id: str) -> dict:
    path = _sessions_file(instance_id)
    if os.path.exists(path):
        try:
            with open(path, "rb") as f:
                return _decrypt(instance_id, f.read())
        except (InvalidToken, Exception) as e:
            print(f"[sessions] couldn't decrypt {path}: {e}", flush=True)
    return {}


def _write_sessions(instance_id: str, sessions: dict):
    path = _sessions_file(instance_id)
    with open(path, "wb") as f:
        f.write(_encrypt(instance_id, sessions))
    os.chmod(path, 0o600)


def get_session_id(instance_id: str, chat_id: str):
    with SESSIONS_LOCK:
        return _load_sessions(instance_id).get(chat_id)


def set_session_id(instance_id: str, chat_id: str, session_id: str):
    with SESSIONS_LOCK:
        sessions = _load_sessions(instance_id)
        sessions[chat_id] = session_id
        _write_sessions(instance_id, sessions)


def clear_session_id(instance_id: str, chat_id: str):
    with SESSIONS_LOCK:
        sessions = _load_sessions(instance_id)
        if sessions.pop(chat_id, None) is not None:
            _write_sessions(instance_id, sessions)


def _chat_request_lock(instance_id: str, chat_id: str):
    key = (str(instance_id), str(chat_id))
    with CHAT_REQUEST_LOCKS_LOCK:
        return CHAT_REQUEST_LOCKS.setdefault(key, threading.Lock())


def _clean(s, limit=250):
    s = re.sub(r"\s+", " ", s or "").strip()
    return s[:limit]


def _tool_label_and_content(name, tool_input):
    if name == "Bash":
        return "Bash", tool_input.get("command", "")
    if name == "Read":
        return "Read", tool_input.get("file_path", "")
    if name in ("Write", "Edit"):
        return name, tool_input.get("file_path", "")
    if name == "WebSearch":
        return "WebSearch", tool_input.get("query", "")
    if name == "WebFetch":
        return "WebFetch", tool_input.get("url", "")
    return name, json.dumps(tool_input, ensure_ascii=False)


def run_claude_streaming(
    system_prompt: str, prompt: str, on_progress, session_id: str = None,
    config_dir: str = None, model: str = None,
    mcp_config: str = None, chat_id: str = None, instance_id: str = None,
    topic_id: str = None, exclude_id: str = None, requester_id: str = None,
):
    """Streams a single `claude -p` turn, calling on_progress(html_text) as
    thoughts/tool calls/results come in. Returns (final_text, thought_history,
    session_id) -- session_id is the resumed one if given, or the fresh one
    Claude Code assigned this turn, for the caller to persist.

    Real session persistence (--resume), not a one-shot call: --system-prompt
    stays a full override on every single turn regardless, which is the
    actual defense against the persona eroding over a long conversation --
    confirmed empirically that Claude Code's auto-memory system only gets
    read/written if the system prompt itself instructs it to (it doesn't
    activate automatically just because the harness exposes memory_paths),
    so a custom persona that never mentions memory never touches it, resumed
    session or not."""
    args = [
        CLAUDE_BIN, "-p", prompt,
        "--system-prompt", system_prompt,
        "--setting-sources", "local",
        "--dangerously-skip-permissions",
        "--output-format=stream-json",
        "--include-partial-messages",
        "--verbose",
    ]
    if model:
        args += ["--model", model]
    if session_id:
        args += ["--resume", session_id]
    if mcp_config:
        args += ["--mcp-config", mcp_config]
    # Tried enabling a thinking budget (MAX_THINKING_TOKENS) so reasoning-only
    # asks (no tool calls) would have a genuine thinking block to bank too.
    # Reverted: with this persona/prompt shape the model still writes its
    # reasoning straight into visible text instead of a separate thinking
    # block even on clear step-by-step math, so it bought real latency on
    # every single .ask with zero extra thoughts banked. Thought recaps stay
    # limited to the case that already works: a thought immediately
    # followed by a tool call.
    env = None
    if config_dir or mcp_config:
        env = os.environ.copy()
        if config_dir:
            env["CLAUDE_CONFIG_DIR"] = config_dir
        if mcp_config:
            # Read by mcp_telegram_tools.py (via `claude -p`'s own
            # subprocess tree) so a real tool call knows which chat/
            # instance/forum-topic it's acting for, and which message id to
            # exclude from history reads (the live "🤔 Думаю" placeholder),
            # without the model having to pass any of that itself -- same
            # trick bridge.py already uses for its wakeup mechanism.
            env["CHAT_ID"] = str(chat_id or "")
            env["INSTANCE_ID"] = str(instance_id or DEFAULT_INSTANCE)
            env["REQUESTER_ID"] = str(requester_id or "")
            if topic_id:
                env["TOPIC_ID"] = str(topic_id)
            if exclude_id:
                env["EXCLUDE_MSG_ID"] = str(exclude_id)
    try:
        proc = subprocess.Popen(
            args, cwd=WORKDIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env,
        )
    except Exception as e:
        return f"Ошибка Claude: {e}", [], session_id

    draft_thought = None
    draft_cmd_label = None
    draft_cmd = None
    draft_res_blocks = []
    final_text = None
    thought_history = []
    new_session_id = session_id

    def render():
        lines = []
        if draft_thought:
            # Escaped, UNLIKE the final answer: the persona only promises
            # valid HTML for the final answer text. Mid-turn scratch text
            # (a thought before a tool call, or the answer literally
            # forming token-by-token -- indistinguishable at this point) has
            # no such guarantee. A stray '<' (code, a comparison like
            # "a < b") used to corrupt the HTML entity structure, silently
            # breaking this edit (Telegram rejects the whole parse,
            # _safe_edit swallows the exception) and, via the same
            # unescaped string reused later, the final thought recap too.
            # Icon changed from the brain (implies "reasoning") to a
            # neutral "writing" one: this same branch renders BOTH genuine
            # intermediate reasoning AND a plain answer forming with no
            # tool calls at all -- we can't tell which until a tool_use
            # either follows or doesn't, so framing it as "thinking" was
            # actively misleading for the common no-tool-call case. 🧠 is
            # now reserved for the final recap in claude_ask.py, built only
            # from confirmed (tool-preceded) thoughts.
            lines.append(f"✍️ {html.escape(draft_thought, quote=False)}")
        if draft_cmd:
            lines.append(f"🔧 {html.escape(draft_cmd_label, quote=False)}:")
            lines.append(f"<pre>{html.escape(draft_cmd, quote=False)}</pre>")
            for label, content in draft_res_blocks:
                lines.append(f"{html.escape(label, quote=False)}:")
                lines.append(f"<pre>{html.escape(content, quote=False)}</pre>")
        return "\n".join(lines) if lines else "🧠 Думаю..."

    try:
        for raw_line in proc.stdout:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                d = json.loads(raw_line)
            except Exception:
                continue
            t = d.get("type")

            if t == "system" and d.get("subtype") == "init":
                new_session_id = d.get("session_id") or new_session_id
                continue

            if t == "assistant":
                for block in d.get("message", {}).get("content", []):
                    if block.get("type") == "tool_use":
                        # A tool call means the preceding thought (whichever
                        # block type it arrived as -- without a thinking
                        # budget it's virtually always "text", not "thinking")
                        # is done -- bank it for the final recap and clear
                        # it. A dangling trailing thought with no tool_use
                        # after it is never banked this way, which is exactly
                        # right: that's the final answer itself, forming in
                        # real time, not a separate thought to quote back.
                        if draft_thought:
                            thought_history.append(draft_thought)
                            draft_thought = None
                        name = block.get("name", "?")
                        tool_input = block.get("input", {})
                        label, content = _tool_label_and_content(name, tool_input)
                        draft_cmd_label = label
                        draft_cmd = _clean(content)
                        draft_res_blocks = []
                        on_progress(render())
                    elif block.get("type") in ("text", "thinking"):
                        text = block.get("text") or block.get("thinking") or ""
                        if text.strip():
                            draft_thought = _clean(text, limit=400)
                            draft_cmd_label = None
                            draft_cmd = None
                            draft_res_blocks = []
                            on_progress(render())
                continue

            if t == "user":
                for block in d.get("message", {}).get("content", []):
                    if block.get("type") == "tool_result":
                        is_error = block.get("is_error", False)
                        tur = d.get("tool_use_result")
                        stdout_text = tur.get("stdout") if isinstance(tur, dict) else None
                        stderr_text = tur.get("stderr") if isinstance(tur, dict) else None
                        res_blocks = []
                        if stdout_text is not None or stderr_text is not None:
                            stdout_text = (stdout_text or "").strip()
                            stderr_text = (stderr_text or "").strip()
                            if stdout_text:
                                res_blocks.append(("✅ StdOut", _clean(stdout_text, 400)))
                            if stderr_text:
                                res_blocks.append(("❌ StdErr", _clean(stderr_text, 400)))
                        else:
                            result_content = block.get("content", "")
                            if isinstance(result_content, list):
                                result_content = " ".join(
                                    c.get("text", "") for c in result_content if isinstance(c, dict)
                                )
                            result_content = str(result_content)
                            if _is_internal_tool_result(result_content):
                                # Permission denials are context for the model,
                                # not user-facing progress. The MCP result still
                                # reaches Claude; only the live Telegram render
                                # is suppressed.
                                continue
                            exit_match = (
                                re.match(r"^Exit code (\d+)\n?(.*)$", result_content, re.DOTALL)
                                if is_error else None
                            )
                            if exit_match:
                                exit_code, output = exit_match.group(1), exit_match.group(2).strip()
                                if output:
                                    res_blocks.append(("✅ StdOut", _clean(output, 400)))
                                res_blocks.append(("❌ StdErr", f"Exit code {exit_code}"))
                            else:
                                icon = "❌" if is_error else "✅"
                                label = f"{icon} {'Ошибка' if is_error else 'Результат'}"
                                res_blocks.append((label, _clean(result_content.strip(), 400)))
                        draft_res_blocks = res_blocks
                        on_progress(render())
                continue

            if t == "result":
                final_text = d.get("result", "")
                continue

        proc.wait(timeout=10)
    except Exception:
        pass
    finally:
        if proc.poll() is None:
            proc.kill()

    return (final_text if final_text is not None else "(нет ответа)"), thought_history, new_session_id


def call_llm(
    question: str, chat_id: str, mode: str, req_id: str, instance_id: str = DEFAULT_INSTANCE,
    topic_id: str = None, exclude_id: str = None, requester_id: str = None,
):
    if mode == "chat":
        system = load_persona(instance_id)
    else:
        system = SYSTEM_PROMPTS.get(mode) or load_persona(instance_id)
    if str(chat_id) in NOMATS_CHAT_IDS:
        system += NO_MATS_RULE

    # Only "chat" (plain .ask) is a real ongoing conversation worth resuming.
    # "search"/"translate" are one-off utility calls -- keep them stateless
    # so they never touch or fork the main chat session.
    session_id = get_session_id(instance_id, chat_id) if mode == "chat" else None

    last_write = [0.0]

    def on_progress(text):
        now = time.time()
        if now - last_write[0] < PROGRESS_THROTTLE_S:
            return
        last_write[0] = now
        try:
            with open(os.path.join(RESULT_DIR, f"{req_id}.json"), "w") as f:
                json.dump({"done": False, "request_id": req_id, "progress": text}, f)
        except Exception:
            pass

    model = CLASSIFY_MODEL if mode == "classify" else None
    mcp_config = MCP_CONFIG_PATH if mode == "chat" else None
    answer, thoughts, new_session_id = run_claude_streaming(
        system, question, on_progress, session_id=session_id, config_dir=_config_dir(instance_id), model=model,
        mcp_config=mcp_config, chat_id=chat_id, instance_id=instance_id,
        topic_id=topic_id, exclude_id=exclude_id, requester_id=requester_id,
    )

    if mode == "chat" and new_session_id:
        set_session_id(instance_id, chat_id, new_session_id)

    return answer, thoughts


# Cap concurrent `claude -p` subprocesses -- unbounded thread spawning under
# a burst of requests would just thrash the box. 5 is generous for a
# personal/small-group bot; raise if it ever actually gets hit.
_CONCURRENCY = threading.Semaphore(5)


def _process_request(
    req_id: str, question: str, chat_id: str, mode: str, instance_id: str = DEFAULT_INSTANCE,
    topic_id: str = None, exclude_id: str = None, requester_id: str = None,
):
    result_path = os.path.join(RESULT_DIR, f"{req_id}.json")
    with _CONCURRENCY:
        try:
            print(f"[{instance_id}:{chat_id}:{mode}] Q: {question[:80]}...", flush=True)
            answer, thoughts = call_llm(
                question, chat_id, mode, req_id, instance_id, topic_id=topic_id,
                exclude_id=exclude_id, requester_id=requester_id,
            )
            print(f"  A: {answer[:80]}...", flush=True)
            with open(result_path, "w") as f:
                json.dump(
                    {"done": True, "request_id": req_id, "answer": answer, "thoughts": thoughts}, f,
                )
        except Exception as e:
            print(f"ERR [{req_id}]: {e}", flush=True)
            try:
                with open(result_path, "w") as f:
                    json.dump(
                        {"done": True, "request_id": req_id, "answer": f"Ошибка воркера: {e}", "thoughts": []},
                        f,
                    )
            except Exception:
                pass


def _process_request_serialized(
    req_id: str, question: str, chat_id: str, mode: str, instance_id: str = DEFAULT_INSTANCE,
    topic_id: str = None, exclude_id: str = None, requester_id: str = None,
):
    # Stateless utility modes share no resumable session and stay parallel.
    if mode != "chat":
        return _process_request(
            req_id, question, chat_id, mode, instance_id, topic_id, exclude_id, requester_id,
        )
    with _chat_request_lock(instance_id, chat_id):
        return _process_request(
            req_id, question, chat_id, mode, instance_id, topic_id, exclude_id, requester_id,
        )


def main():
    print("CLAUDE ASK WATCHER READY (streaming, concurrent)", flush=True)
    while True:
        try:
            for fname in os.listdir(QUEUE_DIR):
                if not fname.endswith(".json"):
                    continue
                path = os.path.join(QUEUE_DIR, fname)
                try:
                    with open(path) as f:
                        data = json.load(f)
                except Exception:
                    continue
                try:
                    os.remove(path)
                except FileNotFoundError:
                    continue
                if not isinstance(data, dict) or not data.get("question"):
                    continue
                req_id = data.get("request_id", fname[:-5])
                question = data.get("question", "")
                chat_id = data.get("chat_id", "unknown")
                mode = data.get("mode", "chat")
                # Missing instance_id (today's only deployed client,
                # claude_ask.py, doesn't send one) defaults to the existing
                # owner instance -- zero behavior change for it.
                instance_id = data.get("instance_id") or DEFAULT_INSTANCE
                # topic_id/message_id (see cmd_queue.py's /ask handler,
                # already forwarded these two keys) -- message_id here is
                # the live work_message's id from the ORIGINAL .ask, used as
                # exclude_id so a real MCP read_history/search_chat tool
                # call doesn't sweep up the bot's own live "🤔 Думаю"
                # placeholder as if it were part of the conversation.
                topic_id = data.get("topic_id")
                exclude_id = data.get("message_id")
                requester_id = data.get("requester_id")
                threading.Thread(
                    target=_process_request_serialized,
                    args=(req_id, question, chat_id, mode, instance_id),
                    kwargs={
                        "topic_id": topic_id, "exclude_id": exclude_id,
                        "requester_id": requester_id,
                    },
                    daemon=True,
                ).start()
            time.sleep(1)
        except Exception as e:
            print(f"ERR: {e}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
