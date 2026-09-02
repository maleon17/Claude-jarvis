#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

die() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 not found"; }

echo "== Claude Jarvis setup =="
need python3
need claude

claude auth status >/dev/null 2>&1 || die "Claude Code is not logged in; run: claude auth login --claudeai"
echo "Claude account: authenticated"

MCP_VENV="${CLAUDE_JARVIS_MCP_VENV:-$ROOT/.venv}"
echo "Installing Python dependencies into $MCP_VENV"
python3 -m venv "$MCP_VENV"
"$MCP_VENV/bin/python" -m pip install --upgrade pip >/dev/null
"$MCP_VENV/bin/python" -m pip install -r requirements.txt -r requirements-mcp.txt

sed \
    -e "s|__MCP_PYTHON__|$MCP_VENV/bin/python|g" \
    -e "s|__INSTALL_DIR__|$ROOT|g" \
    mcp_telegram_tools_config.json.example > mcp_telegram_tools_config.json
mkdir -p "$ROOT/runtime"
chmod 700 "$ROOT/runtime"

python3 -m py_compile claude_watcher.py cmd_queue.py mcp_telegram_tools.py remote_terminal.py

echo
echo "Prepared local MCP config: $ROOT/mcp_telegram_tools_config.json"
echo "The queue relay is shared by ClaudeAsk and CodexAsk; do not start a second"
echo "copy if an existing jarvis-ask-cmd-queue service is already running."
echo
echo "Install service examples manually after replacing placeholders:"
echo "  claude-jarvis-queue.service.example"
echo "  claude-jarvis-watcher.service.example"
echo "Finally upload claude_ask.py to the Telethon userbot and reply .lm."

# --- Jarvis persona: profanity notice + env (owner + softening list) ------
cat <<'NOTE'

Note: the Jarvis persona ships with profanity and dark humour enabled by
default - a deliberately informal, entertainment-leaning bot. To tone that
down, edit the persona after setup with the  .persona  command in Telegram
(CodexAsk: .xpersona), or edit personas/<instance>.md directly (re-read
automatically on change, no restart).
NOTE

_jarvis_clean() {  # drop control chars + shell/systemd-hazardous chars, keep UTF-8 letters
    python3 - "$1" <<'PY'
import sys
bad = set('\\"\'`$')
s = sys.argv[1] if len(sys.argv) > 1 else ""
sys.stdout.write("".join(c for c in s if c >= " " and c != "\x7f" and c not in bad))
PY
}

JARVIS_ENV="$ROOT/jarvis.env"
if [ -t 0 ]; then
    read -rp "Owner display name for the persona template (blank to skip): " J_OWNER_NAME || J_OWNER_NAME=""
    read -rp "Owner Telegram numeric id for the persona template (blank to skip): " J_OWNER_TG_ID || J_OWNER_TG_ID=""
    read -rp "Chat ids where profanity is suppressed, comma-separated (blank for none): " J_NOMATS || J_NOMATS=""
    read -rp "Shared persona directory [$ROOT/personas]: " J_PERSONA_DIR || J_PERSONA_DIR=""
    J_OWNER_NAME="$(_jarvis_clean "$J_OWNER_NAME")"
    J_OWNER_TG_ID="$(printf '%s' "$J_OWNER_TG_ID" | tr -cd '0-9')"
    J_NOMATS="$(printf '%s' "$J_NOMATS" | tr -cd '0-9,-')"
    J_PERSONA_DIR="$(_jarvis_clean "${J_PERSONA_DIR:-$ROOT/personas}")"
    if [ "$J_PERSONA_DIR" != "$ROOT/personas" ]; then
        mkdir -p "$J_PERSONA_DIR"
        [ -f "$J_PERSONA_DIR/default.md.example" ] || \
            cp "$ROOT/personas/default.md.example" "$J_PERSONA_DIR/default.md.example"
    fi
    ( umask 177; : > "$JARVIS_ENV" )
    {
        [ -n "$J_OWNER_NAME" ]  && printf 'JARVIS_OWNER_NAME=%s\n'  "$J_OWNER_NAME"
        [ -n "$J_OWNER_TG_ID" ] && printf 'JARVIS_OWNER_TG_ID=%s\n' "$J_OWNER_TG_ID"
        printf 'JARVIS_NOMATS_CHAT_IDS=%s\n' "$J_NOMATS"
        printf 'JARVIS_PERSONA_DIR=%s\n' "$J_PERSONA_DIR"
    } >> "$JARVIS_ENV"
    chmod 600 "$JARVIS_ENV"
    echo "Wrote $JARVIS_ENV (git-ignored; the watcher loads it via EnvironmentFile=)."
    echo "If the shared cmd_queue.py (from Claude-jarvis) runs from another directory,"
    echo "set the SAME JARVIS_PERSONA_DIR in its service unit too, otherwise"
    echo ".persona/.xpersona writes will not reach this watcher."
fi

# --- optional: graphify code-map -------------------------------------------
# graphify (PyPI package "graphifyy", github.com/Graphify-Labs/graphify) turns
# this repo into a queryable knowledge graph under graphify-out/. Optional.
setup_graphify() {
    local platform="$1"
    if command -v graphify >/dev/null 2>&1; then
        echo "graphify: already on PATH ($(command -v graphify))"
    elif command -v uv >/dev/null 2>&1; then
        uv tool install graphifyy || { echo "graphify: install failed, skipping"; return 0; }
    elif command -v pipx >/dev/null 2>&1; then
        pipx install graphifyy || { echo "graphify: install failed, skipping"; return 0; }
    else
        echo "graphify: needs 'uv' or 'pipx' to install - skipping"
        return 0
    fi
    graphify install --platform "$platform" >/dev/null 2>&1 || true
    graphify update . >/dev/null 2>&1 || true
    echo "graphify: code map built under graphify-out/ (re-run 'graphify update .' after edits;"
    echo "          a post-commit hook keeps it fresh if graphify installed one)"
}

if [ -t 0 ]; then
    read -rp "Set up the graphify code-map for this repo? [y/N] " _SETUP_GRAPHIFY || _SETUP_GRAPHIFY=""
    case "${_SETUP_GRAPHIFY:-}" in
        [Yy]*) setup_graphify "claude" ;;
    esac
fi
