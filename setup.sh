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
