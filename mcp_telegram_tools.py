#!/usr/bin/env python3
"""Compatibility launcher for the shared Codex/Claude Telegram MCP server.

The implementation is versioned with the standalone Codex product. Keeping
this historical path means the existing Claude userbot configuration keeps
working while both assistants use one trigger/action schema and queue.
"""

from runpy import run_path


run_path("/home/mishin/codex-jarvis/telegram_actions_mcp.py", run_name="__main__")
