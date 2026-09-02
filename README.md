# claude-ask

> Part of **[telegram-ai](https://github.com/maleon17/telegram-ai)** — Claude/Codex ↔ Telegram, four ways.

A standalone Telegram userbot/backend for ClaudeAsk. It lets the owner
invoke Jarvis with the `.ask` command in any Telegram chat and perform real
actions through their own Telethon session. This is not the Bot API bridge
and not codex-ask.

Repository: <https://github.com/maleon17/claude-ask>

## Components

- `claude_ask.py` — the main loadable ClaudeAsk module;
- `claude_watcher.py` — the Claude Code backend with persistent sessions;
- `mcp_telegram_tools.py` — MCP tools for Telegram account actions;
- `cmd_queue.py` — the shared HTTP queue relay for `.ask` and `.xask`;
- `setup.sh` and service examples — backend infrastructure setup.

## Commands

ClaudeAsk uses `.ask`, `.search`, `.translate`, and `.new`. Triggers are
stored in the `ClaudeAsk` namespace. CodexAsk uses a separate history
namespace and `x`-prefixed commands, but deliberately reads the same trigger
namespace; the incoming-message watcher stays a single one, so triggers
aren't duplicated.

## Architecture

The userbot module sends a request to `/ask`, the worker processes it
through Claude Code and returns a progress/answer stream via
`/tmp/jarvisask_ask_*`. MCP calls land in `/tmp/jarvisask_tool_queue`; the module
holding a live Telethon session executes the action and returns the result.
Every action gets a real tool result first, and only then is it reflected
in the reply to the user.

The queue relay is shared infrastructure transport. If
`jarvis-ask-cmd-queue.service` is already running on the host, don't start a
second copy on the same ports/directories.

## Backend installation

Requirements: Linux, Python 3.10+, the Claude Code CLI authenticated via
`claude auth login --claudeai`, systemd (for running as a service). The
userbot itself must be installed separately — this project targets the
[Heroku](https://github.com/coddrago/Heroku) userbot; follow its own README
for installing and starting the userbot before loading `claude_ask.py` into
it. Heroku can run on a dedicated Telethon/Hikka host or in Docker on the
same machine as this backend — running it on a separate host is just this
deployment's own choice, not a requirement.

```bash
git clone https://github.com/maleon17/claude-ask.git
cd claude-ask
chmod +x setup.sh
./setup.sh
```

The script creates the MCP virtualenv, a local
`mcp_telegram_tools_config.json`, and a runtime directory. Service templates
need `__USER__` and `__INSTALL_DIR__` replaced; the queue relay should only
be installed once per host.

## Loading the module

Send the same `claude_ask.py` as a document to each userbot instance's
dedicated test Telegram channel and reply to the document with `.lm`. Then,
once per account, run `.asknet local <instance_id>` or
`.asknet tailnet <instance_id> <backend_url>`. Settings persist across later
updates via `.dlm`. After loading, verify `.ask`, `.new`, and a request that
uses a real tool (`list_triggers`, `read_history`, or `search_chat`).

The queue relay's network address and the Mistral key for voice
transcription are set via environment variables
(`CLAUDE_JARVIS_BACKEND_URL`, `MISTRAL_API_KEY`); no secrets are stored in
the repository.

## Verification and updates

```bash
python3 -m py_compile claude_watcher.py cmd_queue.py mcp_telegram_tools.py
systemctl status jarvis-ask-watcher
journalctl -u jarvis-ask-watcher -f
```

A plain `docker cp` does not reload an already-running module.

## Updating

The loaded module reinstalls itself in place from the latest version on
`main`:

```text
.dlm https://raw.githubusercontent.com/maleon17/claude-ask/main/claude_ask.py
```

Each instance's saved `.asknet` configuration stays in Heroku's database, so
there's no need to reconfigure the network after an update.

## Product boundaries

`Claude-jarvis` is ClaudeAsk/the userbot. `Codex-jarvis` is the independent
CodexAsk and its app-server. `Codex-telegram-bot` is yet another independent
Bot API frontend. Only the queue relay and the trigger namespace are
deliberately shared.

## License

MIT, Copyright (c) 2026 maleon17.
