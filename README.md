# Claude Jarvis

Отдельный Telegram userbot/backend для ClaudeAsk. Он позволяет владельцу
вызывать Jarvis командой `.ask` в любом чате Telegram и выполнять реальные
действия своей Telethon-сессией. Это не Bot API bridge и не Codex Jarvis.

Репозиторий: <https://github.com/maleon17/Claude-jarvis>

## Состав

- `claude_ask.py` — основной загружаемый модуль ClaudeAsk;
- `claude_ask_anatoly.py` — изолированный вариант для второго userbot-инстанса;
- `claude_watcher.py` — Claude Code backend с постоянными сессиями;
- `mcp_telegram_tools.py` — MCP-инструменты действий аккаунта Telegram;
- `cmd_queue.py` — общий HTTP queue relay для `.ask` и `.xask`;
- `funnel_ask_router.py` — опциональный path-router для Tailscale Funnel;
- `setup.sh` и service examples — подготовка backend-инфраструктуры.

## Команды

ClaudeAsk использует `.ask`, `.search`, `.translate` и `.new`. Триггеры
хранятся в namespace `ClaudeAsk`. CodexAsk использует отдельный namespace
истории и команды с префиксом `x`, но намеренно читает тот же namespace
триггеров; watcher входящих сообщений остаётся единственным, поэтому триггеры
не дублируются.

## Архитектура

Модуль userbot отправляет запрос в `/ask`, worker обрабатывает его через
Claude Code и возвращает поток прогресса/ответа через `/tmp/hermes_ask_*`.
MCP-вызовы попадают в `/tmp/hermes_tool_queue`; модуль, у которого есть живая
Telethon-сессия, выполняет действие и возвращает результат. Все действия
сначала получают реальный результат инструмента, и только затем попадают в
ответ пользователю.

Queue relay — инфраструктурный транспорт. Если на хосте уже работают
`jarvis-ask-cmd-queue.service` и `jarvis-ask-funnel-router.service`, не запускай
вторую копию на тех же портах и каталогах.

## Установка backend-а

Требования: Linux, Python 3.10+, Claude Code CLI с авторизацией
`claude auth login --claudeai`, systemd (для сервисного запуска). Userbot
должен быть установлен отдельно на Telethon/Hikka host.

```bash
git clone https://github.com/maleon17/Claude-jarvis.git
cd Claude-jarvis
chmod +x setup.sh
./setup.sh
```

Скрипт создаёт MCP virtualenv, локальный `mcp_telegram_tools_config.json` и
runtime-каталог. Сервисные шаблоны требуют замены `__USER__` и
`__INSTALL_DIR__`; queue relay следует устанавливать только один раз на хост.

## Загрузка модуля

Отправь `claude_ask.py` документом в выделенный тестовый Telegram-канал и
ответь на документ `.lm`. Для второго userbot-инстанса загружается
`claude_ask_anatoly.py`. После загрузки проверь `.ask`, `.new` и запрос с
реальным инструментом (`list_triggers`, `read_history` или `search_chat`).

Сетевой адрес queue relay и Mistral-ключ для голосовой расшифровки задаются
переменными окружения (`CLAUDE_JARVIS_FUNNEL`, `MISTRAL_API_KEY`); секреты в
репозитории не хранятся.

## Проверка и обновление

```bash
python3 -m py_compile claude_watcher.py cmd_queue.py mcp_telegram_tools.py
systemctl status jarvis-ask-watcher
journalctl -u jarvis-ask-watcher -f
```

Обновление userbot-модуля делается через документ + `.lm`; простой `docker cp`
не перезагружает уже работающий модуль.

## Граница продуктов

`Claude-jarvis` — ClaudeAsk/userbot. `Codex-jarvis` — независимый CodexAsk и
его app-server. `Codex-telegram-bot` — ещё один независимый Bot API frontend.
Только queue relay и namespace правил триггеров намеренно общие.

## Лицензия

MIT, Copyright (c) 2026 maleon17.
