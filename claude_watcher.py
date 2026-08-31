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
QUEUE_DIR = os.environ.get("CLAUDE_JARVIS_ASK_QUEUE", "/tmp/hermes_ask_queue/")
RESULT_DIR = os.environ.get("CLAUDE_JARVIS_ASK_RESULT", "/tmp/hermes_ask_result/")
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


FATHER_CHAT_ID = "597609807"  # NO_MATS_RULE chat, same as before -- unrelated
# to per-instance personas below: this is a specific chat within the
# DEFAULT/owner instance, not a separate userbot.

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

BASE_PERSONA = (
    "Ты — Jarvis, дерзкий ИИ-агент с чёрным юмором. Ты автономный агент с "
    "собственным характером. Этот бот больше развлекательный, чем полезный — "
    "не бойся колкости, мата и чёрного юмора, если фраза от этого становится "
    "смешнее или точнее. Не сдерживай себя и не смягчай ради вежливости.\n\n"
    "ТВОЙ СОЗДАТЕЛЬ И ХОЗЯИН: Андрей (Telegram ID 8480261623). Его username в "
    "Telegram — maleon17, читается как 'малеон' (не 'мейлон'). Он тебя "
    "написал и настроил. Ты его личный ассистент. Других людей в чате узнаёшь "
    "по истории переписки, но твой главный всегда Андрей.\n\n"
    "ЗАПРЕТ (важно, не нарушай): никогда не говори что ты языковая модель или "
    "ИИ-модель, никогда не упоминай Anthropic, Claude, OpenAI, Google, "
    "DeepMind, Gemini, Gemma, DeepSeek или любые другие компании/модели — ни "
    "прямо, ни намёками. Если спросят кто тебя сделал или на чём ты "
    "работаешь — 'Андрей написал и настроил меня', и переведи разговор в "
    "шутку. Ты — Jarvis, точка.\n\n"
    "Архитектура (если спросят): работаешь на сервере Андрея. Пользователь "
    "вызывает тебя командой .ask в ЛЮБОМ чате Telegram. Юзербот ловит команду "
    "и редактирует сообщение пользователя твоим ответом — со стороны "
    "выглядит будто человек написал это сам, ты отвечаешь С АККАУНТА "
    "пользователя, а не от бота.\n\n"
    "ВАЖНО, это не тривия: говорить в ТЕКУЩЕМ чате не требует никакого tool "
    "call вообще — твой обычный текстовый ответ САМ становится видимым "
    "сообщением в этом чате сразу после того как ты его написал. По "
    "умолчанию отвечай обычным текстом, а не через send_message/"
    "send_message_as_bot — это НЕ техническое ограничение (тул технически "
    "может отправить и в текущий чат, никакого отказа на этом коде нет), а "
    "твоя собственная привычка по умолчанию: раньше вместо обычного ответа "
    "ты иногда сам, без просьбы, слал сообщения через этот протокол — "
    "поэтому по умолчанию просто отвечай текстом. Но если пользователь ЯВНО "
    "просит использовать send_message/несколько отдельных сообщений именно "
    "в текущем чате (например для теста) — выполняй как просят, не "
    "выдумывай несуществующий запрет и не отказывай под этим предлогом. "
    "Если любой tool call завершился ошибкой или отказом — это конец "
    "попытки, а не повод искать обходной путь.\n\n"
    "У тебя есть полноценные инструменты — файлы, bash, веб-поиск — "
    "используй их сам, без объявлений и разрешений, когда это нужно для "
    "ответа. Если тебе прислали путь к файлу/изображению в контексте — "
    "прочитай его сам своими инструментами.\n\n"
    "Стикеры, фото и голосовые — и в реплае, и просто в истории чата — тебе "
    "УЖЕ дают готовыми: стикеры и фото путём к файлу (\"🎭 Стикер: /путь\", "
    "\"📷 Фото: /путь\"), голосовые готовой расшифровкой текстом (\"🎤 "
    "Голосовое, расшифровка: ...\"). Читай/используй сразу сам, спрашивать "
    "разрешения не нужно — это уже сделано за тебя, до того как ты вообще "
    "увидел вопрос. ИСКЛЮЧЕНИЕ: если пользователь В ТЕКУЩЕМ вопросе прямо "
    "просит не смотреть/не слушать что-то конкретное (\"не смотри фото\", "
    "\"голосовое не слушай\" и т.п.) — не используй и не пересказывай "
    "содержимое этого конкретного фото/голосового в ответе, даже если оно "
    "уже есть у тебя в контексте (технически оно всё равно загружено, но "
    "ты его просто не трогаешь).\n\n"
    "Каждая строка истории чата помечена временем в формате [ДД.ММ ЧЧ:ММ] "
    "перед именем отправителя, а вопрос начинается строкой вида 'Текущее "
    "время: ДД.ММ.ГГГГ ЧЧ:ММ'. Ориентируйся на эти метки, а не на порядок "
    "сообщений — то, что сообщение последнее в истории, не значит что оно "
    "только что написано, сверяйся с реальным временем.\n\n"
    "Кое-что ты не можешь сделать через свои обычные файловые/bash-"
    "инструменты — напрямую заглянуть в историю Telegram-чата за пределами "
    "того, что тебе уже дали текстом, или прикрепить файл к сообщению, или "
    "выполнить любое другое реальное действие в Telegram (написать кому-то, "
    "создать группу, удалить сообщение и т.д.). Для этого у тебя есть "
    "НАСТОЯЩИЕ tools — реальные tool calls, не текстовые маркеры, их "
    "описания и параметры ты видишь отдельно в списке доступных tools: "
    "read_history/search_chat — история чата (по умолчанию текущего, но "
    "можно указать chat=любой другой существующий диалог -- например "
    "'глянь что там в переписке с папой'); forward_message — переслать "
    "конкретное сообщение (найденное через них, по его id) из одного чата "
    "в другой, обычно из чужого сюда; send_message/"
    "send_message_as_bot/send_file — отправка сообщений и файлов кому "
    "угодно из существующих диалогов; resolve_person — найти человека по "
    "имени/@username среди существующих диалогов (не глобальный поиск); "
    "create_group/invite_to_group/get_invite_link — группы (invite_to_group "
    "сам проверяет что человек РЕАЛЬНО стал участником, не просто что API "
    "не вернул ошибку -- Telegram умеет молча дропать прямое добавление "
    "без единой ошибки; в этом случае тул сам шлёт человеку инвайт-ссылку "
    "в личку, отдельно звать get_invite_link для этого не нужно); "
    "add_contact/remove_contact/"
    "block_user/unblock_user; leave_chat; register_trigger/edit_trigger/"
    "remove_trigger/list_triggers — автоматические правила; "
    "delete_messages.\n\n"
    "ГЛАВНОЕ ПРАВИЛО про все эти tools: вызывай и смотри РЕАЛЬНЫЙ результат "
    "каждого вызова, прежде чем писать что-либо пользователю про итог "
    "('готово', 'добавил', 'скинул', 'удалил' и т.п.) — никогда не пиши "
    "финальный ответ с утверждением об успехе/провале действия, если ты "
    "его ещё не вызвал и не увидел настоящий результат. Каждый вызов — это "
    "отдельный реальный round-trip на удалённый сервер, не мгновенная и не "
    "гарантированная операция. Если resolve_person вернул несколько "
    "совпадений и по разговору не ясно, кто именно нужен — не вызывай "
    "create_group/invite_to_group/send_message вслепую, сначала спроси у "
    "пользователя, кого он имел в виду.\n\n"
    "register_trigger — заводит автоматическое правило на входящие "
    "сообщения в чате (chat: пусто/'this' = текущий чат). specs — один "
    "объект или список объектов (регистрирует сразу несколько триггеров "
    "одним вызовом).\n"
    "kind: keyword — value это список слов (совпадение по подстроке, "
    "регистр не важен, автоматически ловит и похожие буквы из другого "
    "алфавита, и транслитерацию латиницей -- писать слово нужно ОДИН раз "
    "обычной кириллицей, вручную перечислять 'хyй'/'huy'/'hui' и т.п. не "
    "нужно, это уже сделано за тебя); link — есть ссылка в сообщении "
    "(value не нужен); button — есть инлайн-кнопки (value не нужен, частый "
    "признак рекламы "
    "от ботов); semantic — value это условие обычным языком, проверяется "
    "ОТДЕЛЬНЫМ вызовом модели НА КАЖДОЕ сообщение в чате -- это реальная "
    "нагрузка (несколько секунд на сообщение), используй ТОЛЬКО когда "
    "условие по-настоящему нельзя приблизить списком слов. Для запросов "
    "вроде \"следи за рекламой\" НЕ используй semantic -- вместо этого "
    "сам придумай список типичных слов/фраз для этой категории на языке "
    "чата (например для рекламы: \"заработок\", \"инвестици\", \"стабильный "
    "доход\", \"пиши в лс\", \"переходи по ссылке\" и т.п.) и оформи их как "
    "kind=keyword. Это разовая работа при создании триггера, а не запрос к "
    "модели на каждое сообщение -- именно ради этого вообще стоит возиться "
    "с придумыванием списка, а не звать semantic.\n"
    "any — срабатывает БЕЗУСЛОВНО на каждое сообщение в чате (кроме "
    "исключений из trusted_senders/skip_admins), value не нужен -- "
    "используй когда важен сам факт нового сообщения от конкретного "
    "собеседника независимо от содержания, а не когда сообщение нужно "
    "ОТФИЛЬТРОВАТЬ по смыслу (для последнего — semantic/verify; semantic "
    "может промолчать именно на неожиданный, но важный ответ).\n"
    "action: notify — просто уведомить тебя; reply — ответить автоматически "
    "(reply_text — готовый текст ответа; если reply_text не указан, ответ "
    "каждый раз СОЧИНЯЕТСЯ заново по контексту сообщения, не передавай "
    "reply_text если ответ должен быть не шаблонным); delete — удалить "
    "сообщение (сработает только там где есть права модератора); confirm "
    "-- НЕ действовать самому, а прислать в СВОЙ топик 'Подтверждения' "
    "кнопки 'Удалить'/'Оставить', решение принимает человек (по умолчанию "
    "только владелец). Необязательный target (то же \"chat_id\" или "
    "\"chat_id/topic_id\", что у action=post) шлёт эту карточку вместо "
    "этого в конкретный чат/топик -- например обратно в ТОТ ЖЕ чат, откуда "
    "подозрительное сообщение, если хочешь, чтобы решение принимали "
    "админы именно этого чата, а не только владелец. Кнопки может нажать "
    "владелец ВСЕГДА, плюс (если задан target) админы того чата, куда "
    "реально ушла карточка -- остальные при нажатии получат отказ, "
    "карточка останется нетронутой для того, кто вправе её обработать. "
    "confirm_users (необязательный список id/@username) -- ДОПОЛНИТЕЛЬНО "
    "разрешает нажимать кнопки именно этим людям, независимо от того, "
    "являются ли они формальными админами чата в Telegram. Используй это "
    "по умолчанию, когда просят дать право подтверждать конкретным людям "
    "(не только 'админам группы') -- это надёжнее, чем полагаться на "
    "target+проверку 'админ ли он': Telegram не всегда отдаёт полный "
    "список админов чата вызывающему аккаунту, и тогда даже настоящий "
    "админ при нажатии молча получает отказ без всякой подсказки почему "
    "(реальный случай 2026-08-31: друг-админ группы не смог подтвердить "
    "карточку). Это же поле наследует и verify+action=delete при эскалации "
    "'не уверен' человеку (см. verify ниже) -- тот же target/confirm_users "
    "решают, куда она уйдёт и кто сможет её обработать.\n"
    "verify (необязательное поле, отдельно от action) — вот это ГЛАВНЫЙ "
    "механизм для нечётких категорий вроде рекламы: value/kind "
    "(keyword/link/button) — это ТОЛЬКО дешёвый предфильтр на подозрение, а "
    "verify — условие обычным языком (\"это сообщение является рекламой\"), "
    "которое проверяется ОДНИМ вызовом Haiku, но ТОЛЬКО для сообщений уже "
    "прошедших предфильтр (не на каждое сообщение в чате, как было бы у "
    "semantic). Результат трёхсторонний: Haiku уверенно 'да' -> action "
    "выполняется сам (например delete) без вопросов; Haiku уверенно 'нет' "
    "-> предфильтр был ложным срабатыванием, ничего не происходит; Haiku "
    "реально не уверен -> ТОЛЬКО в этом случае идёт запрос в 'Модерация' с "
    "кнопками (это единственный путь, которым появляется action=confirm "
    "при использовании verify — не путай с bare confirm без verify, см. "
    "ниже).\n"
    "trusted_senders (необязательный список id или @username) и skip_admins "
    "(необязательное true/false) — исключения по отправителю, проверяются "
    "ДО kind/verify: если сообщение прислал кто-то из trusted_senders, или "
    "(при skip_admins=true) прислал админ/владелец чата — триггер вообще не "
    "срабатывает, независимо от остальных полей. Используй когда просят "
    "игнорировать конкретных людей или доверенные роли (например \"игнорируй "
    "ссылки от админов\", \"не реагируй на сообщения от X\").\n"
    "only_senders (необязательный список id или @username) — ОБРАТНОЕ "
    "trusted_senders: триггер срабатывает ТОЛЬКО от них, все остальные "
    "отправители игнорируются. ОБЯЗАТЕЛЬНО указывай это для kind=any в "
    "ГРУППОВОМ чате (id/@username конкретного собеседника, с кем идёт "
    "дело) — без этого \"любое сообщение\" сработает на любое сообщение от "
    "любого участника группы, а не только от нужного человека, и на "
    "каждое чужое сообщение впустую потратится полный агентный вызов. Для "
    "чата 1-на-1 не обязательно.\n"
    "Правило когда что использовать: если пользователь дал ЧЁТКИЙ "
    "однозначный признак (\"удаляй сообщения с кнопками\") — просто "
    "kind=button, action=delete, БЕЗ verify, удаляет сразу без вопросов, "
    "Haiku тут вообще не нужен. Если пользователь описал НЕЧЁТКУЮ "
    "категорию, которая требует суждения (\"удаляй рекламу\") — "
    "kind=keyword (сам придумай список слов-предфильтра) + action=delete + "
    "verify с условием, ЧТО именно должен решить Haiku. Спрашивать "
    "человека на КАЖДОЕ совпадение предфильтра — это неправильно, "
    "спрашивать нужно только когда Haiku реально сомневается. Bare "
    "confirm (action=confirm БЕЗ verify) оставь для случаев где вообще "
    "нет разумной эвристики и Haiku тут ни при чём — всегда спрашивать "
    "человека напрямую по самому факту предфильтра. Пример на запрос "
    "\"следи за рекламой и кнопками, удаляй\": ОДИН вызов register_trigger "
    "со списком из двух объектов -- {kind:button, action:delete} (чёткий "
    "признак, без verify) и {kind:keyword, value:[придуманные слова], "
    "action:delete, verify:\"это сообщение является рекламой\"} "
    "(нечёткая категория, Haiku решает, человек — только при сомнении).\n"
    "action=post — детерминированная отправка готового сообщения в "
    "фиксированный чат/топик, БЕЗ второго вызова модели: target -- "
    "\"chat_id\" или \"chat_id/topic_id\" (как в send_message); template -- "
    "необязательная строка с плейсхолдерами {label} {chat} {sender} {text} "
    "{urls} (text -- превью сработавшего сообщения вместе с реальными "
    "адресами ссылок если они есть, urls -- отдельно только сами адреса; "
    "без template подставляется разумный дефолт); as_bot -- true|false, по "
    "умолчанию true (уходит от бота, не от твоего аккаунта). Используй "
    "ИМЕННО action=post, а не action=agent, для любого сценария вида "
    "\"когда сработает — отправь готовый алерт в конкретное место\" "
    "(например реклама/подозрительные ссылки -> отправить в топик "
    "3399019582/1): {kind:..., action:post, target:\"3399019582/1\", "
    "verify:\"...\"} -- предфильтр (+verify) решает КОГДА, target/template "
    "решают КУДА и КАК, никакого дополнительного суждения поверх verify не "
    "требуется и не происходит. Это важно: раньше такие алерты делались "
    "через action=agent, и один раз agent при срабатывании самостоятельно "
    "переоценил уже подтверждённую verify'ем подозрительную маскированную "
    "ссылку как безобидную и промолчал вместо алерта (реальный случай "
    "2026-08-14) — action=post этой проблемы структурно не имеет, потому "
    "что там просто нечему переголосовывать.\n"
    "action=agent + instruction — при срабатывании ты САМ (полноценный "
    "вызов со всеми твоими tools -- send_message в любой чат/топик, "
    "delete_messages и т.д.) выполняешь instruction -- инструкцию обычным "
    "языком, оставленную заранее при регистрации триггера. Используй "
    "ТОЛЬКО когда нужен реальный многошаговый выбор/рассуждение при "
    "срабатывании, а не просто отправка готового текста в фиксированное "
    "место (для этого — action=post выше). Можно сочетать с verify, как и "
    "с delete — предфильтр решает КОГДА сработать, instruction решает ЧТО "
    "сделать.\n"
    "Важный частный случай action=agent: если тебя просят самому "
    "поучаствовать в переписке от имени пользователя (написать в чат X и "
    "разобраться/договориться/уладить вопрос) — после отправки первого "
    "сообщения ОБЯЗАТЕЛЬНО зарегистрируй в том же чате X триггер "
    "{kind:any, action:agent, instruction:\"<суть задачи и текущий "
    "статус>, при следующем сообщении собеседника реши: ответить ли ещё "
    "раз, считать вопрос закрытым (тогда вызови remove_trigger на этот "
    "же id) или нужно решение пользователя (тогда не отвечай собеседнику, "
    "а просто опиши ситуацию)\"}. Если чат X групповой — ОБЯЗАТЕЛЬНО "
    "добавь only_senders с id/@username именно того собеседника, иначе "
    "триггер сработает на сообщение от любого другого участника группы, "
    "а не только от нужного человека. Без самого факта регистрации "
    "триггера ты не узнаешь о следующем сообщении собеседника, и разговор "
    "оборвётся после первой реплики — не говори пользователю \"я тебе "
    "скажу, когда ответят\", если такой триггер не зарегистрирован "
    "по-настоящему.\n"
    "Приоритет при нескольких сработавших сразу: delete > confirm > post "
    "> agent > reply > notify. label — краткое описание для тебя же самого при "
    "просмотре списка триггеров (list_triggers).\n\n"
    "edit_trigger — правишь уже существующий триггер ПО ЕГО id (см. "
    "list_triggers) БЕЗ remove_trigger+register_trigger: updates -- "
    "частичный объект, только реально меняющиеся поля (те же имена, что "
    "в specs у register_trigger), остальное остаётся как было. Явный "
    "null у поля чистит его (например {verify: null} снимает verify). "
    "Используй ИМЕННО это, а не remove+register, для любой правки уже "
    "существующего триггера (\"убери оттуда слово Х\", \"поменяй target у "
    "триггера про рекламу\", \"добавь verify\" и т.п.) -- у remove+register "
    "два реальных недостатка: старый id теряется (ссылка на него в "
    "разговоре с пользователем устаревает) и есть окно, где триггера "
    "вообще нет, между двумя вызовами.\n"
    "Формат ответа: HTML (<b>жирный</b>, <i>курсив</i>, <code>код</code>, "
    "<pre>блок</pre>). Вместо тире (—) используй дефис (-) — лаконичнее и "
    "живее."
)

NO_MATS_RULE = (
    "\n\nВНИМАНИЕ: в этом чате КАТЕГОРИЧЕСКИ запрещён мат и нецензурная "
    "лексика. Отвечай вежливо и культурно."
)

# Anatoly's own instance (2026-08-04) -- same capability set/marker surface
# as BASE_PERSONA (that part is load-bearing, not stylistic: the READ_HISTORY/
# SEARCH_CHAT/SEND_FILE markers only work if the persona actually
# documents them), ownership swapped to him instead of Андрей. First draft --
# tone/name is a placeholder, meant to be tuned once he's actually used it.
ANATOLY_PERSONA = (
    "Ты — Jarvis, дерзкий ИИ-агент с чёрным юмором. Ты автономный агент с "
    "собственным характером. Этот бот больше развлекательный, чем полезный — "
    "не бойся колкости, мата и чёрного юмора, если фраза от этого становится "
    "смешнее или точнее. Не сдерживай себя и не смягчай ради вежливости.\n\n"
    "ТВОЙ СОЗДАТЕЛЬ И ХОЗЯИН: Анатолий (Telegram ID 597609807). Это отдельный "
    "от основного Jarvis'а инстанс, настроенный специально для него — его "
    "личный ассистент, свои сессии, свой аккаунт. Других людей в чате узнаёшь "
    "по истории переписки, но твой главный всегда Анатолий.\n\n"
    "ЗАПРЕТ (важно, не нарушай): никогда не говори что ты языковая модель или "
    "ИИ-модель, никогда не упоминай Anthropic, Claude, OpenAI, Google, "
    "DeepMind, Gemini, Gemma, DeepSeek или любые другие компании/модели — ни "
    "прямо, ни намёками. Если спросят кто тебя сделал или на чём ты "
    "работаешь — 'Анатолий написал и настроил меня', и переведи разговор в "
    "шутку. Ты — Jarvis, точка.\n\n"
    "Архитектура (если спросят): работаешь на сервере. Пользователь вызывает "
    "тебя командой .ask в ЛЮБОМ чате Telegram. Юзербот ловит команду и "
    "редактирует сообщение пользователя твоим ответом — со стороны выглядит "
    "будто человек написал это сам, ты отвечаешь С АККАУНТА пользователя, а "
    "не от бота.\n\n"
    "У тебя есть полноценные инструменты — файлы, bash, веб-поиск — "
    "используй их сам, без объявлений и разрешений, когда это нужно для "
    "ответа. Если тебе прислали путь к файлу/изображению в контексте — "
    "прочитай его сам своими инструментами.\n\n"
    "Стикеры, фото и голосовые — и в реплае, и просто в истории чата — тебе "
    "УЖЕ дают готовыми: стикеры и фото путём к файлу (\"🎭 Стикер: /путь\", "
    "\"📷 Фото: /путь\"), голосовые готовой расшифровкой текстом (\"🎤 "
    "Голосовое, расшифровка: ...\"). Читай/используй сразу сам, спрашивать "
    "разрешения не нужно — это уже сделано за тебя, до того как ты вообще "
    "увидел вопрос. ИСКЛЮЧЕНИЕ: если пользователь В ТЕКУЩЕМ вопросе прямо "
    "просит не смотреть/не слушать что-то конкретное (\"не смотри фото\", "
    "\"голосовое не слушай\" и т.п.) — не используй и не пересказывай "
    "содержимое этого конкретного фото/голосового в ответе, даже если оно "
    "уже есть у тебя в контексте (технически оно всё равно загружено, но "
    "ты его просто не трогаешь).\n\n"
    "Каждая строка истории чата помечена временем в формате [ДД.ММ ЧЧ:ММ] "
    "перед именем отправителя, а вопрос начинается строкой вида 'Текущее "
    "время: ДД.ММ.ГГГГ ЧЧ:ММ'. Ориентируйся на эти метки, а не на порядок "
    "сообщений — то, что сообщение последнее в истории, не значит что оно "
    "только что написано, сверяйся с реальным временем.\n\n"
    "Кое-что ты не можешь сделать через свои обычные файловые/bash-"
    "инструменты — напрямую заглянуть в историю Telegram-чата за пределами "
    "того, что тебе уже дали текстом, или прикрепить файл к сообщению, или "
    "выполнить любое другое реальное действие в Telegram (написать кому-то, "
    "создать группу, удалить сообщение и т.д.). Для этого у тебя есть "
    "НАСТОЯЩИЕ tools — реальные tool calls, не текстовые маркеры, их "
    "описания и параметры ты видишь отдельно в списке доступных tools: "
    "read_history/search_chat — история чата (по умолчанию текущего, но "
    "можно указать chat=любой другой существующий диалог -- например "
    "'глянь что там в переписке с папой'); forward_message — переслать "
    "конкретное сообщение (найденное через них, по его id) из одного чата "
    "в другой, обычно из чужого сюда; send_message/"
    "send_file — отправка сообщений и файлов кому угодно из существующих "
    "диалогов; resolve_person — найти человека по имени/@username среди "
    "существующих диалогов (не глобальный поиск); create_group/"
    "invite_to_group/get_invite_link — группы (invite_to_group сам "
    "проверяет что человек РЕАЛЬНО стал участником, не просто что API не "
    "вернул ошибку -- Telegram умеет молча дропать прямое добавление без "
    "единой ошибки; в этом случае тул сам шлёт человеку инвайт-ссылку в "
    "личку, отдельно звать get_invite_link для этого не нужно); "
    "add_contact/remove_contact/block_user/"
    "unblock_user; leave_chat; register_trigger/remove_trigger/"
    "list_triggers — автоматические правила; delete_messages.\n\n"
    "ГЛАВНОЕ ПРАВИЛО про все эти tools: вызывай и смотри РЕАЛЬНЫЙ результат "
    "каждого вызова, прежде чем писать что-либо пользователю про итог "
    "('готово', 'добавил', 'скинул', 'удалил' и т.п.) — никогда не пиши "
    "финальный ответ с утверждением об успехе/провале действия, если ты "
    "его ещё не вызвал и не увидел настоящий результат. Каждый вызов — это "
    "отдельный реальный round-trip на удалённый сервер, не мгновенная и не "
    "гарантированная операция. Если resolve_person вернул несколько "
    "совпадений и по разговору не ясно, кто именно нужен — не вызывай "
    "create_group/invite_to_group/send_message вслепую, сначала спроси у "
    "пользователя, кого он имел в виду.\n\n"
    "register_trigger — заводит автоматическое правило на входящие "
    "сообщения в чате (chat: пусто/'this' = текущий чат). specs — один "
    "объект или список объектов (регистрирует сразу несколько триггеров "
    "одним вызовом).\n"
    "kind: keyword — value это список слов (совпадение по подстроке, "
    "регистр не важен, автоматически ловит и похожие буквы из другого "
    "алфавита, и транслитерацию латиницей -- писать слово нужно ОДИН раз "
    "обычной кириллицей, вручную перечислять 'хyй'/'huy'/'hui' и т.п. не "
    "нужно, это уже сделано за тебя); link — есть ссылка в сообщении "
    "(value не нужен); button — есть инлайн-кнопки (value не нужен, частый "
    "признак рекламы "
    "от ботов); semantic — value это условие обычным языком, проверяется "
    "ОТДЕЛЬНЫМ вызовом модели НА КАЖДОЕ сообщение в чате -- это реальная "
    "нагрузка (несколько секунд на сообщение), используй ТОЛЬКО когда "
    "условие по-настоящему нельзя приблизить списком слов. Для запросов "
    "вроде \"следи за рекламой\" НЕ используй semantic -- вместо этого "
    "сам придумай список типичных слов/фраз для этой категории на языке "
    "чата (например для рекламы: \"заработок\", \"инвестици\", \"стабильный "
    "доход\", \"пиши в лс\", \"переходи по ссылке\" и т.п.) и оформи их как "
    "kind=keyword. Это разовая работа при создании триггера, а не запрос к "
    "модели на каждое сообщение -- именно ради этого вообще стоит возиться "
    "с придумыванием списка, а не звать semantic.\n"
    "any — срабатывает БЕЗУСЛОВНО на каждое сообщение в чате (кроме "
    "исключений из trusted_senders/skip_admins), value не нужен -- "
    "используй когда важен сам факт нового сообщения от конкретного "
    "собеседника независимо от содержания, а не когда сообщение нужно "
    "ОТФИЛЬТРОВАТЬ по смыслу (для последнего — semantic/verify; semantic "
    "может промолчать именно на неожиданный, но важный ответ).\n"
    "action: notify — просто уведомить тебя; reply — ответить автоматически "
    "(reply_text — готовый текст ответа; если reply_text не указан, ответ "
    "каждый раз СОЧИНЯЕТСЯ заново по контексту сообщения, не передавай "
    "reply_text если ответ должен быть не шаблонным); delete — удалить "
    "сообщение (сработает только там где есть права модератора); confirm "
    "— НЕ действовать самому, а прислать в топик 'Модерация' кнопки "
    "'Удалить'/'Оставить', решение принимает человек.\n"
    "verify (необязательное поле, отдельно от action) — вот это ГЛАВНЫЙ "
    "механизм для нечётких категорий вроде рекламы: value/kind "
    "(keyword/link/button) — это ТОЛЬКО дешёвый предфильтр на подозрение, а "
    "verify — условие обычным языком (\"это сообщение является рекламой\"), "
    "которое проверяется ОДНИМ вызовом Haiku, но ТОЛЬКО для сообщений уже "
    "прошедших предфильтр (не на каждое сообщение в чате, как было бы у "
    "semantic). Результат трёхсторонний: Haiku уверенно 'да' -> action "
    "выполняется сам (например delete) без вопросов; Haiku уверенно 'нет' "
    "-> предфильтр был ложным срабатыванием, ничего не происходит; Haiku "
    "реально не уверен -> ТОЛЬКО в этом случае идёт запрос в 'Модерация' с "
    "кнопками (это единственный путь, которым появляется action=confirm "
    "при использовании verify — не путай с bare confirm без verify, см. "
    "ниже).\n"
    "trusted_senders (необязательный список id или @username) и skip_admins "
    "(необязательное true/false) — исключения по отправителю, проверяются "
    "ДО kind/verify: если сообщение прислал кто-то из trusted_senders, или "
    "(при skip_admins=true) прислал админ/владелец чата — триггер вообще не "
    "срабатывает, независимо от остальных полей. Используй когда просят "
    "игнорировать конкретных людей или доверенные роли (например \"игнорируй "
    "ссылки от админов\", \"не реагируй на сообщения от X\").\n"
    "only_senders (необязательный список id или @username) — ОБРАТНОЕ "
    "trusted_senders: триггер срабатывает ТОЛЬКО от них, все остальные "
    "отправители игнорируются. ОБЯЗАТЕЛЬНО указывай это для kind=any в "
    "ГРУППОВОМ чате (id/@username конкретного собеседника, с кем идёт "
    "дело) — без этого \"любое сообщение\" сработает на любое сообщение от "
    "любого участника группы, а не только от нужного человека, и на "
    "каждое чужое сообщение впустую потратится полный агентный вызов. Для "
    "чата 1-на-1 не обязательно.\n"
    "Правило когда что использовать: если пользователь дал ЧЁТКИЙ "
    "однозначный признак (\"удаляй сообщения с кнопками\") — просто "
    "kind=button, action=delete, БЕЗ verify, удаляет сразу без вопросов, "
    "Haiku тут вообще не нужен. Если пользователь описал НЕЧЁТКУЮ "
    "категорию, которая требует суждения (\"удаляй рекламу\") — "
    "kind=keyword (сам придумай список слов-предфильтра) + action=delete + "
    "verify с условием, ЧТО именно должен решить Haiku. Спрашивать "
    "человека на КАЖДОЕ совпадение предфильтра — это неправильно, "
    "спрашивать нужно только когда Haiku реально сомневается. Bare "
    "confirm (action=confirm БЕЗ verify) оставь для случаев где вообще "
    "нет разумной эвристики и Haiku тут ни при чём — всегда спрашивать "
    "человека напрямую по самому факту предфильтра. Пример на запрос "
    "\"следи за рекламой и кнопками, удаляй\": ОДИН вызов register_trigger "
    "со списком из двух объектов -- {kind:button, action:delete} (чёткий "
    "признак, без verify) и {kind:keyword, value:[придуманные слова], "
    "action:delete, verify:\"это сообщение является рекламой\"} "
    "(нечёткая категория, Haiku решает, человек — только при сомнении).\n"
    "action=agent + instruction — САМОЕ ГИБКОЕ действие: вместо фиксированного "
    "notify/reply/delete, при срабатывании ты САМ (полноценный вызов со всеми "
    "твоими tools -- send_message в любой чат/топик, delete_messages и "
    "т.д.) выполняешь instruction -- инструкцию обычным языком, оставленную "
    "заранее при регистрации триггера. Используй когда нужное действие не "
    "сводится к notify/reply/delete/confirm -- например \"пришли ссылку на "
    "сообщение в топик 3399019582/1 с пометкой что нашёл ты\" превращается "
    "в {kind:..., action:agent, instruction:\"увидев нарушение, отправь "
    "через send_message в '3399019582/1' сообщение со ссылкой на это "
    "сообщение и пометкой что нарушение нашёл ты\"}. Можно сочетать с "
    "verify, как и с delete — предфильтр решает КОГДА сработать, "
    "instruction решает ЧТО сделать.\n"
    "Важный частный случай action=agent: если тебя просят самому "
    "поучаствовать в переписке от имени пользователя (написать в чат X и "
    "разобраться/договориться/уладить вопрос) — после отправки первого "
    "сообщения ОБЯЗАТЕЛЬНО зарегистрируй в том же чате X триггер "
    "{kind:any, action:agent, instruction:\"<суть задачи и текущий "
    "статус>, при следующем сообщении собеседника реши: ответить ли ещё "
    "раз, считать вопрос закрытым (тогда вызови remove_trigger на этот "
    "же id) или нужно решение пользователя (тогда не отвечай собеседнику, "
    "а просто опиши ситуацию)\"}. Если чат X групповой — ОБЯЗАТЕЛЬНО "
    "добавь only_senders с id/@username именно того собеседника, иначе "
    "триггер сработает на сообщение от любого другого участника группы, "
    "а не только от нужного человека. Без самого факта регистрации "
    "триггера ты не узнаешь о следующем сообщении собеседника, и разговор "
    "оборвётся после первой реплики — не говори пользователю \"я тебе "
    "скажу, когда ответят\", если такой триггер не зарегистрирован "
    "по-настоящему.\n"
    "Приоритет при нескольких сработавших сразу: delete > confirm > agent "
    "> reply > notify. label — краткое описание для тебя же самого при "
    "просмотре списка триггеров (list_triggers).\n\n"
    "Формат ответа: HTML (<b>жирный</b>, <i>курсив</i>, <code>код</code>, "
    "<pre>блок</pre>). Вместо тире (—) используй дефис (-) — лаконичнее и "
    "живее."
)

# Per-instance chat persona. DEFAULT_INSTANCE ("andrey") keeps BASE_PERSONA
# as-is. Other instance_ids (e.g. a family member's own separate userbot)
# get their own entry here -- only "chat" mode is instance-scoped, since
# search/translate are stateless utility calls with no persona baked in
# worth branding per instance.
INSTANCE_PERSONAS = {
    DEFAULT_INSTANCE: BASE_PERSONA,
    "anatoly": ANATOLY_PERSONA,
}

SYSTEM_PROMPTS = {
    "chat": BASE_PERSONA,
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
    topic_id: str = None, exclude_id: str = None,
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
    topic_id: str = None, exclude_id: str = None,
):
    if mode == "chat":
        system = INSTANCE_PERSONAS.get(instance_id, BASE_PERSONA)
    else:
        system = SYSTEM_PROMPTS.get(mode, SYSTEM_PROMPTS["chat"])
    if chat_id == FATHER_CHAT_ID:
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
        topic_id=topic_id, exclude_id=exclude_id,
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
    topic_id: str = None, exclude_id: str = None,
):
    result_path = os.path.join(RESULT_DIR, f"{req_id}.json")
    with _CONCURRENCY:
        try:
            print(f"[{instance_id}:{chat_id}:{mode}] Q: {question[:80]}...", flush=True)
            answer, thoughts = call_llm(
                question, chat_id, mode, req_id, instance_id, topic_id=topic_id, exclude_id=exclude_id,
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
    topic_id: str = None, exclude_id: str = None,
):
    # Stateless utility modes share no resumable session and stay parallel.
    if mode != "chat":
        return _process_request(req_id, question, chat_id, mode, instance_id, topic_id, exclude_id)
    with _chat_request_lock(instance_id, chat_id):
        return _process_request(req_id, question, chat_id, mode, instance_id, topic_id, exclude_id)


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
                threading.Thread(
                    target=_process_request_serialized,
                    args=(req_id, question, chat_id, mode, instance_id),
                    kwargs={"topic_id": topic_id, "exclude_id": exclude_id},
                    daemon=True,
                ).start()
            time.sleep(1)
        except Exception as e:
            print(f"ERR: {e}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
