#!/usr/bin/env python3
"""Local MCP server (stdio) exposing real Telegram actions as tools for
`claude -p --mcp-config=...` (see claude_watcher.py), replacing the old
text-marker hack ([CREATE_GROUP:...], [SEND_MESSAGE:...] etc parsed out of
the model's final answer after the fact -- see the 2026-08-11 incident this
replaces: the model narrated "готово" before the marker even ran, because
the marker's real result was never fed back into the model's own context).
Renamed from mcp_group_tools.py once this grew past just group/person
actions to the entire former marker surface.

Runs on THIS host, same one as `claude -p`. The actual Telethon session
that can create groups/send messages/etc lives on the REMOTE userbot host
-- each tool call here does a synchronous HTTP round-trip through
cmd_queue.py (POST /tool_call to enqueue, poll GET /tool_call?request_id=...
for the result), mirroring the existing /ask relay pattern but in the
opposite direction: the remote host polls cmd_queue.py for pending tool
calls (claude_ask.py's tool_call_watcher loop) instead of this host pushing
to it directly -- there's no way to reach into a Telethon event loop on
another host except through the same funnel both sides already share.

CHAT_ID/INSTANCE_ID/TOPIC_ID/EXCLUDE_MSG_ID come from the environment,
injected by claude_watcher.py into this whole `claude -p` subprocess tree
for exactly this purpose (same trick bridge.py already uses for its own
wakeup-signal mechanism) -- so the model never has to know or pass the
current chat/topic/its-own-placeholder-message id itself.
"""
import json
import os
import time
import urllib.request
import uuid

from mcp.server.mcpserver import MCPServer

CMDQ = os.environ.get("JARVIS_CMD_QUEUE_URL", "http://127.0.0.1:9092")
POLL_TIMEOUT_S = 30
POLL_INTERVAL_S = 0.5

# ClaudeAsk historically injects CHAT_ID/INSTANCE_ID.  Codex uses the
# dedicated names because its static Codex config may still contain the
# owner's legacy values; prefer the dynamic per-process context when present.
INSTANCE_ID = os.environ.get("CODEX_TELEGRAM_INSTANCE_ID") or os.environ.get("INSTANCE_ID", "andrey")
CHAT_ID = os.environ.get("CODEX_TELEGRAM_CHAT_ID") or os.environ.get("CHAT_ID", "")
TOPIC_ID = int(os.environ.get("CODEX_TELEGRAM_TOPIC_ID") or os.environ["TOPIC_ID"]) if (
    os.environ.get("CODEX_TELEGRAM_TOPIC_ID") or os.environ.get("TOPIC_ID")
) else None
EXCLUDE_MSG_ID = int(os.environ.get("CODEX_TELEGRAM_EXCLUDE_MSG_ID") or os.environ["EXCLUDE_MSG_ID"]) if (
    os.environ.get("CODEX_TELEGRAM_EXCLUDE_MSG_ID") or os.environ.get("EXCLUDE_MSG_ID")
) else None

mcp = MCPServer("telegram-actions")


def _call_tool(tool: str, args: dict) -> str:
    req_id = str(uuid.uuid4())
    body = json.dumps({
        "request_id": req_id, "instance_id": INSTANCE_ID, "chat_id": CHAT_ID,
        "tool": tool, "args": args,
    }).encode()
    try:
        urllib.request.urlopen(
            urllib.request.Request(
                f"{CMDQ}/tool_call", data=body,
                headers={"Content-Type": "application/json"}, method="POST",
            ),
            timeout=5,
        )
    except Exception as e:
        return f"Не удалось поставить действие в очередь: {e}"

    deadline = time.time() + POLL_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        try:
            with urllib.request.urlopen(f"{CMDQ}/tool_call?request_id={req_id}", timeout=5) as r:
                data = json.loads(r.read())
        except Exception:
            continue
        if data.get("done"):
            return data.get("result") or "(пустой результат)"
    return "Таймаут: удалённый юзербот не ответил за 30 секунд."


@mcp.tool()
def resolve_person(query: str) -> str:
    """Найти человека среди СУЩЕСТВУЮЩИХ диалогов пользователя в Telegram
    (не глобальный поиск -- только те, с кем уже была переписка, неважно
    насколько старой). Точный @username даёт однозначный результат. Обычное
    имя может вернуть несколько совпадений -- если результат неоднозначен,
    не вызывай create_group/invite_to_group/send_message вслепую, сначала
    уточни у пользователя, кого именно он имел в виду."""
    return _call_tool("resolve_person", {"query": query})


@mcp.tool()
def create_group(title: str, members: list[str] | None = None) -> str:
    """Создать новую группу в Telegram и сразу добавить в неё людей (по
    id/@username из resolve_person, или точному имени/username если он
    однозначен). members можно оставить пустым -- тогда просто создаст
    пустую группу. Возвращает РЕАЛЬНЫЙ результат (кто добавлен, кого не
    нашли, у кого приватность не позволила) -- отвечай пользователю по
    этому результату, а не заранее."""
    return _call_tool("create_group", {"title": title, "members": members or []})


@mcp.tool()
def invite_to_group(members: list[str], group: str = "") -> str:
    """Добавить людей в УЖЕ существующую группу. group -- точное название
    существующей группы или её id; пусто = ТЕКУЩИЙ чат, из которого задали
    вопрос. Возвращает реальный результат добавления по каждому имени --
    ПРОВЕРЕННЫЙ настоящим членством в группе, не просто "API не вернул
    ошибку" (Telegram умеет молча дропать прямое добавление без единой
    ошибки, если получатель не взаимный контакт или у него закрыта
    приватность "кто может добавлять в группы"). В таком случае тул сам
    экспортирует инвайт-ссылку и шлёт её человеку в личку -- отдельно
    вызывать get_invite_link для этого не нужно, это уже встроено."""
    return _call_tool("invite_to_group", {"group": group, "members": members})


@mcp.tool()
def get_invite_link(group: str = "") -> str:
    """Сгенерировать ссылку-приглашение в группу/канал. group -- точное
    название или id; пусто = ТЕКУЩИЙ чат. invite_to_group уже делает это
    сам и шлёт ссылку в личку, когда прямое добавление молча не сработало
    -- используй этот тул отдельно только когда явно просят именно ссылку
    (например показать её самому пользователю, а не отправлять кому-то
    ещё)."""
    return _call_tool("get_invite_link", {"group": group})


@mcp.tool()
def send_message(target: str, text: str) -> str:
    """Отправить сообщение НАПРЯМУЮ, отдельным сообщением, в личку человеку
    ИЛИ в другую группу (включая ту, что сам только что создал через
    create_group), а не в текущий разговор. target -- id, @username, точное
    название группы, или 'chat_id/topic_id' (например '3399019582/1') чтобы
    попасть в конкретный топик форума, а не в его General. Если не уверен,
    кто это -- сначала вызови resolve_person."""
    return _call_tool("send_message", {"target": target, "text": text})


@mcp.tool()
def send_message_as_bot(target: str, text: str) -> str:
    """То же самое, что send_message (тот же формат target, включая
    'chat_id/topic_id'), но сообщение уходит от имени бота, а не от
    собственного аккаунта пользователя. Используй когда явно просят
    прислать куда-то ИМЕННО от бота (например в группу, куда бота
    специально добавили для этого) -- send_message в такой ситуации не
    подходит, там отправителем всегда будет личный аккаунт. Сработает
    только если бот реально состоит в целевом чате."""
    return _call_tool("send_message_as_bot", {"target": target, "text": text})


@mcp.tool()
def send_file(path: str, target: str = "") -> str:
    """Отправить получателю файл, который ты только что записал на диск
    своими файловыми инструментами (Write/Bash), отдельным сообщением.
    path -- абсолютный путь к файлу. target -- id/@username/точное название
    группы/'chat_id/topic_id'; пусто = отправить в ТЕКУЩИЙ чат, из которого
    задали вопрос."""
    return _call_tool("send_file", {"path": path, "target": target})


@mcp.tool()
def add_contact(target: str) -> str:
    """Добавить человека (id или @username, среди существующих диалогов) в
    контакты."""
    return _call_tool("add_contact", {"target": target})


@mcp.tool()
def remove_contact(target: str) -> str:
    """Убрать человека из контактов."""
    return _call_tool("remove_contact", {"target": target})


@mcp.tool()
def block_user(target: str) -> str:
    """Заблокировать человека (работает и для ботов тоже -- это те же
    диалоги)."""
    return _call_tool("block_user", {"target": target})


@mcp.tool()
def unblock_user(target: str) -> str:
    """Разблокировать человека."""
    return _call_tool("unblock_user", {"target": target})


@mcp.tool()
def leave_chat(target: str = "") -> str:
    """Выйти из группы или канала (НЕ удаляет её для остальных участников --
    уходишь только ты; удалить группу целиком нельзя вообще, только выйти).
    target -- точное название или id; пусто/'this'/'здесь' = чат, из
    которого задали вопрос."""
    return _call_tool("leave_chat", {"target": target})


@mcp.tool()
def register_trigger(specs: list[dict] | dict, chat: str = "") -> str:
    """Завести автоматическое правило на входящие сообщения в чате. chat --
    пусто/'this' = текущий чат. specs -- один объект или список объектов
    (регистрирует сразу несколько триггеров одним вызовом), каждый вида
    {"kind": "keyword"|"link"|"button"|"semantic"|"any", "value": ...,
    "action": "notify"|"reply"|"delete"|"confirm"|"agent"|"post", "verify": "...",
    "instruction": "...", "reply_text": "...", "label": "...",
    "trusted_senders": [...], "only_senders": [...], "skip_admins": true|false,
    "target": "...", "template": "...", "as_bot": true|false}.
    trusted_senders/skip_admins -- исключения по отправителю (id, @username
    или "не трогать админов/владельца"), проверяются раньше всего остального
    и просто гасят срабатывание для этого отправителя, что бы ещё ни было
    указано в триггере. only_senders -- ОБРАТНОЕ: список id/@username, и
    триггер срабатывает ТОЛЬКО от них, все остальные отправители в чате
    игнорируются (пусто/не указано = без ограничения, как раньше). Это
    ОБЯЗАТЕЛЬНО указывать для kind=any в ГРУППОВОМ чате -- иначе "любое
    сообщение" означает буквально любое сообщение от любого участника
    группы, а не только от того одного собеседника, с кем реально идёт
    дело, и на каждое чужое сообщение будет впустую тратиться полный
    агентный вызов. Для чата 1-на-1 не обязательно (там и так только два
    участника), но не помешает.
    kind=keyword: value -- список слов/фраз (точное вхождение подстроки,
    регистронезависимо). kind=link: value можно оставить пустым (просто
    любая ссылка) либо списком доменов. kind=button: сработает на сообщение
    с инлайн-кнопками, value не нужен. kind=semantic: value -- текстовое
    описание условия, проверяется дешёвой Haiku-классификацией (используй
    когда нужный сигнал не сводится к словам/ссылке/кнопке). kind=any:
    срабатывает БЕЗУСЛОВНО на любое сообщение в чате, value не нужен --
    используй когда важен сам факт нового сообщения от конкретного
    собеседника, а не его содержание (semantic тут не годится: это фильтр
    по смыслу, который может не распознать нейтральный/неожиданный ответ и
    промолчать именно тогда, когда сообщение как раз важно).
    action=notify: тихо уведомить в топик модерации, ничего в чате не
    происходит. action=reply: ответить reply_text в этом же чате.
    action=delete: удалить сработавшее сообщение. action=confirm: прислать
    кнопки подтвердить/отклонить перед действием (используй когда не уверен,
    что действие должно быть автоматическим) -- по умолчанию владельцу, в
    его личный топик Подтверждения; необязательный target (то же "chat_id"
    или "chat_id/topic_id", что у action=post) отправляет карточку вместо
    этого в конкретный чат/топик -- например обратно в ТОТ ЖЕ чат, откуда
    подозрительное сообщение, чтобы решение принимали админы ИМЕННО этого
    чата, а не только владелец. Нажать кнопки может владелец ВСЕГДА, плюс
    (если задан target) админы того чата, куда реально ушла карточка --
    остальные получат отказ по нажатию, карточка останется как есть для
    того, кто действительно может её обработать. Это же поле verify+
    action=delete тоже наследует автоматически: если Haiku «не уверен» и
    делегирует человеку (см. verify ниже), тот же target определяет, куда
    уйдёт эта эскалация. action=post:
    детерминированно отправить готовое сообщение в конкретный чат/топик
    (target: "chat_id" или "chat_id/topic_id", как в send_message; template
    -- опциональная строка с плейсхолдерами {label} {chat} {sender} {text}
    {urls} (text -- превью сработавшего сообщения, urls -- реальные адреса
    ссылок в нём, если есть; без template используется разумный дефолт);
    as_bot: true|false, по умолчанию true = уходит от бота, не от твоего
    аккаунта). Используй ИМЕННО ЭТО, а не action=agent, для любого "формат
    + отправка в фиксированное место" сценария (например алерты про
    рекламу/подозрительные ссылки в конкретный топик) -- никакого второго
    вызова модели, дешевле и без риска, что агент на срабатывании
    самостоятельно переоценит уже принятое verify-решение и передумает
    (реальный случай 2026-08-14: agent-триггер один раз именно так и
    смолчал про уже подтверждённую подозрительную ссылку). action=agent:
    самое гибкое -- при срабатывании instruction (обычным языком) выполняется
    полноценным агентным вызовом с доступом ко ВСЕМ этим же tools, оставь
    для случаев, где реально нужно рассуждение/несколько шагов, а не просто
    форматирование и отправка.
    verify (опционально, для keyword/link/button) -- текстовое условие,
    дополнительно проверяемое Haiku перед действием (например "это
    сообщение является рекламой") -- используй когда простое совпадение
    слова/ссылки само по себе даёт слишком много ложных срабатываний.
    ВАЖНЫЙ ПАТТЕРН: если ты сам ведёшь переписку с кем-то от имени
    пользователя (написал в чат X и ждёшь ответа, чтобы продолжить
    диалог/довести дело до конца) -- сразу после отправки сообщения заведи
    здесь {kind:"any", action:"agent", instruction:"<суть задачи и
    текущий статус>"} в ТОМ ЖЕ чате X. Если X -- групповой чат (не 1-на-1),
    ОБЯЗАТЕЛЬНО добавь only_senders с id/@username именно того собеседника,
    с кем идёт дело -- иначе триггер будет дёргаться на сообщения от всех
    остальных участников группы. Без самого факта регистрации триггера ты
    не узнаешь о следующем сообщении собеседника, и разговор оборвётся
    после первой реплики -- не обещай пользователю "я тебе скажу, когда
    ответят", если это не сделано по-настоящему. Когда дело закрыто,
    вызови remove_trigger на этот же id."""
    return _call_tool(
        "register_trigger", {"specs": specs, "chat": chat, "anchor_msg_id": EXCLUDE_MSG_ID},
    )


@mcp.tool()
def remove_trigger(trigger_id: str) -> str:
    """Удалить ранее зарегистрированный триггер по его id (см.
    list_triggers)."""
    return _call_tool("remove_trigger", {"trigger_id": trigger_id})


@mcp.tool()
def edit_trigger(trigger_id: str, updates: dict) -> str:
    """Изменить ранее зарегистрированный триггер БЕЗ remove_trigger+
    register_trigger -- используй это по умолчанию для любой правки
    существующего триггера (поменять слово в value, включить/выключить
    verify, поменять action/target/template и т.п.), а не пересоздание.
    trigger_id -- id из list_triggers. updates -- ЧАСТИЧНЫЙ объект, те же
    поля что у specs в register_trigger (kind/value/action/verify/
    instruction/reply_text/label/target/template/as_bot/trusted_senders/
    only_senders/skip_admins) -- указывай ТОЛЬКО то, что реально меняешь,
    остальное остаётся как было. Явный null у поля ЧИСТИТ его (например
    {"verify": null} снимет verify-условие). id/чат триггера и (для
    action=agent) origin_chat_id/origin_msg_id сохраняются как есть.
    Осторожно: если меняешь value, но не даёшь новый label -- старый
    label не пересчитывается автоматически, задай его явно, если он
    перестал описывать новое условие."""
    return _call_tool("edit_trigger", {"trigger_id": trigger_id, "updates": updates})


@mcp.tool()
def list_triggers(chat: str = "") -> str:
    """Показать активные автоматические триггеры. chat -- пусто = все чаты
    сразу, иначе только конкретный (id/@username/точное название/'this')."""
    return _call_tool("list_triggers", {"chat": chat})


@mcp.tool()
def delete_messages(ids: list[int]) -> str:
    """Удалить сообщения по их id (см. id=N в выдаче read_history/
    search_chat) в ТЕКУЩЕМ чате, из которого задали вопрос -- например
    "прочитай последние 20 и удали рекламу"."""
    return _call_tool("delete_messages", {"ids": ids})


@mcp.tool()
def search_chat(keyword: str, limit: int = 20, chat: str = "") -> str:
    """Найти сообщения по ключевому слову. chat -- id/@username/точное
    название чата; пусто/'this' = ТЕКУЩИЙ чат (по умолчанию). Можно искать
    в ЛЮБОМ другом чате из существующих диалогов (личка, группа, канал) --
    не только в текущем."""
    return _call_tool("search_chat", {"keyword": keyword, "limit": limit, "topic_id": TOPIC_ID, "chat": chat})


@mcp.tool()
def read_history(count: int = 50, direction: str = "", reply_id: int = 0, chat: str = "") -> str:
    """Прислать больше сообщений истории чата -- то, что уже дали в начале
    разговора, может быть неполным (только то, что накопилось с прошлого
    ответа). Используй если реально не хватает контекста, а не просто на
    всякий случай. count -- сколько сообщений (например 50), игнорируется
    при direction='today'. direction -- пусто (последние count сообщений
    чата), 'after'/'before' (count сообщений ПОСЛЕ/ДО сообщения-реплая, на
    которое юзер ответил -- работает только вместе с reply_id, который ты
    уже видишь в контексте как "Реплай на сообщение (id=N)", если он есть
    -- используй когда явно просят посмотреть вокруг конкретного
    выделенного сообщения), или 'today' -- ВСЕ сообщения чата с начала
    текущих суток по местному времени (используй на запрос вроде "прочитай
    всё за сегодня", count и reply_id при этом не нужны). chat --
    id/@username/точное название чата; пусто/'this' = ТЕКУЩИЙ чат (по
    умолчанию) -- можно читать историю ЛЮБОГО другого чата из
    существующих диалогов, не только текущего (например "глянь что там в
    переписке с папой")."""
    return _call_tool("read_history", {
        "count": count, "direction": direction or None, "reply_id": reply_id or None,
        "topic_id": TOPIC_ID, "exclude_id": EXCLUDE_MSG_ID, "chat": chat,
    })


@mcp.tool()
def forward_message(chat: str, message_id: int, to: str = "") -> str:
    """Переслать конкретное сообщение (id из выдачи read_history/
    search_chat, поле id=N) из одного чата в другой -- НАТИВНАЯ пересылка
    Telegram, работает для чего угодно: фото, голосовое, файл, обычный
    текст, без скачивания/перезаливки. chat -- где сообщение реально лежит
    (id/@username/точное название); to -- куда переслать, пусто = ТЕКУЩИЙ
    чат (обычный случай: нашёл нужное в чужом чате через read_history/
    search_chat с chat=..., теперь принеси это сюда)."""
    return _call_tool("forward_message", {"chat": chat, "message_id": message_id, "to": to})


if __name__ == "__main__":
    mcp.run()
