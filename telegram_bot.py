from __future__ import annotations

import html
import os
import time
from typing import Any

import requests

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
WEB_API_URL = os.environ.get("WEB_API_URL", "http://web:5000/api/incoming_tasks")
TELEGRAM_AUTH_URL = os.environ.get("TELEGRAM_AUTH_URL", "http://web:5000/api/telegram_auth/request_code")
INCOMING_API_KEY = os.environ.get("INCOMING_API_KEY", "dev-incoming-token")
POLL_TIMEOUT = int(os.environ.get("TELEGRAM_POLL_TIMEOUT", "25"))

# Простое состояние в памяти: чат нажал кнопку "Создать задачу",
# следующий текст от него считаем текстом задачи.
PENDING_TASK_CHATS: dict[int, float] = {}
PENDING_TASK_TTL_SECONDS = 15 * 60

BTN_LOGIN = "🔐 Получить код"
BTN_TASK = "📝 Создать задачу"


def main_keyboard() -> dict[str, Any]:
    return {
        "keyboard": [
            [{"text": BTN_LOGIN}, {"text": BTN_TASK}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Выберите действие или напишите /task ...",
    }


def telegram(method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    response = requests.post(url, json=payload or {}, timeout=35)
    response.raise_for_status()
    return response.json()


def send_message(chat_id: int, text: str, *, keyboard: bool = True) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if keyboard:
        payload["reply_markup"] = main_keyboard()
    telegram("sendMessage", payload)


def setup_bot_commands() -> None:
    telegram(
        "setMyCommands",
        {
            "commands": [
                {"command": "start", "description": "Открыть меню"},
                {"command": "login", "description": "Получить код для входа"},
                {"command": "task", "description": "Создать задачу"},
                {"command": "cancel", "description": "Отменить ввод задачи"},
                {"command": "help", "description": "Помощь"},
            ]
        },
    )


def normalize_task_text(text: str) -> str:
    raw = text.strip()
    for command in ("/task", "/newtask"):
        if raw.startswith(command):
            return raw[len(command):].strip()
    return raw


def parse_task(text: str) -> dict[str, Any]:
    raw = normalize_task_text(text)

    tags = []
    cleaned_words = []
    for word in raw.split():
        if word.startswith("#") and len(word) > 1:
            tags.append(word[1:].strip())
        else:
            cleaned_words.append(word)

    title = " ".join(cleaned_words).strip() or raw
    return {
        "title": title,
        "tags": ", ".join(tags),
        "source": "telegram",
        "description": "Задача создана через Telegram-бота",
    }


def push_to_queue(payload: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        WEB_API_URL,
        json=payload,
        headers={"X-API-Key": INCOMING_API_KEY},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def request_login_code(user: dict[str, Any]) -> dict[str, Any]:
    response = requests.post(
        TELEGRAM_AUTH_URL,
        json={
            "telegram_id": str(user.get("id")),
            "telegram_username": user.get("username") or "",
            "first_name": user.get("first_name") or "",
            "last_name": user.get("last_name") or "",
        },
        headers={"X-API-Key": INCOMING_API_KEY},
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def send_help(chat_id: int) -> None:
    send_message(
        chat_id,
        "👋 <b>NineCell бот</b>\n\n"
        "Я умею две главные вещи:\n"
        f"{BTN_LOGIN} — получить одноразовый код для входа на сайт.\n"
        f"{BTN_TASK} — создать задачу через входящую очередь RabbitMQ.\n\n"
        "Можно также писать командами:\n"
        "<code>/login</code>\n"
        "<code>/task Срочно: баг оплаты #bug #urgent</code>\n\n"
        "Задачи идут по цепочке: Telegram → Flask API → PostgreSQL Outbox → RabbitMQ → worker → Kanban.",
    )


def handle_login(chat_id: int, user: dict[str, Any]) -> None:
    try:
        result = request_login_code(user)
        code = result.get("code")
        expires = int(result.get("expires_in", 600))
        minutes = max(1, expires // 60)
        send_message(
            chat_id,
            "🔐 <b>Код для входа в NineCell</b>\n\n"
            f"<code>{html.escape(str(code))}</code>\n\n"
            f"Код действует примерно {minutes} минут. Введите его на странице входа или регистрации.",
        )
    except Exception as exc:  # noqa: BLE001
        send_message(chat_id, f"⚠️ Не удалось создать код входа: <code>{html.escape(str(exc))}</code>")


def handle_task_text(chat_id: int, message: dict[str, Any], text: str) -> None:
    payload = parse_task(text)

    if not payload["title"].strip():
        send_message(chat_id, "⚠️ Я не вижу названия задачи. Напишите, например: <code>Срочно баг оплаты #bug</code>")
        return

    payload["external_id"] = f"tg-{chat_id}-{message.get('message_id')}"

    try:
        result = push_to_queue(payload)
        send_message(
            chat_id,
            "✅ <b>Задача принята в очередь</b>\n\n"
            f"Название: <code>{html.escape(payload['title'])}</code>\n"
            f"Теги: <code>{html.escape(payload.get('tags') or 'нет')}</code>\n"
            f"Status: <b>{html.escape(str(result.get('status')))}</b>\n\n"
            "Скоро она появится на канбан-доске.",
        )
    except Exception as exc:  # noqa: BLE001
        send_message(chat_id, f"⚠️ Не удалось отправить задачу: <code>{html.escape(str(exc))}</code>")


def cleanup_pending() -> None:
    now = time.time()
    expired = [chat_id for chat_id, created_at in PENDING_TASK_CHATS.items() if now - created_at > PENDING_TASK_TTL_SECONDS]
    for chat_id in expired:
        PENDING_TASK_CHATS.pop(chat_id, None)


def handle_message(message: dict[str, Any]) -> None:
    cleanup_pending()

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = str(message.get("text", "")).strip()
    if not chat_id or not text:
        return

    user = message.get("from", {})

    if text in ("/start", "/help") or text.startswith("/start") or text.startswith("/help"):
        PENDING_TASK_CHATS.pop(chat_id, None)
        send_help(chat_id)
        return

    if text == "/cancel":
        PENDING_TASK_CHATS.pop(chat_id, None)
        send_message(chat_id, "Ок, отменено. Выберите действие кнопкой ниже.")
        return

    if text == BTN_LOGIN or text.startswith("/login"):
        PENDING_TASK_CHATS.pop(chat_id, None)
        handle_login(chat_id, user)
        return

    if text == BTN_TASK:
        PENDING_TASK_CHATS[chat_id] = time.time()
        send_message(
            chat_id,
            "📝 Напишите текст задачи следующим сообщением.\n\n"
            "Пример:\n"
            "<code>Срочно: баг оплаты #bug #urgent #payment</code>\n\n"
            "Отмена: <code>/cancel</code>",
        )
        return

    if text.startswith("/task") or text.startswith("/newtask"):
        PENDING_TASK_CHATS.pop(chat_id, None)
        handle_task_text(chat_id, message, text)
        return

    if chat_id in PENDING_TASK_CHATS:
        PENDING_TASK_CHATS.pop(chat_id, None)
        handle_task_text(chat_id, message, text)
        return

    send_message(
        chat_id,
        "Я не понял сообщение. Нажмите кнопку ниже или используйте:\n"
        "<code>/login</code> — код входа\n"
        "<code>/task Срочно баг оплаты #bug</code> — создать задачу",
    )


def main() -> None:
    if not BOT_TOKEN:
        print("[TelegramBot] TELEGRAM_BOT_TOKEN is not set. Bot is disabled; container will stay alive.")
        while True:
            time.sleep(3600)

    print("[TelegramBot] started")
    try:
        setup_bot_commands()
    except Exception as exc:  # noqa: BLE001
        print(f"[TelegramBot] cannot set commands: {exc}")

    offset = None
    while True:
        try:
            payload: dict[str, Any] = {"timeout": POLL_TIMEOUT}
            if offset is not None:
                payload["offset"] = offset
            updates = telegram("getUpdates", payload)
            for update in updates.get("result", []):
                offset = update["update_id"] + 1
                if "message" in update:
                    handle_message(update["message"])
        except Exception as exc:  # noqa: BLE001
            print(f"[TelegramBot] error: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
