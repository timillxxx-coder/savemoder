"""
Telegram Business bot: отслеживает редактирование и удаление сообщений
в чатах пользователя, подключившего бота через Telegram Business.

Подключение со стороны пользователя:
    Настройки Telegram → Telegram Business → Чат-боты →
    указать @username этого бота. Требуется Telegram Premium.

Зависимости:
    pip install "aiogram>=3.7"

Запуск:
    set BOT_TOKEN=...
    python bot.py
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timezone
from html import escape

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    BusinessConnection,
    BusinessMessagesDeleted,
    Message,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "messages.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("watcher")


def init_db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS connections (
            connection_id TEXT PRIMARY KEY,
            user_chat_id  INTEGER NOT NULL,
            user_id       INTEGER NOT NULL,
            is_enabled    INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS messages (
            chat_id       INTEGER NOT NULL,
            message_id    INTEGER NOT NULL,
            connection_id TEXT,
            user_id       INTEGER,
            user_name     TEXT,
            chat_title    TEXT,
            text          TEXT,
            created_at    TEXT NOT NULL,
            PRIMARY KEY (chat_id, message_id)
        );
        """
    )
    c.commit()
    return c


db = init_db()


def fmt_user(message: Message) -> str:
    u = message.from_user
    if u is None:
        return "неизвестно"
    name = " ".join(filter(None, [u.first_name, u.last_name])) or "?"
    return f"{name} (@{u.username})" if u.username else f"{name} [id={u.id}]"


def fmt_chat(message: Message) -> str:
    chat = message.chat
    if chat.type == "private":
        name = " ".join(filter(None, [chat.first_name, chat.last_name])) or "?"
        return f"ЛС: {name}" + (f" (@{chat.username})" if chat.username else "")
    return chat.title or "?"


def extract_text(message: Message) -> str:
    return message.text or message.caption or ""


def owner_chat_for(connection_id: str | None) -> int | None:
    if not connection_id:
        return None
    row = db.execute(
        "SELECT user_chat_id FROM connections WHERE connection_id = ?",
        (connection_id,),
    ).fetchone()
    return row[0] if row else None


async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("Установите переменную окружения BOT_TOKEN")

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    # /start — обычный апдейт type=message в личке с ботом.
    @dp.message(CommandStart())
    async def on_start(message: Message) -> None:
        await message.answer(
            "👋 Привет! Я Business-бот, слежу за правками и удалениями сообщений.\n\n"
            "<b>Как подключить:</b>\n"
            "Настройки Telegram → <b>Telegram Business</b> → <b>Чат-боты</b> → "
            "указать мой @username. Нужен Telegram Premium.\n\n"
            "После подключения я буду присылать сюда уведомления о каждом "
            "редактировании и удалении сообщения в ваших чатах."
        )

    @dp.business_connection()
    async def on_connection(bc: BusinessConnection) -> None:
        enabled = getattr(bc, "is_enabled", getattr(bc, "enabled", True))
        db.execute(
            "INSERT OR REPLACE INTO connections "
            "(connection_id, user_chat_id, user_id, is_enabled) VALUES (?, ?, ?, ?)",
            (bc.id, bc.user_chat_id, bc.user.id, int(bool(enabled))),
        )
        db.commit()
        log.info("Business connection %s user=%s enabled=%s", bc.id, bc.user.id, enabled)
        if enabled:
            try:
                await bot.send_message(
                    bc.user_chat_id,
                    "✅ Подключено. Уведомления о правках и удалениях будут приходить сюда.",
                )
            except Exception as e:
                log.warning("send_message failed: %s", e)

    @dp.business_message()
    async def on_business_message(message: Message) -> None:
        text = extract_text(message)
        db.execute(
            "INSERT OR REPLACE INTO messages "
            "(chat_id, message_id, connection_id, user_id, user_name, chat_title, text, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                message.chat.id,
                message.message_id,
                message.business_connection_id,
                message.from_user.id if message.from_user else None,
                fmt_user(message),
                fmt_chat(message),
                text,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        db.commit()

    @dp.edited_business_message()
    async def on_business_edit(message: Message) -> None:
        new_text = extract_text(message)
        row = db.execute(
            "SELECT user_name, chat_title, text FROM messages "
            "WHERE chat_id = ? AND message_id = ?",
            (message.chat.id, message.message_id),
        ).fetchone()

        if row:
            user_name, chat_title, old_text = row
        else:
            user_name = fmt_user(message)
            chat_title = fmt_chat(message)
            old_text = "(не было в базе — бот тогда ещё не получал сообщение)"

        if old_text == new_text:
            return

        owner = owner_chat_for(message.business_connection_id)
        if owner:
            report = (
                "✏️ <b>Сообщение отредактировано</b>\n"
                f"Чат: {escape(chat_title)}\n"
                f"Автор: {escape(user_name)}\n\n"
                f"<b>Было:</b>\n<blockquote>{escape(old_text) or '(пусто)'}</blockquote>\n"
                f"<b>Стало:</b>\n<blockquote>{escape(new_text) or '(пусто)'}</blockquote>"
            )
            try:
                await bot.send_message(owner, report)
            except Exception as e:
                log.warning("send edit report failed: %s", e)

        db.execute(
            "UPDATE messages SET text = ? WHERE chat_id = ? AND message_id = ?",
            (new_text, message.chat.id, message.message_id),
        )
        db.commit()

    @dp.deleted_business_messages()
    async def on_business_delete(event: BusinessMessagesDeleted) -> None:
        owner = owner_chat_for(event.business_connection_id)
        if not owner:
            log.warning("Удаление в неизвестном connection_id=%s", event.business_connection_id)
            return

        for msg_id in event.message_ids:
            row = db.execute(
                "SELECT user_name, chat_title, text FROM messages "
                "WHERE chat_id = ? AND message_id = ?",
                (event.chat.id, msg_id),
            ).fetchone()

            if row:
                user_name, chat_title, text = row
                report = (
                    "🗑 <b>Сообщение удалено</b>\n"
                    f"Чат: {escape(chat_title)}\n"
                    f"Автор: {escape(user_name)}\n\n"
                    f"<blockquote>{escape(text) or '(пусто)'}</blockquote>"
                )
            else:
                report = (
                    "🗑 Удалено сообщение, которого нет в локальной базе "
                    f"(chat_id={event.chat.id}, message_id={msg_id})."
                )
            try:
                await bot.send_message(owner, report)
            except Exception as e:
                log.warning("send delete report failed: %s", e)

    me = await bot.get_me()
    log.info("Запущен @%s (id=%s)", me.username, me.id)
    await dp.start_polling(
        bot,
        allowed_updates=[
            "message",
            "edited_message",
            "business_connection",
            "business_message",
            "edited_business_message",
            "deleted_business_messages",
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())
