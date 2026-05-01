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
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    BusinessConnection,
    BusinessMessagesDeleted,
    FSInputFile,
    Message,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "messages.db")
CACHE_DIR = Path(os.getenv("MEDIA_CACHE_DIR", "media_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

EXT_BY_TYPE = {
    "photo": ".jpg",
    "video": ".mp4",
    "animation": ".mp4",
    "video_note": ".mp4",
    "voice": ".ogg",
    "audio": ".mp3",
    "document": ".bin",
    "sticker": ".webp",
}

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
            media_type    TEXT,
            media_file_id TEXT,
            created_at    TEXT NOT NULL,
            PRIMARY KEY (chat_id, message_id)
        );
        """
    )
    cols = {row[1] for row in c.execute("PRAGMA table_info(messages)").fetchall()}
    if "media_type" not in cols:
        c.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
    if "media_file_id" not in cols:
        c.execute("ALTER TABLE messages ADD COLUMN media_file_id TEXT")
    if "media_local_path" not in cols:
        c.execute("ALTER TABLE messages ADD COLUMN media_local_path TEXT")
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


def extract_media(message: Message) -> tuple[str | None, str | None]:
    if message.photo:
        return "photo", message.photo[-1].file_id
    if message.video:
        return "video", message.video.file_id
    if message.animation:
        return "animation", message.animation.file_id
    if message.document:
        return "document", message.document.file_id
    if message.audio:
        return "audio", message.audio.file_id
    if message.voice:
        return "voice", message.voice.file_id
    if message.video_note:
        return "video_note", message.video_note.file_id
    if message.sticker:
        return "sticker", message.sticker.file_id
    return None, None


async def send_saved_media(
    bot: Bot,
    chat_id: int,
    media_type: str,
    file_id: str | None,
    caption: str,
    local_path: str | None = None,
) -> None:
    if local_path and Path(local_path).exists():
        media: FSInputFile | str = FSInputFile(local_path)
    elif file_id:
        media = file_id
    else:
        await bot.send_message(chat_id, caption + "\n\n⚠️ Медиа недоступно.")
        return

    if media_type == "photo":
        await bot.send_photo(chat_id, media, caption=caption)
    elif media_type == "video":
        await bot.send_video(chat_id, media, caption=caption)
    elif media_type == "animation":
        await bot.send_animation(chat_id, media, caption=caption)
    elif media_type == "document":
        await bot.send_document(chat_id, media, caption=caption)
    elif media_type == "audio":
        await bot.send_audio(chat_id, media, caption=caption)
    elif media_type == "voice":
        await bot.send_voice(chat_id, media, caption=caption)
    elif media_type == "video_note":
        if caption:
            await bot.send_message(chat_id, caption)
        await bot.send_video_note(chat_id, media)
    elif media_type == "sticker":
        if caption:
            await bot.send_message(chat_id, caption)
        await bot.send_sticker(chat_id, media)
    else:
        await bot.send_message(chat_id, caption)


async def cache_media_file(
    bot: Bot,
    chat_id: int,
    message_id: int,
    media_type: str,
    file_id: str,
) -> None:
    """Скачивает медиа-файл сразу при получении и сохраняет путь в БД.

    Это нужно потому, что у исчезающих фото/видео file_id одноразовый —
    Telegram не даст переотправить его позже. Поэтому файл нужно скачать
    немедленно, пока он ещё доступен.
    """
    try:
        ext = EXT_BY_TYPE.get(media_type, ".bin")
        local_path = CACHE_DIR / f"{chat_id}_{message_id}{ext}"
        await bot.download(file_id, destination=local_path)
        db.execute(
            "UPDATE messages SET media_local_path = ? WHERE chat_id = ? AND message_id = ?",
            (str(local_path), chat_id, message_id),
        )
        db.commit()
        log.info(
            "cached %s %s/%s -> %s (%d bytes)",
            media_type, chat_id, message_id, local_path,
            local_path.stat().st_size if local_path.exists() else -1,
        )
    except Exception as e:
        log.warning(
            "cache_media_file failed for %s %s/%s: %s",
            media_type, chat_id, message_id, e,
        )


def owner_chat_for(connection_id: str | None) -> int | None:
    if not connection_id:
        return None
    row = db.execute(
        "SELECT user_chat_id FROM connections WHERE connection_id = ?",
        (connection_id,),
    ).fetchone()
    return row[0] if row else None


def owner_info_for(connection_id: str | None) -> tuple[int, int] | None:
    if not connection_id:
        return None
    row = db.execute(
        "SELECT user_chat_id, user_id FROM connections WHERE connection_id = ?",
        (connection_id,),
    ).fetchone()
    return (row[0], row[1]) if row else None


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
        media_type, media_file_id = extract_media(message)
        log.info(
            "business_message chat=%s msg=%s from=%s media=%s reply_to=%s",
            message.chat.id,
            message.message_id,
            message.from_user.id if message.from_user else None,
            media_type,
            message.reply_to_message.message_id if message.reply_to_message else None,
        )
        db.execute(
            "INSERT OR REPLACE INTO messages "
            "(chat_id, message_id, connection_id, user_id, user_name, chat_title, text, media_type, media_file_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                message.chat.id,
                message.message_id,
                message.business_connection_id,
                message.from_user.id if message.from_user else None,
                fmt_user(message),
                fmt_chat(message),
                text,
                media_type,
                media_file_id,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        db.commit()

        # Скачиваем медиа сразу — file_id у исчезающих сообщений одноразовый.
        if media_type and media_file_id:
            asyncio.create_task(
                cache_media_file(
                    bot, message.chat.id, message.message_id, media_type, media_file_id
                )
            )

        # Если владелец ответил на медиа-сообщение собеседника — переслать его
        # в ЛС с ботом без ограничений на просмотр (для исчезающих фото/видео/кружков).
        info = owner_info_for(message.business_connection_id)
        if info is None:
            log.info("reply skip: no connection info")
            return
        if message.reply_to_message is None:
            return
        if message.from_user is None or message.from_user.id != info[1]:
            log.info(
                "reply skip: not from owner (from=%s owner=%s)",
                message.from_user.id if message.from_user else None,
                info[1],
            )
            return

        owner_chat, owner_user_id = info
        replied_id = message.reply_to_message.message_id

        # Если в исходной БД-записи ещё нет медиа (например, мы только что её
        # получили этим же апдейтом или она не была сохранена) — попробуем взять
        # медиа прямо из reply_to_message.
        replied = db.execute(
            "SELECT user_id, user_name, text, media_type, media_file_id, media_local_path FROM messages "
            "WHERE chat_id = ? AND message_id = ?",
            (message.chat.id, replied_id),
        ).fetchone()

        replied_user_id = replied[0] if replied else None
        replied_user_name = replied[1] if replied else None
        replied_text = replied[2] if replied else ""
        replied_media_type = replied[3] if replied else None
        replied_media_file_id = replied[4] if replied else None
        replied_local_path = replied[5] if replied else None

        if not replied_media_type or not replied_media_file_id:
            r_type, r_fid = extract_media(message.reply_to_message)
            if r_type and r_fid:
                replied_media_type = r_type
                replied_media_file_id = r_fid
                replied_text = replied_text or extract_text(message.reply_to_message)
                if message.reply_to_message.from_user:
                    replied_user_id = message.reply_to_message.from_user.id
                    replied_user_name = replied_user_name or (
                        " ".join(
                            filter(
                                None,
                                [
                                    message.reply_to_message.from_user.first_name,
                                    message.reply_to_message.from_user.last_name,
                                ],
                            )
                        )
                        or "?"
                    )

        log.info(
            "reply by owner: replied_id=%s found_in_db=%s media_type=%s has_file_id=%s has_local=%s replied_user=%s",
            replied_id,
            bool(replied),
            replied_media_type,
            bool(replied_media_file_id),
            bool(replied_local_path),
            replied_user_id,
        )

        if not replied_media_type:
            log.info("reply skip: replied message has no media")
            return
        if replied_user_id == owner_user_id:
            log.info("reply skip: replied message is from owner")
            return

        caption = (
            "🔓 <b>Медиа без ограничений на просмотр</b>\n"
            f"Чат: {escape(fmt_chat(message))}\n"
            f"Автор: {escape(replied_user_name or '?')}"
        )
        if replied_text:
            caption += f"\n\n<blockquote>{escape(replied_text)}</blockquote>"

        # Если локальной копии ещё нет, но есть file_id — попробуем скачать
        # прямо сейчас (фоновое скачивание могло не успеть завершиться).
        if not replied_local_path and replied_media_file_id:
            try:
                await cache_media_file(
                    bot, message.chat.id, replied_id, replied_media_type, replied_media_file_id
                )
                row2 = db.execute(
                    "SELECT media_local_path FROM messages WHERE chat_id = ? AND message_id = ?",
                    (message.chat.id, replied_id),
                ).fetchone()
                replied_local_path = row2[0] if row2 else None
            except Exception as e:
                log.warning("on-demand cache failed: %s", e)

        try:
            await send_saved_media(
                bot,
                owner_chat,
                replied_media_type,
                replied_media_file_id,
                caption,
                local_path=replied_local_path,
            )
            log.info("forwarded unrestricted media to owner_chat=%s", owner_chat)
        except Exception as e:
            log.warning("send unrestricted media failed: %s", e)
            try:
                await bot.send_message(
                    owner_chat,
                    caption + f"\n\n⚠️ Не удалось переслать медиа ({replied_media_type}): {escape(str(e))}",
                )
            except Exception as e2:
                log.warning("send unrestricted fallback failed: %s", e2)

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
                "SELECT user_name, chat_title, text, media_type, media_file_id, media_local_path FROM messages "
                "WHERE chat_id = ? AND message_id = ?",
                (event.chat.id, msg_id),
            ).fetchone()

            if row:
                user_name, chat_title, text, media_type, media_file_id, media_local_path = row
                header = (
                    "🗑 <b>Сообщение удалено</b>\n"
                    f"Чат: {escape(chat_title)}\n"
                    f"Автор: {escape(user_name)}"
                )
                if media_type and (media_file_id or media_local_path):
                    caption = header
                    if text:
                        caption += f"\n\n<blockquote>{escape(text)}</blockquote>"
                    try:
                        await send_saved_media(
                            bot, owner, media_type, media_file_id, caption,
                            local_path=media_local_path,
                        )
                    except Exception as e:
                        log.warning("send deleted media failed: %s", e)
                        try:
                            await bot.send_message(
                                owner,
                                caption + f"\n\n⚠️ Не удалось переслать медиа ({media_type}): {escape(str(e))}",
                            )
                        except Exception as e2:
                            log.warning("send delete fallback failed: %s", e2)
                else:
                    report = (
                        f"{header}\n\n"
                        f"<blockquote>{escape(text) or '(пусто)'}</blockquote>"
                    )
                    try:
                        await bot.send_message(owner, report)
                    except Exception as e:
                        log.warning("send delete report failed: %s", e)
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
