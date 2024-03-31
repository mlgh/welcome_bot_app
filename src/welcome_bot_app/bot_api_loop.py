import aiogram.types
from typing import Generator
from aiogram import Bot, Dispatcher
import time
import logging
import asyncio
from datetime import timezone
from welcome_bot_app.event_storage import SqliteEventStorage
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.model import (
    BotApiNewChatMember,
    BotApiChatMemberLeft,
    BotApiNewTextMessage,
    UserKey,
    BotApiUserInfo,
    Event,
)


def extract_bot_events(message: aiogram.types.Message, local_timestamp: float) -> Generator[Event, None, None] :
    if message.content_type == aiogram.types.ContentType.NEW_CHAT_MEMBERS:
        if message.new_chat_members is None:
            logging.error(
                "Invalid message: content_type is NEW_CHAT_MEMBERS, but new_chat_members is None: %s",
                message,
            )
            return
        for member in message.new_chat_members:
            user_key = UserKey(user_id=member.id, chat_id=message.chat.id)
            user_info = BotApiUserInfo(
                is_bot=member.is_bot, first_name=member.first_name
            )
            yield BotApiNewChatMember(
                local_timestamp=local_timestamp,
                user_key=user_key,
                tg_timestamp=message.date.astimezone(timezone.utc).timestamp(),
            )
    elif message.content_type == aiogram.types.ContentType.LEFT_CHAT_MEMBER:
        if message.left_chat_member is None:
            logging.error(
                "Invalid message: content_type is LEFT_CHAT_MEMBER, but left_chat_member is None: %s",
                message,
            )
            return
        yield BotApiChatMemberLeft(
            local_timestamp=local_timestamp,
            user_key=UserKey(
                user_id=message.left_chat_member.id, chat_id=message.chat.id
            ),
            tg_timestamp=message.date.astimezone(timezone.utc).timestamp(),
        )
    elif message.text is not None:
        # Ignore messages from non-users for now.
        if message.from_user is None:
            return
        user_key = UserKey(user_id=message.from_user.id, chat_id=message.chat.id)
        user_info = BotApiUserInfo(
            is_bot=message.from_user.is_bot, first_name=message.from_user.first_name
        )
        yield BotApiNewTextMessage(
            local_timestamp=local_timestamp,
            user_key=user_key,
            user_info=user_info,
            text=message.text,
            message_id=message.message_id,
            tg_timestamp=message.date.astimezone(timezone.utc).timestamp(),
        )


async def bot_api_main(
    bot: Bot, event_processor: EventProcessor, event_storage: SqliteEventStorage
) -> None:
    try:
        dp = Dispatcher()

        @dp.message()
        async def message_handler(message: aiogram.types.Message) -> None:
            local_timestamp = time.time()
            event_storage.log_raw_bot_api_event(message, local_timestamp)
            # TODO: Veriy: if the message handler ends with exception, will Telegram
            # resend it? If yes, we should probably retry an update a few time, and
            # then log the unrecoverable error and skip the update.
            try:
                for event in extract_bot_events(message, local_timestamp):
                    await event_processor.put_event(event)
            except Exception:
                logging.error("Failed to process Bot API message: %s", message)
                raise

        try:
            await dp.start_polling(bot)
        except asyncio.CancelledError:
            logging.info("bot_api_main cancelled")
    except:
        logging.error("Error in bot_api_main", exc_info=True)
        raise
