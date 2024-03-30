import aiogram.types
from aiogram import Bot, Dispatcher
import time
import logging
import asyncio
from welcome_bot_app.event_storage import SqliteEventStorage
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.model import (
    BotApiNewChatMember,
    BotApiChatMemberLeft,
    BotApiNewTextMessage,
    UserKey,
    BotApiUserInfo,
)


def extract_bot_events(message: aiogram.types.Message, timestamp: float):
    if message.content_type == aiogram.types.ContentType.NEW_CHAT_MEMBERS:
        for member in message.new_chat_members:
            user_key = UserKey(user_id=member.id, chat_id=message.chat.id)
            user_info = BotApiUserInfo(
                is_bot=member.is_bot, first_name=member.first_name
            )
            yield BotApiNewChatMember(timestamp=timestamp, user_key=user_key)
    elif message.content_type == aiogram.types.ContentType.LEFT_CHAT_MEMBER:
        yield BotApiChatMemberLeft(
            timestamp=timestamp,
            user_key=UserKey(
                user_id=message.left_chat_member.id, chat_id=message.chat.id
            ),
        )
    elif message.text is not None:
        user_key = UserKey(user_id=message.from_user.id, chat_id=message.chat.id)
        user_info = BotApiUserInfo(
            is_bot=message.from_user.is_bot, first_name=message.from_user.first_name
        )
        yield BotApiNewTextMessage(
            timestamp=timestamp,
            user_key=user_key,
            user_info=user_info,
            text=message.text,
            message_id=message.message_id,
        )


async def bot_api_main(bot: Bot, event_processor: EventProcessor, event_storage : SqliteEventStorage) -> None:
    try:
        dp = Dispatcher()

        @dp.message()
        async def message_handler(message: aiogram.types.Message):
            timestamp = time.time()
            try:
                event_storage.log_raw_bot_api_event(message, timestamp)
            except Exception:
                logging.error("Failed to log raw aiogram event: %s", message, exc_info=True)
            # TODO: Veriy: if the message handler ends with exception, will Telegram
            # resend it? If yes, we should probably retry an update a few time, and
            # then log the unrecoverable error and skip the update.
            try:
                for event in extract_bot_events(message, timestamp):
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
