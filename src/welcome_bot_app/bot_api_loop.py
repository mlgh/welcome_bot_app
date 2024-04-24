import aiogram.types
from typing import Generator
from aiogram import Bot, Dispatcher
import time
import logging
import asyncio
from datetime import timezone
from welcome_bot_app.event_queue import BaseEventQueue
from welcome_bot_app.model.events import (
    BotApiChatInfo,
    BotApiChatMemberJoined,
    BotApiChatMemberLeft,
    BotApiNewTextMessage,
    BotApiUTCTimestamp,
    BasicUserInfo,
    BaseEvent,
)
from welcome_bot_app.model import LocalUTCTimestamp
from welcome_bot_app.model import ChatId, UserChatId, UserId, BotApiMessageId
from welcome_bot_app.event_log import EventLog


def extract_bot_events(
    message: aiogram.types.Message, recv_timestamp: LocalUTCTimestamp, is_edited: bool
) -> Generator[BaseEvent, None, None]:
    if message.content_type == aiogram.types.ContentType.NEW_CHAT_MEMBERS:
        if message.new_chat_members is None:
            logging.error(
                "Invalid message: content_type is NEW_CHAT_MEMBERS, but new_chat_members is None: %s",
                message,
            )
            return
        for member in message.new_chat_members:
            user_chat_id = UserChatId(
                user_id=UserId(member.id), chat_id=ChatId(message.chat.id)
            )
            basic_user_info = BasicUserInfo(
                is_bot=member.is_bot,
                first_name=member.first_name,
                last_name=member.last_name,
            )
            yield BotApiChatMemberJoined(
                recv_timestamp=recv_timestamp,
                user_chat_id=user_chat_id,
                basic_user_info=basic_user_info,
                tg_timestamp=BotApiUTCTimestamp(
                    message.date.astimezone(timezone.utc).timestamp()
                ),
                chat_info=BotApiChatInfo.from_bot_api_chat(message.chat),
            )
    elif message.content_type == aiogram.types.ContentType.LEFT_CHAT_MEMBER:
        if message.left_chat_member is None:
            logging.error(
                "Invalid message: content_type is LEFT_CHAT_MEMBER, but left_chat_member is None: %s",
                message,
            )
            return
        yield BotApiChatMemberLeft(
            recv_timestamp=recv_timestamp,
            user_chat_id=UserChatId(
                user_id=UserId(message.left_chat_member.id),
                chat_id=ChatId(message.chat.id),
            ),
            tg_timestamp=BotApiUTCTimestamp(
                message.date.astimezone(timezone.utc).timestamp()
            ),
            chat_info=BotApiChatInfo.from_bot_api_chat(message.chat),
        )
    elif message.text is not None:
        # Ignore messages from non-users for now.
        if message.from_user is None:
            return
        user_chat_id = UserChatId(
            user_id=UserId(message.from_user.id), chat_id=ChatId(message.chat.id)
        )
        basic_user_info = BasicUserInfo(
            is_bot=message.from_user.is_bot,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )
        yield BotApiNewTextMessage(
            recv_timestamp=recv_timestamp,
            user_chat_id=user_chat_id,
            basic_user_info=basic_user_info,
            text=message.text,
            is_edited=is_edited,
            message_id=BotApiMessageId(message.message_id),
            tg_timestamp=BotApiUTCTimestamp(
                message.date.astimezone(timezone.utc).timestamp()
            ),
            chat_info=BotApiChatInfo.from_bot_api_chat(message.chat),
        )


async def bot_api_main(
    bot: Bot, event_queue: BaseEventQueue, event_log: EventLog
) -> None:
    try:
        dp = Dispatcher()

        async def common_message_handler(
            message: aiogram.types.Message, is_edited: bool
        ) -> None:
            recv_timestamp = LocalUTCTimestamp(time.time())
            event_log.log_bot_api_event(recv_timestamp, message)
            try:
                bot_events = list(
                    extract_bot_events(message, recv_timestamp, is_edited=is_edited)
                )
                for event in bot_events:
                    event_log.log_base_event(recv_timestamp, event)
                await event_queue.put_events(bot_events)
            except Exception:
                logging.error("Failed to process Bot API message: %s", message)
                raise

        @dp.message()
        async def message_handler(message: aiogram.types.Message) -> None:
            await common_message_handler(message, is_edited=False)

        @dp.edited_message()
        async def edited_message_handler(message: aiogram.types.Message) -> None:
            await common_message_handler(message, is_edited=True)

        try:
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            logging.info("bot_api_main cancelled")
    except:
        logging.error("Error in bot_api_main", exc_info=True)
        raise
