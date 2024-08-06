import aiogram.types
from typing import Generator, Optional
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
    BotApiUpdate,
    BotApiMessageReactionChanged,
    BotApiReactionEmoji,
)
from welcome_bot_app.model import LocalUTCTimestamp
from welcome_bot_app.model import ChatId, UserChatId, UserId, BotApiMessageId
from welcome_bot_app.event_log import EventLog


def extract_bot_events_from_message(
    message: aiogram.types.Message, recv_timestamp: LocalUTCTimestamp, is_edited: bool
) -> Generator[BaseEvent, None, None]:
    message_text: Optional[str] = message.text
    if message_text is None:
        message_text = message.caption
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
    elif message_text is not None:
        # XXX: mypy bug.
        assert message_text is not None
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
            text=message_text,
            is_edited=is_edited,
            message_id=BotApiMessageId(message.message_id),
            tg_timestamp=BotApiUTCTimestamp(
                message.date.astimezone(timezone.utc).timestamp()
            ),
            chat_info=BotApiChatInfo.from_bot_api_chat(message.chat),
        )


def extract_bot_events_from_message_reaction(
    message_reaction: aiogram.types.MessageReactionUpdated,
    recv_timestamp: LocalUTCTimestamp,
) -> Generator[BaseEvent, None, None]:
    if message_reaction.user is None:
        return
    user_chat_id = UserChatId(
        user_id=UserId(message_reaction.user.id),
        chat_id=ChatId(message_reaction.chat.id),
    )
    basic_user_info = BasicUserInfo(
        is_bot=message_reaction.user.is_bot,
        first_name=message_reaction.user.first_name,
        last_name=message_reaction.user.last_name,
    )
    yield BotApiMessageReactionChanged(
        recv_timestamp=recv_timestamp,
        user_chat_id=user_chat_id,
        basic_user_info=basic_user_info,
        message_id=BotApiMessageId(message_reaction.message_id),
        tg_timestamp=BotApiUTCTimestamp(
            message_reaction.date.astimezone(timezone.utc).timestamp()
        ),
        chat_info=BotApiChatInfo.from_bot_api_chat(message_reaction.chat),
        old_reaction=[
            BotApiReactionEmoji.from_bot_api_reaction(reaction)
            for reaction in message_reaction.old_reaction
        ],
        new_reaction=[
            BotApiReactionEmoji.from_bot_api_reaction(reaction)
            for reaction in message_reaction.new_reaction
        ],
    )


def extract_bot_events(
    update: BotApiUpdate, recv_timestamp: LocalUTCTimestamp
) -> Generator[BaseEvent, None, None]:
    if update.message is not None or update.edited_message is not None:
        if update.message is not None:
            message = update.message
            is_edited = False
        elif update.edited_message is not None:
            message = update.edited_message
            is_edited = True
        else:
            raise RuntimeError("BUG: message is None")
        yield from extract_bot_events_from_message(message, recv_timestamp, is_edited)
    elif update.message_reaction is not None:
        yield from extract_bot_events_from_message_reaction(
            update.message_reaction, recv_timestamp
        )


async def bot_api_main(
    bot: Bot, event_queue: BaseEventQueue, event_log: EventLog
) -> None:
    try:
        dp = Dispatcher()

        async def common_update_handler(update: BotApiUpdate) -> None:
            recv_timestamp = LocalUTCTimestamp(time.time())
            event_log.log_bot_api_update(recv_timestamp, update)
            try:
                bot_events = list(extract_bot_events(update, recv_timestamp))
                for event in bot_events:
                    event_log.log_base_event(recv_timestamp, event)
                await event_queue.put_events(bot_events)
            except Exception:
                logging.error("Failed to process Bot API update: %s", update)
                raise

        @dp.message()
        async def message_handler(message: aiogram.types.Message) -> None:
            await common_update_handler(BotApiUpdate(message=message))

        @dp.edited_message()
        async def edited_message_handler(message: aiogram.types.Message) -> None:
            await common_update_handler(BotApiUpdate(edited_message=message))

        @dp.message_reaction()
        async def message_reaction_handler(
            message_reaction: aiogram.types.MessageReactionUpdated,
        ) -> None:
            await common_update_handler(BotApiUpdate(message_reaction=message_reaction))

        try:
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            logging.info("bot_api_main cancelled")
    except:
        logging.error("Error in bot_api_main", exc_info=True)
        raise
