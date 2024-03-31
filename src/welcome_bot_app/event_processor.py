import asyncio
import logging
import time
import re
from typing import Mapping
import aiogram
import telethon
import aiogram.utils.formatting
from welcome_bot_app.event_storage import SqliteEventStorage
from welcome_bot_app.model import (
    Event,
    PeriodicEvent,
    BotApiNewChatMember,
    BotApiChatMemberLeft,
    BotApiNewTextMessage,
    StopEvent,
    UserKey,
)
from welcome_bot_app.user_storage import SqliteUserStorage

from aiogram.utils.formatting import Text, TextMention
from aiogram.enums.parse_mode import ParseMode

PLEASE_INTRODUCE_TEXT = """Добрый день, @USER
Пожалуйста, представьтесь 😊:
как зовут по имени, где и чему учитесь (или кем работаете).
! Обязательно добавьте #ichbin в сообщение.

Лучше представиться прямо сейчас, чтобы не забыть, а то бот удалит через трое суток😈

По желанию добавьте: какие у Вас интересы/хобби, откуда Вы, как узнали о группе, собираетесь ли прийти на наши встречи."""

WELCOME_TEXT = """Добро пожаловать, @USER
Типа у нас ещё есть всякие чаты."""

USER_IS_KICKED_TEXT = """@USER молчит и покидает чат."""


def _create_user_mention(user_id: int, name: str) -> TextMention:
    return TextMention(
        name,
        user=aiogram.types.User(
            id=user_id,
            is_bot=False,
            first_name="",
        ),
    )


def _create_message_text(text: str, substitutions: Mapping[str, TextMention]) -> Text:
    parts = re.split(r"(\@[A-Z]+)", text)
    body: list[str | TextMention] = []
    for part in parts:
        if part.startswith("@") and part[1:] in substitutions:
            body.append(substitutions[part[1:]])
        else:
            body.append(part)
    return Text(*body)


class EventProcessor:
    def __init__(
        self,
        bot: aiogram.Bot,
        telethon_client: telethon.TelegramClient,
        event_storage: SqliteEventStorage,
        user_storage: SqliteUserStorage,
    ):
        self._bot = bot
        self._telethon_client = telethon_client
        self._event_storage = event_storage
        self._user_storage = user_storage

        self._event_queue: asyncio.Queue[Event] = asyncio.Queue()

    async def periodic_event_generator(self, period: float) -> None:
        """Helper function for periodic event generation."""
        while True:
            logging.info("Periodic task")
            local_timestamp = time.time()
            try:
                await self.put_event(PeriodicEvent(local_timestamp=local_timestamp))
            except asyncio.CancelledError:
                logging.info("periodic cancelled")
                break
            # Sleep until (current_time + period)
            await asyncio.sleep(local_timestamp + period - time.time())

    async def put_event(self, event: Event) -> None:
        await self._event_queue.put(event)

    async def run(self) -> None:
        while True:
            try:
                event: Event = await self._event_queue.get()
            except asyncio.CancelledError:
                logging.info("EventProcessor cancelled")
                break
            except Exception:
                logging.error(
                    "Error in EventProcessor while getting event", exc_info=True
                )
                continue
            if not isinstance(event, Event):
                logging.critical(
                    "BUG: Got event that's not inherited from Event class: %r, skipping",
                    event,
                )
                continue
            if isinstance(event, StopEvent):
                logging.info("Got StopEvent. Stopping.")
                break

            if not isinstance(event, PeriodicEvent):
                self._event_storage.log_event(event)
            try:
                await self._handle_event(event)
            except asyncio.CancelledError:
                logging.info("EventProcessor cancelled")
                break
            except Exception:
                logging.critical(
                    "Error in EventProcessor while processing event: %s",
                    event,
                    exc_info=True,
                )
                continue

    async def _handle_event(self, event: Event) -> None:
        if isinstance(event, BotApiNewTextMessage):
            await self._on_bot_api_new_text_message(event)
        elif isinstance(event, BotApiNewChatMember):
            await self._on_bot_api_new_chat_member(event)
        elif isinstance(event, BotApiChatMemberLeft):
            await self._on_bot_api_chat_member_left(event)
        elif isinstance(event, PeriodicEvent):
            await self._on_periodic(event)
        else:
            logging.critical("BUG: Unknown event: %s, skipping.", event)

    # TODO: Actually contains business logic.
    async def _on_bot_api_new_chat_member(self, event: BotApiNewChatMember) -> None:
        logging.info("New chat member: %s", event)
        user_profile = self._user_storage.get_profile(event.user_key)
        if user_profile.ichbin_message_timestamp is not None:
            logging.info(
                "User %s already has an existing #ichbin message", event.user_key
            )
            return
        if user_profile.ichbin_request_timestamp is not None:
            logging.info(
                "User %s already has an existing #ichbin request timestamp.",
                event.user_key,
            )
            # TODO: If you have like 10 minutes left, bump the timeout to 2 hours, so that people can have time to write about them.
            await self._bot.send_message(
                event.user_key.chat_id,
                "Welcome, user! You still have to write the #ichbin, or I'll kick you.",
            )
            return
        user_profile.first_name_when_joining = event.user_info.first_name
        user_profile.last_name_when_joining = event.user_info.last_name
        # XXX: We first write the message, then try to save to the database.
        await self._bot.send_message(
            event.user_key.chat_id,
            "Welcome, user! Write #ichbin during the next X days, or I'll kick you out.",
        )
        user_profile.ichbin_request_timestamp = event.tg_timestamp
        logging.info(
            "Updating #ichbin request timestamp of user %s to %s",
            event.user_key,
            user_profile.ichbin_request_timestamp,
        )
        self._user_storage.save_profile(user_profile)
        logging.info(
            "Updated #ichbin request timestamp of user %s to %s",
            event.user_key,
            user_profile.ichbin_request_timestamp,
        )

    async def _on_bot_api_chat_member_left(self, event: BotApiChatMemberLeft) -> None:
        logging.info("Chat member left: %s", event.user_key)

    async def _on_bot_api_new_text_message(self, event: BotApiNewTextMessage) -> None:
        if "#ichbin" not in event.text:
            return
        logging.info("User %s wrote #ichbin.", event.user_key)
        user_profile = self._user_storage.get_profile(event.user_key)
        if user_profile.ichbin_message is not None:
            logging.info(
                "User %s already has an existing #ichbin message.", event.user_key
            )
            return
        user_profile.ichbin_message = event.text
        user_profile.ichbin_message_timestamp = event.tg_timestamp
        logging.info(
            "Updating #ichbin message of user %s to %s", event.user_key, event.text
        )
        self._user_storage.save_profile(user_profile)
        logging.info(
            "Successfully updated #ichbin message of user %s to %s",
            event.user_key,
            event.text,
        )

        logging.info(
            "Sending welcome message to user %s as a reply to message %s",
            event.user_key,
            event.message_id,
        )
        # TODO: Make sure that we keep only a single ichbin-welcoming message?

        # await self._bot.send_message(event.user_key.chat_id, f"Welcome, user! You are now a member of the chat.", reply_to_message_id = event.message_id)
        content = _create_message_text(
            WELCOME_TEXT,
            {
                "USER": _create_user_mention(
                    event.user_key.user_id, event.user_info.first_name
                )
            },
        )

        await self._bot.send_message(
            event.user_key.chat_id,
            text=content.as_html(),
            parse_mode=ParseMode.HTML,
            reply_to_message_id=event.message_id,
        )

        logging.info("Sent welcome message to user %s", event.user_key)

        if user_profile.ichbin_message_id is not None:
            # TODO: Can use telethon to remove messages older than 48 hours?
            try:
                await self._bot.delete_message(
                    chat_id=user_profile.user_key.chat_id,
                    message_id=user_profile.ichbin_message_id,
                )
            except Exception:
                logging.error(
                    "Failed to delete message %s by user %s",
                    user_profile.ichbin_message_id,
                    user_profile.user_key,
                    exc_info=True,
                )

    async def _on_periodic(self, event: PeriodicEvent) -> None:
        # TODO: Magic number.
        # TODO: Warning, here we compare tg_timestamp with local_timestamp!
        max_ichbin_request_timestamp = event.local_timestamp - 5.0
        users_to_kick = self._user_storage.get_users_to_kick(
            max_ichbin_request_timestamp
        )
        for user_key in users_to_kick:
            try:
                await self._verify_and_kick_user(
                    user_key,
                    local_timestamp=event.local_timestamp,
                    max_ichbin_request_timestamp=max_ichbin_request_timestamp,
                )
            except Exception:
                logging.error("Error while kicking user %s", user_key, exc_info=True)
                continue

    async def _verify_and_kick_user(
        self,
        user_key: UserKey,
        local_timestamp: float,
        max_ichbin_request_timestamp: float,
    ) -> None:
        logging.info("Attempting to kick user %s", user_key)
        user_profile = self._user_storage.get_profile(user_key)
        if user_profile.ichbin_message_timestamp is not None:
            logging.warning(
                "User %s wrote #ichbin at %s, skipping",
                user_profile.ichbin_message_timestamp,
            )
            return
        if user_profile.ichbin_request_timestamp is None:
            logging.warning(
                "User %s didn't get a request to write #ichbin, skipping",
                user_profile.user_key,
            )
            return
        if user_profile.local_kicked_timestamp is not None:
            logging.warning(
                "User %s was already kicked at %s, skipping",
                user_profile.user_key,
                user_profile.local_kicked_timestamp,
            )
            return
        if user_profile.ichbin_request_timestamp > max_ichbin_request_timestamp:
            logging.warning(
                "User %s is still in grace period, skipping: %s > %s",
                user_profile.user_key,
                user_profile.ichbin_request_timestamp,
                max_ichbin_request_timestamp,
            )
            return
        logging.info("Kicking user %s as they didn't write #ichbin in time.", user_key)
        # TODO: Adjust timedelta.
        # DO_NOT_SUBMIT
        # await self._bot.ban_chat_member(
        #     chat_id=user_profile.user_key.chat_id,
        #     user_id=user_profile.user_key.user_id,
        #     until_date=datetime.timedelta(minutes=3),
        # )
        # TODO: Test how the ban above works.
        # TODO: Find out when we should unban.
        # # Unban immediately, so that they could join again.
        # await self._bot.unban_chat_member(user_profile.chat_id, user_profile.user_id, only_if_banned=True)
        logging.info("Kicked user %s, saving result into the database.")
        user_profile.local_kicked_timestamp = local_timestamp
        self._user_storage.save_profile(user_profile)
        logging.info(
            "Saved information about the kick of user %s into the database.", user_key
        )
        first_name = user_profile.first_name_when_joining
        if first_name is None:
            first_name = "user_id:%s" % user_key.user_id
        content = _create_message_text(
            USER_IS_KICKED_TEXT,
            {"USER": _create_user_mention(user_key.user_id, first_name)},
        )
        await self._bot.send_message(
            user_key.chat_id,
            text=content.as_html(),
            parse_mode=ParseMode.HTML,
        )
