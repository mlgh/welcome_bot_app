import asyncio
import logging
import time
import re
import html
from typing import Mapping, Optional
import aiogram
import telethon
import aiogram.utils.formatting
from datetime import timedelta
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

from aiogram.enums.parse_mode import ParseMode


class safe_html_str(str):
    """A string that's safe to render in html."""

    pass


PLEASE_INTRODUCE_HTML = safe_html_str("""Добрый день, $USER
Пожалуйста, представьтесь 😊:

как зовут <b>по имени</b>, где и чему учитесь (или кем работаете).
! Обязательно добавьте #ichbin в сообщение.

Лучше представиться прямо сейчас, чтобы не забыть, а то бот удалит через трое суток😈

По желанию добавьте: какие у Вас интересы/хобби, откуда Вы, как узнали о группе, собираетесь ли прийти на наши встречи.""")

STILL_PLEASE_INTRODUCE_AFTER_REJOINING_HTML = safe_html_str("""Добрый день, $USER, Вы уже были у нас в чате, но не представились.

У вас осталось $HOURS_LEFT часов, чтобы это сделать. Пожалуйста, представьтесь и не забудьте указать тег #ichbin 😊""")

WELCOME_HTML = safe_html_str("""Добро пожаловать, $USER!
Обратите внимание на <a href="https://docs.google.com/document/d/1XywThEaZI6u6tjtN0RUo9ChO9BGvumzz9gEaxBQ1Xis/edit?usp=sharing">правила и информацию о группе</a>

Также полезное:
<a href="https://ru-ch.github.io/faq/">Гайд</a> с информацией о жизни в Швейцарии, отдельная <a href="https://ru-ch.github.io/faq/inbox/%D0%A1%D1%82%D1%83%D0%B4%D0%B5%D0%BD%D1%82%D0%B0%D0%BC-%D0%B8-%D0%BF%D0%BE%D1%81%D1%82%D1%83%D0%BF%D0%B0%D1%8E%D1%89%D0%B8%D0%BC.html">секция</a> для студентов/молодёжи
<a href="https://t.me/chEVENTru">Инфоканал</a> русскоязычных мероприятий
<a href="https://t.me/+ZGvPekUVQOg1N2Ey">Группа</a> "Что, где, когда" в Цюрихе
Интернациональный <a href="https://chat.whatsapp.com/KKDNO75dnNh0rbTm8sfexo">чатик любителей музыки</a>""")

USER_IS_KICKED_HTML = safe_html_str("""$USER молчит и покидает чат.""")

ICHBIN_WAITING_TIMEDELTA = timedelta(days=3)


def escape_html(s: str) -> safe_html_str:
    return safe_html_str(html.escape(s))


def safe_html_format(
    s: safe_html_str, dct: Mapping[str, safe_html_str]
) -> safe_html_str:
    if not isinstance(s, safe_html_str):
        raise ValueError(f"Value {s} is not safe.")
    for k, v in dct.items():
        if not isinstance(v, safe_html_str):
            raise ValueError(f"Value {v} for key {k} is not safe.")
    return safe_html_str(s.format(**dct))


def _create_user_mention_html(
    user_id: int, first_name: Optional[str], last_name: Optional[str]
) -> safe_html_str:
    if first_name is None:
        name = "{user_id:%s}" % user_id
    elif last_name is not None and last_name != "":
        name = f"{first_name} {last_name}"
    else:
        name = first_name
    return safe_html_format(
        safe_html_str('<a href="tg://user?id={user_id}">{name}</a>'),
        {"user_id": escape_html(str(user_id)), "name": escape_html(name)},
    )


def _create_message_html(
    text: safe_html_str, substitutions: Mapping[str, safe_html_str]
) -> safe_html_str:
    parts = re.split(r"(\$[A-Z_]+)", text)
    body: list[safe_html_str] = []
    for part in parts:
        if part.startswith("$") and part[1:] in substitutions:
            body.append(substitutions[part[1:]])
        else:
            body.append(safe_html_str(part))
    return safe_html_str("".join(body))


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
            # TODO: Override SIGINT/SIGTERM - we should stop gracefully.
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
            # This requires changing DB schema.
            will_be_kicked_at_timestamp = (
                user_profile.ichbin_request_timestamp
                + ICHBIN_WAITING_TIMEDELTA.total_seconds()
            )
            welcome_again_message_html = _create_message_html(
                STILL_PLEASE_INTRODUCE_AFTER_REJOINING_HTML,
                {
                    "USER": _create_user_mention_html(
                        event.user_key.user_id,
                        first_name=event.user_info.first_name,
                        last_name=event.user_info.last_name,
                    ),
                    "HOURS_LEFT": safe_html_str(
                        int(
                            (will_be_kicked_at_timestamp - event.local_timestamp) / 3600
                        )
                    ),
                },
            )
            await self._bot.send_message(
                event.user_key.chat_id,
                welcome_again_message_html,
                parse_mode=ParseMode.HTML,
            )
            return
        user_profile.first_name_when_joining = event.user_info.first_name
        user_profile.last_name_when_joining = event.user_info.last_name
        please_introduce_content_md = _create_message_html(
            PLEASE_INTRODUCE_HTML,
            {
                "USER": _create_user_mention_html(
                    event.user_key.user_id,
                    first_name=event.user_info.first_name,
                    last_name=event.user_info.last_name,
                )
            },
        )
        sent_msg = await self._bot.send_message(
            event.user_key.chat_id,
            text=please_introduce_content_md,
            parse_mode=ParseMode.HTML,
        )
        user_profile.ichbin_message_id = sent_msg.message_id
        user_profile.ichbin_request_timestamp = event.tg_timestamp
        logging.info(
            "Updating #ichbin request timestamp of user %s to %s, and ichbin_message_id to %s",
            event.user_key,
            user_profile.ichbin_request_timestamp,
            user_profile.ichbin_message_id,
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
        event.user_key.user_id = 2070387399  # DO_NOT_SUBMIT
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

        welcome_html = _create_message_html(
            WELCOME_HTML,
            {
                "USER": _create_user_mention_html(
                    event.user_key.user_id,
                    first_name=event.user_info.first_name,
                    last_name=event.user_info.last_name,
                )
            },
        )

        await self._bot.send_message(
            event.user_key.chat_id,
            text=welcome_html,
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
        # TODO: Warning, here we compare tg_timestamp with local_timestamp!
        max_ichbin_request_timestamp = (
            event.local_timestamp - ICHBIN_WAITING_TIMEDELTA.total_seconds()
        )
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
        # TODO: Adjust timedelta, telegram docs say that if it's less than 30sec, or longer than some number, it will be a perma-ban.
        await self._bot.ban_chat_member(
            chat_id=user_profile.user_key.chat_id,
            user_id=user_profile.user_key.user_id,
            until_date=timedelta(minutes=3),
        )
        # TODO: Test how the ban above works.
        # TODO: Find out when we should unban.
        # # Unban immediately, so that they could join again.
        # await self._bot.unban_chat_member(user_profile.chat_id, user_profile.user_id, only_if_banned=True)
        logging.info("Kicked user %s, saving result into the database.", user_key)
        user_profile.local_kicked_timestamp = local_timestamp
        self._user_storage.save_profile(user_profile)
        logging.info(
            "Saved information about the kick of user %s into the database.", user_key
        )
        kick_message_html = _create_message_html(
            USER_IS_KICKED_HTML,
            {
                "USER_": _create_user_mention_html(
                    user_key.user_id,
                    first_name=user_profile.first_name_when_joining,
                    last_name=user_profile.last_name_when_joining,
                )
            },
        )
        await self._bot.send_message(
            user_key.chat_id,
            text=kick_message_html,
            parse_mode=ParseMode.HTML,
        )
