import asyncio
import logging
import time
from collections import defaultdict
import re
import html
from typing import Iterable, Iterator, List, Mapping, Optional
import aiogram
from pydantic import BaseModel
import telethon
import contextlib
import aiogram.utils.formatting
from datetime import timedelta
from welcome_bot_app.model import (
    BotApiMessageId,
    ChatId,
    LocalUTCTimestamp,
    UserChatId,
    UserId,
)
from welcome_bot_app.model.user_profile import (
    BotApiMessage,
    BotApiMessageType,
    UserProfile,
    UserProfileParams,
)
from welcome_bot_app.user_storage import SqliteUserStorage
from welcome_bot_app.event_queue import BaseEventQueue
from welcome_bot_app.model.events import (
    BaseEvent,
    BotApiNewTextMessage,
    BotApiChatMemberJoined,
    BotApiChatMemberLeft,
    PeriodicEvent,
    StopEvent,
)

from aiogram.enums.parse_mode import ParseMode


class safe_html_str(str):
    """A string that's safe to render in html."""

    pass


ICHBIN_REQUEST_HTML = safe_html_str("""Добрый день, $USER
Пожалуйста, представьтесь 😊:

как зовут <b>по имени</b>, где и чему учитесь (или кем работаете).
! Обязательно добавьте #ichbin в сообщение.

Лучше представиться прямо сейчас, чтобы не забыть, а то бот удалит через трое суток😈

По желанию добавьте: какие у Вас интересы/хобби, откуда Вы, как узнали о группе, собираетесь ли прийти на наши встречи.""")

NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN_HTML = safe_html_str(
    """Добрый день, $USER, пожалуйста, представьтесь с тегом #ichbin как можно скорее, иначе бот удалит."""
)

STILL_PLEASE_INTRODUCE_AFTER_REJOINING_HTML = safe_html_str("""Добрый день, $USER, Вы уже были у нас в чате, но не представились.

У вас осталось $HOURS_LEFT часов, чтобы это сделать. Пожалуйста, представьтесь и не забудьте указать тег #ichbin 😊""")

WELCOME_HTML = safe_html_str("""Добро пожаловать, $USER!
Обратите внимание на <a href="https://docs.google.com/document/d/1XywThEaZI6u6tjtN0RUo9ChO9BGvumzz9gEaxBQ1Xis/edit?usp=sharing">правила и информацию о группе</a>

Также полезное:
<a href="https://ru-ch.github.io/faq/">Гайд</a> с информацией о жизни в Швейцарии, отдельная <a href="https://ru-ch.github.io/faq/inbox/%D0%A1%D1%82%D1%83%D0%B4%D0%B5%D0%BD%D1%82%D0%B0%D0%BC-%D0%B8-%D0%BF%D0%BE%D1%81%D1%82%D1%83%D0%BF%D0%B0%D1%8E%D1%89%D0%B8%D0%BC.html">секция</a> для студентов/молодёжи
<a href="https://t.me/chEVENTru">Инфоканал</a> русскоязычных мероприятий
<a href="https://t.me/+ZGvPekUVQOg1N2Ey">Группа</a> "Что, где, когда" в Цюрихе
Интернациональный <a href="https://chat.whatsapp.com/KKDNO75dnNh0rbTm8sfexo">чатик любителей музыки</a>""")

WELCOME_AGAIN_HTML = safe_html_str("""Добро пожаловать снова, $USER!""")

USER_IS_KICKED_HTML = safe_html_str("""$USER молчит и покидает чат.""")


def create_message_html(
    message: safe_html_str, user_profile: UserProfile
) -> safe_html_str:
    return _substitute_html(
        message,
        {
            "USER": _create_user_mention_html(
                user_profile.user_chat_id.user_id,
                first_name=user_profile.first_name(),
                last_name=user_profile.last_name(),
            )
        },
    )


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
    user_id: UserId, first_name: Optional[str], last_name: Optional[str]
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


def _substitute_html(
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


@contextlib.contextmanager
def open_user_profile(
    user_chat_id: UserChatId,
    user_storage: SqliteUserStorage,
    user_profile_params: UserProfileParams,
) -> Iterator[UserProfile]:
    user_profile = user_storage.get_profile(user_chat_id)
    original_json = user_profile.model_dump_json()
    yield user_profile
    # TODO: Make sure that model_dump_json is deterministic.
    modified_json = user_profile.model_dump_json()
    if original_json == modified_json:
        return
    logging.info("Saving profile of user %r", user_chat_id)
    user_storage.save_profile(user_profile, user_profile_params)


class EventProcessor:
    class Config(BaseModel):
        # For how long to ban the user if he didn't write ichbin.
        ban_duration: timedelta = timedelta(minutes=1)
        # How long to wait for the user to write ichbin.
        ichbin_waiting_time: timedelta = timedelta(days=3)
        # If the user joined the chat 1 month ago, then rejoined it today, we should not kick him, instead we should give him some grace time to write the ichbin message.
        extra_ichbin_waiting_time_after_rejoining: timedelta = timedelta(hours=1)
        # How often should we check for periodic stuff, like users to kick,
        periodic_event_interval: timedelta = timedelta(seconds=3)
        dark_launch_sink_chat_id: Optional[ChatId] = None
        admin_user_id: UserId = UserId(6776217955)

    def __init__(
        self,
        config: Config,
        bot: aiogram.Bot,
        telethon_client: Optional[telethon.TelegramClient],
        event_queue: BaseEventQueue,
        user_storage: SqliteUserStorage,
    ):
        self._config = config

        # TODO: Add ability to configure this via Telegram.
        self._config.dark_launch_sink_chat_id = ChatId(-1002052048428)

        self._bot = bot
        self._telethon_client = telethon_client
        self._event_queue = event_queue
        self._user_storage = user_storage
        self._user_profile_params = UserProfileParams(
            ichbin_waiting_time=self._config.ichbin_waiting_time
        )
        self._last_periodic_event_timestamp = LocalUTCTimestamp(0.0)
        self._stopped = False

    @contextlib.contextmanager
    def _open_user_profile(self, user_chat_id: UserChatId) -> Iterator[UserProfile]:
        with open_user_profile(
            user_chat_id, self._user_storage, self._user_profile_params
        ) as user_profile:
            yield user_profile

    async def stop(self) -> None:
        logging.info("Putting stop event")
        await self._event_queue.put_events(
            [StopEvent(recv_timestamp=LocalUTCTimestamp(time.time()))]
        )

    async def run(self) -> None:
        while not self._stopped:
            event: BaseEvent | None = None
            try:
                if (
                    self._last_periodic_event_timestamp
                    + self._config.periodic_event_interval.total_seconds()
                    < time.time()
                ):
                    self._last_periodic_event_timestamp = LocalUTCTimestamp(time.time())
                    event = PeriodicEvent(recv_timestamp=LocalUTCTimestamp(time.time()))
                    await self._handle_event(event)
                else:
                    async with self._event_queue.get_event_for_processing(
                        timeout=1.0
                    ) as event:
                        if event is None:
                            continue
                        await self._handle_event(event)
            except asyncio.CancelledError:
                logging.info("EventProcessor cancelled")
                break
            except Exception:
                if event is None:
                    logging.error(
                        "Error in EventProcessor (event is None)", exc_info=True
                    )
                else:
                    logging.error(
                        "Error in EventProcessor while processing event: %s",
                        event,
                        exc_info=True,
                    )
        logging.info("Exiting the EventProcessor.run() loop")

    async def _handle_event(self, event: BaseEvent) -> None:
        if isinstance(event, BotApiNewTextMessage):
            await self._on_bot_api_new_text_message(event)
        elif isinstance(event, BotApiChatMemberJoined):
            await self._on_bot_api_new_chat_member(event)
        elif isinstance(event, BotApiChatMemberLeft):
            await self._on_bot_api_chat_member_left(event)
        elif isinstance(event, PeriodicEvent):
            await self._on_periodic_event(event)
        elif isinstance(event, StopEvent):
            logging.info("Handled stop event.")
            self._stopped = True
        else:
            logging.critical("BUG: Unknown event: %s, skipping.", event)

    async def _on_admin_message(self, event: BotApiNewTextMessage) -> None:
        text = event.text
        command, _, rest = text.partition(" ")
        try:
            if command == "/message":
                destination_chat_id_str, _, message = rest.partition(" ")
                destination_chat_id = ChatId(int(destination_chat_id_str))
                await self._bot.send_message(chat_id=destination_chat_id, text=message)
            else:
                raise ValueError(f"Unknown command: {command}")
        except Exception:
            logging.error("Failed to execute admin command: %s", text, exc_info=True)
            response_text = "Failed to execute command. Traceback is in the logs."
            await self._bot.send_message(
                chat_id=event.user_chat_id.chat_id,
                text=response_text,
                reply_parameters=aiogram.types.ReplyParameters(
                    message_id=event.message_id, chat_id=event.user_chat_id.chat_id
                ),
            )
        else:
            response_text = "Command executed successfully."
            await self._bot.send_message(
                chat_id=event.user_chat_id.chat_id,
                text=response_text,
                reply_parameters=aiogram.types.ReplyParameters(
                    message_id=event.message_id, chat_id=event.user_chat_id.chat_id
                ),
            )

    async def _on_bot_api_new_text_message(self, event: BotApiNewTextMessage) -> None:
        if event.user_chat_id.user_id == self._config.admin_user_id:
            logging.info("Got a message from admin: %r", event)
            await self._on_admin_message(event)
            return
        with self._open_user_profile(event.user_chat_id) as user_profile:
            user_profile.basic_user_info = event.basic_user_info
            if "#ichbin" not in event.text:
                return
            if not user_profile.is_waiting_for_ichbin_message():
                return
            user_profile.ichbin_message_timestamp = event.recv_timestamp
            await self._send_message(
                user_profile, WELCOME_HTML, BotApiMessageType.WELCOME
            )

    async def _on_bot_api_new_chat_member(self, event: BotApiChatMemberJoined) -> None:
        with self._open_user_profile(event.user_chat_id) as user_profile:
            user_profile.basic_user_info = event.basic_user_info
            user_profile.on_joined(event.recv_timestamp)
            if user_profile.basic_user_info.is_bot:
                logging.info("Ignoring bot %r", user_profile.user_chat_id)
                return
            if user_profile.ichbin_message_timestamp is not None:
                await self._send_message(
                    user_profile, WELCOME_AGAIN_HTML, BotApiMessageType.WELCOME_AGAIN
                )
                return
            if user_profile.ichbin_request_timestamp is None:
                bot_message = await self._send_message(
                    user_profile, ICHBIN_REQUEST_HTML, BotApiMessageType.ICHBIN_REQUEST
                )
                user_profile.ichbin_request_timestamp = bot_message.sent_timestamp
                return
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                self._user_profile_params
            )
            if kick_at_timestamp is None:
                logging.warning(
                    "BUG: kick_at_timestamp is None at current point : %r", user_profile
                )
                return
            time_left = kick_at_timestamp - event.recv_timestamp
            if (
                time_left
                > self._config.extra_ichbin_waiting_time_after_rejoining.total_seconds()
            ):
                # No need yet to warn the user that he will be kicked soon.
                return
            user_profile.add_extra_grace_time(
                self._config.extra_ichbin_waiting_time_after_rejoining.total_seconds()
                - time_left
            )
            await self._send_message(
                user_profile,
                NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN_HTML,
                BotApiMessageType.NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN,
            )

    async def _send_message(
        self,
        user_profile: UserProfile,
        message_template: safe_html_str,
        message_type: BotApiMessageType,
    ) -> BotApiMessage:
        if not self._is_dark_launch(user_profile.user_chat_id.chat_id):
            sent_message = await self._bot.send_message(
                user_profile.user_chat_id.chat_id,
                create_message_html(message_template, user_profile),
                parse_mode=ParseMode.HTML,
            )
        else:
            assert self._config.dark_launch_sink_chat_id is not None
            logging.info(
                "Redirecting message to dark launch chat %r",
                self._config.dark_launch_sink_chat_id,
            )
            sent_message = await self._bot.send_message(
                self._config.dark_launch_sink_chat_id,
                create_message_html(message_template, user_profile),
                parse_mode=ParseMode.HTML,
            )
        sent_timestamp = LocalUTCTimestamp(time.time())
        bot_api_message = BotApiMessage(
            user_chat_id=user_profile.user_chat_id,
            message_id=BotApiMessageId(sent_message.message_id),
            message_type=message_type,
            sent_timestamp=sent_timestamp,
        )
        self._user_storage.add_bot_message(bot_api_message)
        return bot_api_message

    async def _on_bot_api_chat_member_left(self, event: BotApiChatMemberLeft) -> None:
        # Nothing need to be done here.
        with self._open_user_profile(event.user_chat_id) as user_profile:
            user_profile.on_left(left_timestamp=event.recv_timestamp)

    async def _on_periodic_event(self, event: PeriodicEvent) -> None:
        users_to_kick = self._user_storage.get_users_to_kick(event.recv_timestamp)
        for user_chat_id in users_to_kick:
            try:
                await self._verify_and_kick_user(user_chat_id, event.recv_timestamp)
            except Exception:
                logging.error(
                    "Error while kicking user %r", user_chat_id, exc_info=True
                )
                continue
        welcome_messages_per_chat = defaultdict(list)
        messages_per_user = defaultdict(list)
        for bot_api_message in self._user_storage.get_bot_messages():
            if bot_api_message.message_type == BotApiMessageType.WELCOME:
                welcome_messages_per_chat[bot_api_message.user_chat_id.chat_id].append(
                    bot_api_message
                )
            messages_per_user[bot_api_message.user_chat_id].append(bot_api_message)
        for chat_id, welcome_messages in welcome_messages_per_chat.items():
            welcome_messages = await self._delete_all_but_last_message(
                welcome_messages, event.recv_timestamp, delete_welcome_messages=True
            )
            await self._delete_expired_messages(
                welcome_messages, event.recv_timestamp, delete_welcome_messages=True
            )
        for user, messages in messages_per_user.items():
            messages = await self._delete_all_but_last_message(
                messages, event.recv_timestamp, delete_welcome_messages=False
            )
            await self._delete_expired_messages(
                messages, event.recv_timestamp, delete_welcome_messages=False
            )

    async def _delete_all_but_last_message(
        self,
        messages: Iterable[BotApiMessage],
        current_timestamp: LocalUTCTimestamp,
        delete_welcome_messages: bool,
    ) -> List[BotApiMessage]:
        messages = list(messages)
        messages.sort(key=lambda msg: msg.sent_timestamp)
        for msg in messages[:-1]:
            # Even if we don't delete welcome messages, we still make them take space in the list, so that earlier messages would get removed.
            if (
                msg.message_type == BotApiMessageType.WELCOME
                and not delete_welcome_messages
            ):
                continue
            await self._delete_message(msg, current_timestamp)
        return messages[-1:]

    async def _delete_expired_messages(
        self,
        messages: Iterable[BotApiMessage],
        current_timestamp: LocalUTCTimestamp,
        delete_welcome_messages: bool,
    ) -> None:
        # TODO: Move this to some kind of config.
        msg_type_to_ttl: dict[BotApiMessageType, timedelta] = {
            BotApiMessageType.WELCOME: timedelta(days=3),
            BotApiMessageType.WELCOME_AGAIN: timedelta(minutes=5),
            BotApiMessageType.ICHBIN_REQUEST: timedelta(days=3),
            BotApiMessageType.NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN: timedelta(days=3),
            BotApiMessageType.USER_IS_KICKED: timedelta(hours=1),
        }
        for msg in messages:
            if (
                msg.message_type == BotApiMessageType.WELCOME
                and not delete_welcome_messages
            ):
                continue
            ttl = msg_type_to_ttl.get(msg.message_type, timedelta(hours=1))
            if current_timestamp > msg.sent_timestamp + ttl.total_seconds():
                await self._delete_message(msg, current_timestamp)

    def _is_dark_launch(self, chat_id: ChatId) -> bool:
        return (
            self._config.dark_launch_sink_chat_id is not None
            and self._config.dark_launch_sink_chat_id != chat_id
        )

    async def _delete_message(
        self, message: BotApiMessage, current_timestamp: LocalUTCTimestamp
    ) -> None:
        try:
            if not self._is_dark_launch(message.user_chat_id.chat_id):
                logging.info("Trying to delete message %r", message)
                await self._bot.delete_message(
                    chat_id=message.user_chat_id.chat_id, message_id=message.message_id
                )
            else:
                assert self._config.dark_launch_sink_chat_id is not None
                logging.info(
                    "Deleting message %r in dark launch chat %r",
                    message,
                    self._config.dark_launch_sink_chat_id,
                )
                await self._bot.delete_message(
                    chat_id=self._config.dark_launch_sink_chat_id,
                    message_id=message.message_id,
                )
            self._user_storage.mark_bot_message_as_deleted(
                message.user_chat_id,
                message.message_id,
                delete_timestamp=current_timestamp,
            )
        except Exception:
            logging.error("Failed to delete message %r", message, exc_info=True)

    async def _verify_and_kick_user(
        self,
        user_chat_id: UserChatId,
        current_timestamp: LocalUTCTimestamp,
    ) -> None:
        logging.info("Attempting to kick user %r", user_chat_id)
        with self._open_user_profile(user_chat_id) as user_profile:
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                self._user_profile_params
            )
            if kick_at_timestamp is None:
                logging.warning(
                    "User %r kick_at_timestamp is None, skipping",
                    user_chat_id,
                )
                return
            if kick_at_timestamp > current_timestamp:
                logging.warning(
                    "User %r is still in grace period, skipping: %s > %s",
                    user_chat_id,
                    kick_at_timestamp,
                    current_timestamp,
                )
                return
            if not self._is_dark_launch(user_chat_id.chat_id):
                logging.info("Kicking user %r", user_chat_id)
                await self._bot.ban_chat_member(
                    chat_id=user_chat_id.chat_id,
                    user_id=user_chat_id.user_id,
                    until_date=self._config.ban_duration,
                )
            else:
                logging.info(
                    "Would have kicked user %r, but dark launch is enabled.",
                    user_chat_id,
                )
            user_profile.on_kicked(kick_timestamp=LocalUTCTimestamp(time.time()))
            try:
                await self._send_message(
                    user_profile, USER_IS_KICKED_HTML, BotApiMessageType.USER_IS_KICKED
                )
            except Exception:
                logging.error(
                    "Failed to report that the user %r was kicked.",
                    user_chat_id,
                    exc_info=True,
                )
