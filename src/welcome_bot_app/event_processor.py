import asyncio
import logging
import time
from collections import defaultdict
from typing import Iterable, Iterator, List, Optional
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
from welcome_bot_app.model.chat_settings import ChatSettings
from welcome_bot_app.model.user_profile import (
    BotApiMessage,
    BotApiMessageType,
    UserProfile,
    UserProfileParams,
)
from welcome_bot_app.safe_html import (
    escape_html,
    safe_html_format,
    safe_html_str,
    substitute_html,
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


def create_message_html(
    message: safe_html_str, user_profile: UserProfile
) -> safe_html_str:
    return substitute_html(
        message,
        {
            "USER": _create_user_mention_html(
                user_profile.user_chat_id.user_id,
                first_name=user_profile.first_name(),
                last_name=user_profile.last_name(),
            )
        },
    )


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


@contextlib.contextmanager
def open_user_profile(
    user_chat_id: UserChatId,
    user_storage: SqliteUserStorage,
) -> Iterator[tuple[UserProfile, ChatSettings]]:
    user_profile = user_storage.get_profile(user_chat_id)
    chat_settings = user_storage.get_chat_settings(user_chat_id.chat_id)
    original_json = user_profile.model_dump_json()
    yield user_profile, chat_settings
    # TODO: Make sure that model_dump_json is deterministic.
    modified_json = user_profile.model_dump_json()
    if original_json == modified_json:
        return
    logging.info("Saving profile of user %r", user_chat_id)
    user_storage.save_profile(
        user_profile,
        UserProfileParams(ichbin_waiting_time=chat_settings.ichbin_waiting_time),
    )


class EventProcessor:
    class Config(BaseModel):
        # How often should we check for periodic stuff, like users to kick,
        periodic_event_interval: timedelta = timedelta(seconds=3)
        admin_user_id: UserId = UserId(290342629)  # @icebergler

    def __init__(
        self,
        config: Config,
        bot: aiogram.Bot,
        telethon_client: Optional[telethon.TelegramClient],
        event_queue: BaseEventQueue,
        user_storage: SqliteUserStorage,
    ):
        self._config = config
        self._bot = bot
        self._telethon_client = telethon_client
        self._event_queue = event_queue
        self._user_storage = user_storage
        self._last_periodic_event_timestamp = LocalUTCTimestamp(0.0)
        self._stopped = False

    @contextlib.contextmanager
    def _open_user_profile(
        self, user_chat_id: UserChatId
    ) -> Iterator[tuple[UserProfile, ChatSettings]]:
        with open_user_profile(user_chat_id, self._user_storage) as user_profile:
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
        # TODO: Only execute command if it's actually targeted to the bot.
        if not command.startswith("/lancet_"):
            return
        response_message: str | None = None
        try:
            if command == "/lancet_message":
                destination_chat_id_str, _, message = rest.partition(" ")
                destination_chat_id = ChatId(int(destination_chat_id_str))
                logging.info(
                    "Sending message %r to chat %r", message, destination_chat_id
                )
                await self._bot.send_message(chat_id=destination_chat_id, text=message)
                response_message = "Message sent!"
            elif command == "/lancet_chats":
                chats = self._user_storage.get_chats()
                chat_lines = []
                for chat_id, chat_info in chats.items():
                    chat_lines.append(f"{chat_id}: {chat_info!r}")
                response_message = "Chats:\n" + "\n".join(chat_lines)
            elif command == "/lancet_get_settings":
                chat_id_str, _, _ = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                chat_settings = self._user_storage.get_chat_settings(chat_id)
                response_message = f"Settings for chat {chat_id}:\n{chat_settings.model_dump_json(indent=2)}"
            elif command == "/lancet_set_settings":
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                chat_settings = ChatSettings.model_validate_json(rest)
                self._user_storage.set_chat_settings(chat_id, chat_settings)
                response_message = f"Settings for chat {chat_id} updated."
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
            if response_message is None:
                response_message = "Command executed successfully."
            await self._bot.send_message(
                chat_id=event.user_chat_id.chat_id,
                text=response_message,
                reply_parameters=aiogram.types.ReplyParameters(
                    message_id=event.message_id, chat_id=event.user_chat_id.chat_id
                ),
            )

    async def _on_bot_api_new_text_message(self, event: BotApiNewTextMessage) -> None:
        self._user_storage.add_chat(event.user_chat_id.chat_id, event.chat_info)
        if event.user_chat_id.user_id == self._config.admin_user_id:
            logging.info("Got a message from admin: %r", event)
            await self._on_admin_message(event)
            return
        with self._open_user_profile(event.user_chat_id) as (
            user_profile,
            chat_settings,
        ):
            user_profile.basic_user_info = event.basic_user_info
            if "#ichbin" not in event.text:
                return
            if not user_profile.is_waiting_for_ichbin_message():
                return
            user_profile.ichbin_message_timestamp = event.recv_timestamp
            await self._send_message(
                user_profile,
                chat_settings.bot_reply_templates.welcome,
                BotApiMessageType.WELCOME,
            )

    async def _is_me(self, user_id: UserId) -> bool:
        return user_id == (await self._bot.me()).id

    async def _on_bot_api_new_chat_member(self, event: BotApiChatMemberJoined) -> None:
        self._user_storage.add_chat(event.user_chat_id.chat_id, event.chat_info)
        with self._open_user_profile(event.user_chat_id) as (
            user_profile,
            chat_settings,
        ):
            user_profile.basic_user_info = event.basic_user_info
            user_profile.on_joined(event.recv_timestamp)
            if user_profile.basic_user_info.is_bot:
                logging.info("Ignoring bot %r", user_profile.user_chat_id)
                return
            if user_profile.ichbin_message_timestamp is not None:
                await self._send_message(
                    user_profile,
                    chat_settings.bot_reply_templates.welcome_again,
                    BotApiMessageType.WELCOME_AGAIN,
                )
                return
            if user_profile.ichbin_request_timestamp is None:
                bot_message = await self._send_message(
                    user_profile,
                    chat_settings.bot_reply_templates.ichbin_request,
                    BotApiMessageType.ICHBIN_REQUEST,
                )
                user_profile.ichbin_request_timestamp = bot_message.sent_timestamp
                return
            chat_settings = self._user_storage.get_chat_settings(
                event.user_chat_id.chat_id
            )
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                UserProfileParams(ichbin_waiting_time=chat_settings.ichbin_waiting_time)
            )
            if kick_at_timestamp is None:
                logging.warning(
                    "BUG: kick_at_timestamp is None at current point : %r", user_profile
                )
                return
            time_left = kick_at_timestamp - event.recv_timestamp
            if (
                time_left
                > chat_settings.extra_ichbin_waiting_time_after_rejoining.total_seconds()
            ):
                # No need yet to warn the user that he will be kicked soon.
                return
            user_profile.add_extra_grace_time(
                chat_settings.extra_ichbin_waiting_time_after_rejoining.total_seconds()
                - time_left
            )
            await self._send_message(
                user_profile,
                chat_settings.bot_reply_templates.not_much_time_left_to_write_ichbin,
                BotApiMessageType.NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN,
            )

    async def _send_message(
        self,
        user_profile: UserProfile,
        message_template: safe_html_str,
        message_type: BotApiMessageType,
    ) -> BotApiMessage:
        chat_settings = self._user_storage.get_chat_settings(
            user_profile.user_chat_id.chat_id
        )
        if chat_settings.dark_launch_sink_chat_id is None:
            sent_message = await self._bot.send_message(
                user_profile.user_chat_id.chat_id,
                create_message_html(message_template, user_profile),
                parse_mode=ParseMode.HTML,
            )
        else:
            logging.info(
                "Redirecting message to dark launch chat %r",
                chat_settings.dark_launch_sink_chat_id,
            )
            sent_message = await self._bot.send_message(
                chat_settings.dark_launch_sink_chat_id,
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
        if await self._is_me(event.user_chat_id.user_id):
            self._user_storage.remove_chat(event.user_chat_id.chat_id)
        with self._open_user_profile(event.user_chat_id) as (
            user_profile,
            chat_settings,
        ):
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

    async def _delete_message(
        self, message: BotApiMessage, current_timestamp: LocalUTCTimestamp
    ) -> None:
        try:
            chat_settings = self._user_storage.get_chat_settings(
                message.user_chat_id.chat_id
            )
            if chat_settings.dark_launch_sink_chat_id is None:
                logging.info("Trying to delete message %r", message)
                await self._bot.delete_message(
                    chat_id=message.user_chat_id.chat_id, message_id=message.message_id
                )
            else:
                logging.info(
                    "Deleting message %r in dark launch chat %r",
                    message,
                    chat_settings.dark_launch_sink_chat_id,
                )
                await self._bot.delete_message(
                    chat_id=chat_settings.dark_launch_sink_chat_id,
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
        with self._open_user_profile(user_chat_id) as (user_profile, chat_settings):
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                UserProfileParams(ichbin_waiting_time=chat_settings.ichbin_waiting_time)
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
            if chat_settings.dark_launch_sink_chat_id is None:
                logging.info("Kicking user %r", user_chat_id)
                await self._bot.ban_chat_member(
                    chat_id=user_chat_id.chat_id,
                    user_id=user_chat_id.user_id,
                    until_date=chat_settings.ban_duration,
                )
            else:
                logging.info(
                    "Would have kicked user %r, but dark launch is enabled.",
                    user_chat_id,
                )
            user_profile.on_kicked(kick_timestamp=LocalUTCTimestamp(time.time()))
            try:
                await self._send_message(
                    user_profile,
                    chat_settings.bot_reply_templates.user_is_kicked,
                    BotApiMessageType.USER_IS_KICKED,
                )
            except Exception:
                logging.error(
                    "Failed to report that the user %r was kicked.",
                    user_chat_id,
                    exc_info=True,
                )
