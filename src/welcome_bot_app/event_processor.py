import asyncio
import difflib
import logging
import time
import traceback
import html
from collections import defaultdict
from typing import DefaultDict, Iterable, Iterator, List, Optional
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
from welcome_bot_app.model.chat_settings import BotReplyType, ChatSettings
from welcome_bot_app.model.user_profile import (
    BotApiMessage,
    UserProfile,
    UserProfileParams,
    UserChatCapabilities,
)
from welcome_bot_app.safe_html import (
    escape_html,
    safe_html_format,
    safe_html_str,
    substitute_html,
)
from welcome_bot_app.bot_storage import BotStorage
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
    message: safe_html_str, user_profile: UserProfile, chat_settings: ChatSettings
) -> safe_html_str:
    return substitute_html(
        message,
        {
            "TAG": safe_html_str(chat_settings.introduction_tag),
            "USER": _create_user_mention_html(
                user_profile.user_chat_id.user_id,
                first_name=user_profile.first_name(),
                last_name=user_profile.last_name(),
            ),
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
    bot_storage: BotStorage,
    chat_settings: ChatSettings,
) -> Iterator[UserProfile]:
    user_profile = bot_storage.get_profile(user_chat_id)
    user_profile_params = UserProfileParams(
        ichbin_waiting_time=chat_settings.ichbin_waiting_time,
        failed_kick_retry_time=chat_settings.failed_kick_retry_time,
    )
    original_json = user_profile.model_dump_json()
    pretty_original_json = user_profile.model_dump_json(indent=2)
    previous_kick_at_timestamp = user_profile.get_kick_at_timestamp(user_profile_params)
    yield user_profile
    modified_json = user_profile.model_dump_json()
    if original_json == modified_json:
        return
    pretty_modified_json = user_profile.model_dump_json(indent=2)
    modified_kick_at_timestamp = user_profile.get_kick_at_timestamp(user_profile_params)
    diff = list(
        difflib.unified_diff(
            pretty_original_json.splitlines(), pretty_modified_json.splitlines(), n=0
        )
    )
    logging.info("Saving profile of user %r", user_chat_id)
    # First two lines are file names.
    for line in diff[2:]:
        if line.startswith("-") or line.startswith("+"):
            logging.info("Diff: %s: %s", user_chat_id, line)
    if previous_kick_at_timestamp != modified_kick_at_timestamp:
        logging.info(
            "Diff: %s: kick_at_timestamp changed from %r to %r",
            user_chat_id,
            previous_kick_at_timestamp,
            modified_kick_at_timestamp,
        )
    bot_storage.save_profile(user_profile, user_profile_params)


class MissingCapabilities(Exception):
    """Raised when user tries to run a bot command without necessary capabilities."""

    pass


class EventProcessor:
    class Config(BaseModel):
        # How often should we check for periodic stuff, like users to kick,
        periodic_event_interval: timedelta = timedelta(seconds=3)
        # TODO: Make this a flag.
        # Global admin, @icebergler
        root_admin_user_id: UserId = UserId(290342629)
        chat_cmd_prefix: str = "/lancet_"

    def __init__(
        self,
        config: Config,
        bot: aiogram.Bot,
        telethon_client: Optional[telethon.TelegramClient],
        event_queue: BaseEventQueue,
        bot_storage: BotStorage,
    ):
        self._config = config
        self._bot = bot
        self._telethon_client = telethon_client
        self._event_queue = event_queue
        self._bot_storage = bot_storage
        self._last_periodic_event_timestamp = LocalUTCTimestamp(0.0)
        self._stopped = False

    @contextlib.contextmanager
    def _open_user_profile(
        self, user_chat_id: UserChatId, chat_settings: ChatSettings
    ) -> Iterator[UserProfile]:
        with open_user_profile(
            user_chat_id, self._bot_storage, chat_settings
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

    def _get_capabilities(
        self, user_id: UserId, chat_id: ChatId
    ) -> UserChatCapabilities:
        if user_id == self._config.root_admin_user_id:
            return UserChatCapabilities.root_capabilities()
        return self._bot_storage.get_user_chat_capabilities(user_id, chat_id)

    async def _on_admin_message(self, event: BotApiNewTextMessage) -> None:
        cmd_user_id = event.user_chat_id.user_id
        text = event.text
        command, _, rest = text.partition(" ")
        response_message: str | None = None
        traceback_message: str | None = None
        try:
            # TODO: Add easier settings handling.
            # TODO: Make this readable and easy to extend.
            # TODO: Add safe_html_str here, so that we could use HTML in messages.
            if command == self._config.chat_cmd_prefix + "message":
                destination_chat_id_str, _, message = rest.partition(" ")
                destination_chat_id = ChatId(int(destination_chat_id_str))
                capabilities = self._bot_storage.get_user_chat_capabilities(
                    cmd_user_id, destination_chat_id
                )
                if not capabilities.can_send_messages_from_bot:
                    raise MissingCapabilities(
                        "User %s isn't allowed to send messages to chat %s from bot's name."
                        % (cmd_user_id, destination_chat_id)
                    )
                logging.info(
                    "Sending message %r to chat %r", message, destination_chat_id
                )
                await self._bot.send_message(chat_id=destination_chat_id, text=message)
                response_message = "Message sent!"
            elif command == self._config.chat_cmd_prefix + "chats":
                chats = self._bot_storage.get_chats()
                chat_lines = []
                for chat_id, chat_info in chats.items():
                    # Don't show chats which the user can't edit.
                    if not self._get_capabilities(
                        cmd_user_id, chat_id
                    ).can_update_settings:
                        continue
                    chat_settings = self._bot_storage.get_chat_settings(chat_id)
                    is_enabled = (
                        "**ENABLED**"
                        if chat_settings.ichbin_enabled
                        else "**DISABLED**"
                    )
                    chat_lines.append(f"{chat_id}: {is_enabled} {chat_info!r}")
                if not chat_lines:
                    response_message = "No chats to show."
                else:
                    response_message = "Chats:\n" + "\n".join(chat_lines)
            elif command == self._config.chat_cmd_prefix + "get_settings":
                chat_id_str, _, _ = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(cmd_user_id, chat_id).can_update_settings:
                    raise MissingCapabilities(
                        "User %s isn't allowed to view settings for chat %s."
                        % (cmd_user_id, chat_id)
                    )
                chat_settings = self._bot_storage.get_chat_settings(chat_id)
                response_message = f"Settings for chat {chat_id}:\n{chat_settings.model_dump_json(indent=2)}"
            elif command == self._config.chat_cmd_prefix + "set_settings":
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(cmd_user_id, chat_id).can_update_settings:
                    raise MissingCapabilities(
                        "User %s isn't allowed to update settings for chat %s."
                        % (cmd_user_id, chat_id)
                    )
                chat_settings = ChatSettings.model_validate_json(rest)
                self._bot_storage.set_chat_settings(chat_id, chat_settings)
                response_message = f"Settings for chat {chat_id} updated."
            elif command == self._config.chat_cmd_prefix + "set_message":
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(cmd_user_id, chat_id).can_update_settings:
                    raise MissingCapabilities(
                        "User %s isn't allowed to update messages for chat %s."
                        % (cmd_user_id, chat_id)
                    )
                bot_reply_type_str, _, message_template = rest.partition(" ")
                bot_reply_type = BotReplyType(bot_reply_type_str)
                chat_settings = self._bot_storage.get_chat_settings(chat_id)
                chat_settings.bot_replies.get_reply(
                    bot_reply_type
                ).template = safe_html_str(message_template)
                self._bot_storage.set_chat_settings(chat_id, chat_settings)
                response_message = f"Message template for reply type {bot_reply_type.value} is set to:\n{message_template}"
            elif command == self._config.chat_cmd_prefix + "chat_enable":
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(cmd_user_id, chat_id).can_update_settings:
                    raise MissingCapabilities(
                        "User %s isn't allowed to enable #ichbin for chat %s."
                        % (cmd_user_id, chat_id)
                    )
                chat_settings = self._bot_storage.get_chat_settings(chat_id)
                chat_settings.ichbin_enabled = True
                self._bot_storage.set_chat_settings(chat_id, chat_settings)
                response_message = f"#ichbin feature enabled for chat {chat_id}."
            elif command == self._config.chat_cmd_prefix + "chat_disable":
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(cmd_user_id, chat_id).can_update_settings:
                    raise MissingCapabilities(
                        "User %s isn't allowed to disable #ichbin for chat %s."
                        % (cmd_user_id, chat_id)
                    )
                chat_settings = self._bot_storage.get_chat_settings(chat_id)
                chat_settings.ichbin_enabled = False
                self._bot_storage.set_chat_settings(chat_id, chat_settings)
                response_message = f"#ichbin feature disabled for chat {chat_id}."
            elif command == self._config.chat_cmd_prefix + "set_caps":
                user_id_str, _, rest = rest.partition(" ")
                user_id = UserId(int(user_id_str))
                chat_id_str, _, rest = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(
                    cmd_user_id, chat_id
                ).can_update_capabilities:
                    raise MissingCapabilities(
                        "User %s isn't allowed to set capabilities in chat %s."
                        % (cmd_user_id, chat_id)
                    )
                capabilities = UserChatCapabilities.model_validate_json(rest)
                if cmd_user_id == user_id and capabilities.can_update_capabilities:
                    response_message = 'You are trying to set "can_update_capabilities" to False for yourself. This is not allowed.'
                else:
                    self._bot_storage.set_user_chat_capabilities(
                        user_id, chat_id, capabilities
                    )
            elif command == self._config.chat_cmd_prefix + "get_caps":
                user_id_str, _, rest = rest.partition(" ")
                user_id = UserId(int(user_id_str))
                chat_id_str, _, _ = rest.partition(" ")
                chat_id = ChatId(int(chat_id_str))
                if not self._get_capabilities(
                    cmd_user_id, chat_id
                ).can_update_capabilities:
                    raise MissingCapabilities(
                        "User %s isn't allowed to view capabilities in chat %s."
                        % (cmd_user_id, chat_id)
                    )
                capabilities = self._get_capabilities(user_id, chat_id)
                response_message = f"Capabilities for user {user_id} in chat {chat_id}:\n{capabilities.model_dump_json(indent=2)}"
            else:
                raise ValueError(f"Unknown command: {command}")
        except MissingCapabilities as exc:
            logging.warning(
                "User %r tried to run command %r without necessary capabilities: %r",
                cmd_user_id,
                text,
                exc,
            )
            response_message = "You don't have enough capabilities to run this command."
            traceback_message = traceback.format_exc()
        except Exception:
            logging.error("Failed to execute admin command: %s", text, exc_info=True)
            response_message = "Failed to execute command."
            traceback_message = traceback.format_exc()
        if response_message is None:
            response_message = "Command executed successfully."
        if traceback_message is not None:
            if not self._get_capabilities(
                cmd_user_id, chat_id=ChatId(cmd_user_id)
            ).can_view_tracebacks:
                response_message += (
                    "\n\n" + "Traceback is in the logs. Ask bot admin for more info."
                )
            else:
                if event.chat_info.is_private():
                    response_message += "\n\n" + traceback_message
                else:
                    response_message += (
                        "\n\n" + "Traceback was sent to you in a private message."
                    )
                    await self._bot.send_message(
                        chat_id=event.user_chat_id.user_id,
                        text=html.escape(traceback_message),
                        parse_mode=ParseMode.HTML,
                    )
        await self._bot.send_message(
            chat_id=event.user_chat_id.chat_id,
            text=html.escape(response_message),
            reply_parameters=aiogram.types.ReplyParameters(
                message_id=event.message_id, chat_id=event.user_chat_id.chat_id
            ),
            parse_mode=ParseMode.HTML,
        )

    async def _on_bot_api_new_text_message(self, event: BotApiNewTextMessage) -> None:
        self._bot_storage.add_chat(event.user_chat_id.chat_id, event.chat_info)

        chat_settings = self._bot_storage.get_chat_settings(event.user_chat_id.chat_id)
        if event.text.startswith(self._config.chat_cmd_prefix):
            logging.info("Got a command-like message from admin: %r", event)
            await self._on_admin_message(event)
            return
        # We process #ichbin messages even if the bot is disabled in the chat.
        # TODO: Update this behavior if it's not desired.
        with self._open_user_profile(event.user_chat_id, chat_settings) as user_profile:
            user_profile.basic_user_info = event.basic_user_info
            if "#ichbin" not in event.text:
                return
            if not user_profile.is_waiting_for_ichbin_message():
                logging.info(
                    "User %s is not waiting for ichbin message", event.user_chat_id
                )
                return
            logging.info(
                "Setting ichbin_message_timestamp for user %s to %s",
                event.user_chat_id,
                event.recv_timestamp,
            )
            user_profile.ichbin_message_timestamp = event.recv_timestamp
            await self._send_bot_reply(
                user_profile,
                BotReplyType.WELCOME,
                chat_settings,
            )

    async def _is_me(self, user_id: UserId) -> bool:
        return user_id == (await self._bot.me()).id

    async def _on_bot_api_new_chat_member(self, event: BotApiChatMemberJoined) -> None:
        self._bot_storage.add_chat(event.user_chat_id.chat_id, event.chat_info)
        chat_settings = self._bot_storage.get_chat_settings(event.user_chat_id.chat_id)
        if not chat_settings.ichbin_enabled:
            logging.info(
                "Chat %s has #ichbin disabled, ignoring new member %r.",
                event.user_chat_id.chat_id,
                event.user_chat_id,
            )
            return
        with self._open_user_profile(event.user_chat_id, chat_settings) as user_profile:
            user_profile.basic_user_info = event.basic_user_info
            user_profile.on_joined(event.recv_timestamp)
            if user_profile.basic_user_info.is_bot:
                logging.info("Ignoring bot %r", user_profile.user_chat_id)
                return
            if user_profile.ichbin_message_timestamp is not None:
                logging.info(
                    "Joined user %s already has ichbin message at timestamp %s",
                    user_profile.user_chat_id,
                    user_profile.ichbin_message_timestamp,
                )
                await self._send_bot_reply(
                    user_profile,
                    BotReplyType.WELCOME_AGAIN,
                    chat_settings,
                )
                return
            if user_profile.ichbin_request_timestamp is None:
                logging.info(
                    "User %s has no ichbin request timestamp, sending ichbin request",
                    user_profile.user_chat_id,
                )
                bot_message = await self._send_bot_reply(
                    user_profile,
                    BotReplyType.ICHBIN_REQUEST,
                    chat_settings,
                )
                user_profile.ichbin_request_timestamp = bot_message.sent_timestamp
                return
            chat_settings = self._bot_storage.get_chat_settings(
                event.user_chat_id.chat_id
            )
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                UserProfileParams(
                    ichbin_waiting_time=chat_settings.ichbin_waiting_time,
                    failed_kick_retry_time=chat_settings.failed_kick_retry_time,
                )
            )
            if kick_at_timestamp is None:
                logging.warning(
                    "BUG: kick_at_timestamp is None at current point : %r", user_profile
                )
                return
            time_left = kick_at_timestamp - event.recv_timestamp
            logging.info(
                "Time left for user %s to write ichbin: %s (boundary: %s)",
                user_profile.user_chat_id,
                time_left,
                chat_settings.extra_ichbin_waiting_time_after_rejoining.total_seconds(),
            )
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
            await self._send_bot_reply(
                user_profile,
                BotReplyType.NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN,
                chat_settings,
            )

    async def _send_bot_reply(
        self,
        user_profile: UserProfile,
        bot_reply_type: BotReplyType,
        chat_settings: ChatSettings,
    ) -> BotApiMessage:
        if chat_settings.dark_launch_sink_chat_id is None:
            sent_message = await self._bot.send_message(
                user_profile.user_chat_id.chat_id,
                create_message_html(
                    chat_settings.bot_replies.get_reply(bot_reply_type).template,
                    user_profile,
                    chat_settings,
                ),
                parse_mode=ParseMode.HTML,
            )
        else:
            logging.info(
                "Redirecting message to dark launch chat %r",
                chat_settings.dark_launch_sink_chat_id,
            )
            sent_message = await self._bot.send_message(
                chat_settings.dark_launch_sink_chat_id,
                create_message_html(
                    chat_settings.bot_replies.get_reply(bot_reply_type).template,
                    user_profile,
                    chat_settings,
                ),
                parse_mode=ParseMode.HTML,
            )
        sent_timestamp = LocalUTCTimestamp(time.time())
        bot_api_message = BotApiMessage(
            user_chat_id=user_profile.user_chat_id,
            message_id=BotApiMessageId(sent_message.message_id),
            reply_type=bot_reply_type,
            sent_timestamp=sent_timestamp,
        )
        logging.info("Sent message: %r", bot_api_message)
        self._bot_storage.add_bot_message(bot_api_message)
        return bot_api_message

    async def _on_bot_api_chat_member_left(self, event: BotApiChatMemberLeft) -> None:
        logging.info("User %s left the chat", event.user_chat_id)
        if await self._is_me(event.user_chat_id.user_id):
            logging.info("I left the chat %r", event.user_chat_id.chat_id)
            self._bot_storage.remove_chat(event.user_chat_id.chat_id)
        chat_settings = self._bot_storage.get_chat_settings(event.user_chat_id.chat_id)
        with self._open_user_profile(event.user_chat_id, chat_settings) as user_profile:
            user_profile.on_left(left_timestamp=event.recv_timestamp)

    async def _on_periodic_event(self, event: PeriodicEvent) -> None:
        users_to_kick = self._bot_storage.get_users_to_kick(event.recv_timestamp)
        if users_to_kick:
            logging.info("Found users to kick: %r", users_to_kick)
        for user_chat_id in users_to_kick:
            try:
                await self._verify_and_kick_user(user_chat_id, event.recv_timestamp)
            except Exception:
                logging.error(
                    "Error while kicking user %r", user_chat_id, exc_info=True
                )
                continue
        welcome_messages_per_chat = defaultdict(list)
        messages_per_user: DefaultDict[UserChatId, List[BotApiMessage]] = defaultdict(
            list
        )
        for bot_api_message in self._bot_storage.get_bot_messages():
            if bot_api_message.reply_type == BotReplyType.WELCOME:
                welcome_messages_per_chat[bot_api_message.user_chat_id.chat_id].append(
                    bot_api_message
                )
            messages_per_user[bot_api_message.user_chat_id].append(bot_api_message)
        for chat_id, welcome_messages in welcome_messages_per_chat.items():
            chat_settings = self._bot_storage.get_chat_settings(chat_id)
            welcome_messages = await self._delete_all_but_last_message(
                welcome_messages,
                event.recv_timestamp,
                delete_welcome_messages=True,
                chat_settings=chat_settings,
            )
            await self._delete_expired_messages(
                welcome_messages, event.recv_timestamp, delete_welcome_messages=True
            )
        for user, messages in messages_per_user.items():
            chat_settings = self._bot_storage.get_chat_settings(user.chat_id)
            messages = await self._delete_all_but_last_message(
                messages,
                event.recv_timestamp,
                delete_welcome_messages=False,
                chat_settings=chat_settings,
            )
            await self._delete_expired_messages(
                messages, event.recv_timestamp, delete_welcome_messages=False
            )

    async def _delete_all_but_last_message(
        self,
        messages: Iterable[BotApiMessage],
        current_timestamp: LocalUTCTimestamp,
        delete_welcome_messages: bool,
        chat_settings: ChatSettings,
    ) -> List[BotApiMessage]:
        messages = list(messages)
        messages.sort(key=lambda msg: msg.sent_timestamp)
        for msg in messages[:-1]:
            # Even if we don't delete welcome messages, we still make them take space in the list, so that earlier messages would get removed.
            if msg.reply_type == BotReplyType.WELCOME and not delete_welcome_messages:
                continue
            await self._delete_message(msg, current_timestamp, chat_settings)
        return messages[-1:]

    async def _delete_expired_messages(
        self,
        messages: Iterable[BotApiMessage],
        current_timestamp: LocalUTCTimestamp,
        delete_welcome_messages: bool,
    ) -> None:
        for msg in messages:
            if msg.reply_type == BotReplyType.WELCOME and not delete_welcome_messages:
                continue
            chat_settings = self._bot_storage.get_chat_settings(
                msg.user_chat_id.chat_id
            )
            msg_ttl = chat_settings.bot_replies.get_reply(msg.reply_type).ttl
            if current_timestamp > msg.sent_timestamp + msg_ttl.total_seconds():
                await self._delete_message(msg, current_timestamp, chat_settings)

    async def _delete_message(
        self,
        message: BotApiMessage,
        current_timestamp: LocalUTCTimestamp,
        chat_settings: ChatSettings,
    ) -> None:
        try:
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
            self._bot_storage.mark_bot_message_as_deleted(
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
        chat_settings = self._bot_storage.get_chat_settings(user_chat_id.chat_id)
        with self._open_user_profile(user_chat_id, chat_settings) as user_profile:
            if not chat_settings.ichbin_enabled:
                logging.info(
                    "Chat %s has #ichbin disabled, user %r is forgiven.",
                    user_chat_id.chat_id,
                    user_chat_id,
                )
                user_profile.forgiven_timestamp = current_timestamp
                return
            kick_at_timestamp = user_profile.get_kick_at_timestamp(
                UserProfileParams(
                    ichbin_waiting_time=chat_settings.ichbin_waiting_time,
                    failed_kick_retry_time=chat_settings.failed_kick_retry_time,
                )
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
                try:
                    await self._bot.ban_chat_member(
                        chat_id=user_chat_id.chat_id,
                        user_id=user_chat_id.user_id,
                        until_date=chat_settings.ban_duration,
                    )
                except Exception:
                    logging.error("Failed to kick user %s", user_chat_id, exc_info=True)
                    user_profile.on_failed_to_kick(kick_timestamp=current_timestamp)
                    return
                else:
                    logging.info("Successfully kicked user %s", user_chat_id)
                    user_profile.on_kicked(
                        kick_timestamp=current_timestamp, is_dark_launch=False
                    )
            else:
                logging.info(
                    "Would have kicked user %r, but dark launch is enabled.",
                    user_chat_id,
                )
                user_profile.on_kicked(
                    kick_timestamp=current_timestamp, is_dark_launch=True
                )
            try:
                await self._send_bot_reply(
                    user_profile,
                    BotReplyType.USER_IS_KICKED,
                    chat_settings,
                )
            except Exception:
                logging.error(
                    "Failed to report that the user %r was kicked.",
                    user_chat_id,
                    exc_info=True,
                )
