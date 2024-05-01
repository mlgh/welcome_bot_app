from pydantic import BaseModel
from datetime import timedelta
from welcome_bot_app.model import UserChatId, LocalUTCTimestamp, BotApiMessageId
from welcome_bot_app.model.chat_settings import BotReplyType
from welcome_bot_app.model.events import BasicUserInfo


class UserChatCapabilities(BaseModel):
    # Whether user could modify capabilities of other users. This is a dangerous capability: the user could potentially strip themself of all capabilities.
    can_update_capabilities: bool = False
    # Whether the user could update chat settings.
    can_update_settings: bool = False
    # Whether the user could send messages from the bot's name.
    can_send_messages_from_bot: bool = False
    # Whether the user could view tracebacks. This property is only read for private chats.
    can_view_tracebacks: bool = False

    @classmethod
    def root_capabilities(cls) -> "UserChatCapabilities":
        instance = cls()
        # Set all fields starting with "can_" to True
        # XXX: This is a bit hacky.
        for field in cls.model_fields.keys():
            if field.startswith("can_"):
                setattr(instance, field, True)
        return instance


class UserProfileParams(BaseModel):
    """Parameters for computing fields of the user profile."""

    # How long to wait for an #ichbin message before kicking the user.
    ichbin_waiting_time: timedelta

    # How long to wait after an unsuccessful kick before trying again.
    failed_kick_retry_time: timedelta


class BotApiMessage(BaseModel):
    # User + chat who sent this message
    user_chat_id: UserChatId
    # Message id in this chat.
    message_id: BotApiMessageId
    # Type of the bot reply.
    reply_type: BotReplyType
    # Timestamp when this message was sent.
    sent_timestamp: LocalUTCTimestamp


class PresenceInfo(BaseModel):
    # Timestamp when the user joined.
    joined_timestamp: LocalUTCTimestamp | None = None
    # Timestamp when the user was kicked.
    kick_timestamp: LocalUTCTimestamp | None = None
    # Timestamp when the user left.
    left_timestamp: LocalUTCTimestamp | None = None
    # Treat as if the user has left.
    treat_as_left: bool = False
    # Timestamp when we last failed to kick the user.
    failed_kick_timestamp: LocalUTCTimestamp | None = None

    def is_present(self) -> bool:
        if self.treat_as_left:
            return False
        if self.joined_timestamp is None:
            return False
        if self.left_timestamp is not None:
            return False
        return True


class UserProfile(BaseModel):
    """User profile, stored in Sqlite."""

    user_chat_id: UserChatId
    # Information about user presence in the chat.
    presence_info: PresenceInfo

    # Stuff like first/last name, is bot, etc.
    basic_user_info: BasicUserInfo | None = None

    ichbin_request_timestamp: LocalUTCTimestamp | None = None
    # Grace time given to the user
    extra_grace_time: float = 0.0

    ichbin_message_timestamp: LocalUTCTimestamp | None = None

    # This user had to be kicked when the bot was disabled, therefore it was forgiven at the given timestamp.
    forgiven_timestamp: LocalUTCTimestamp | None = None

    def on_joined(self, joined_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info = PresenceInfo(joined_timestamp=joined_timestamp)

    def on_left(self, left_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info.left_timestamp = left_timestamp

    def on_kicked(
        self, kick_timestamp: LocalUTCTimestamp, is_dark_launch: bool
    ) -> None:
        self.presence_info.kick_timestamp = kick_timestamp
        if is_dark_launch:
            self.presence_info.treat_as_left = True

    def on_failed_to_kick(self, kick_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info.failed_kick_timestamp = kick_timestamp

    def first_name(self) -> str | None:
        return (
            self.basic_user_info.first_name
            if self.basic_user_info is not None
            else None
        )

    def last_name(self) -> str | None:
        return (
            self.basic_user_info.last_name if self.basic_user_info is not None else None
        )

    def is_waiting_for_ichbin_message(self) -> bool:
        return (
            self.ichbin_request_timestamp is not None
            and self.ichbin_message_timestamp is None
            and self.forgiven_timestamp is None
        )

    def add_extra_grace_time(self, extra_grace_time: float) -> None:
        self.extra_grace_time += extra_grace_time

    def get_kick_at_timestamp(
        self, user_profile_params: UserProfileParams
    ) -> LocalUTCTimestamp | None:
        if not self.presence_info.is_present():
            return None
        if not self.is_waiting_for_ichbin_message():
            return None
        # Invariant enforced by self.is_waiting_for_ichbin_message()
        assert self.ichbin_request_timestamp is not None
        result = (
            self.ichbin_request_timestamp
            + user_profile_params.ichbin_waiting_time.total_seconds()
            + self.extra_grace_time
        )
        if self.presence_info.failed_kick_timestamp is not None:
            result = max(
                result,
                self.presence_info.failed_kick_timestamp
                + user_profile_params.failed_kick_retry_time.total_seconds(),
            )
        return LocalUTCTimestamp(result)
