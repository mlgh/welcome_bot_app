from pydantic import BaseModel
from enum import Enum
from datetime import timedelta
from welcome_bot_app.model import UserChatId, LocalUTCTimestamp, BotApiMessageId
from welcome_bot_app.model.events import BasicUserInfo


class UserProfileParams(BaseModel):
    """Parameters for computing fields of the user profile."""

    # How long to wait for an #ichbin message before kicking the user.
    ichbin_waiting_time: timedelta


class BotApiMessageType(Enum):
    ICHBIN_REQUEST = "ICHBIN_REQUEST"
    WELCOME = "WELCOME"
    WELCOME_AGAIN = "WELCOME_AGAIN"
    NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN = "NOT_MUCH_TIME_LEFT_TO_WRITE_ICHBIN"
    USER_IS_KICKED = "USER_IS_KICKED"


class BotApiMessage(BaseModel):
    # User + chat who sent this message
    user_chat_id: UserChatId
    # Message id in this chat.
    message_id: BotApiMessageId
    # Type of the message.
    message_type: BotApiMessageType
    # Timestamp when this message was sent.
    sent_timestamp: LocalUTCTimestamp


class PresenceInfo(BaseModel):
    # Timestamp when the user joined.
    joined_timestamp: LocalUTCTimestamp | None = None
    # Timestamp when the user was kicked.
    kick_timestamp: LocalUTCTimestamp | None = None
    # Timestampt when the user left.
    left_timestamp: LocalUTCTimestamp | None = None

    def is_present(self) -> bool:
        return self.joined_timestamp is not None and self.left_timestamp is None


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

    def on_joined(self, joined_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info = PresenceInfo(joined_timestamp=joined_timestamp)

    def on_left(self, left_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info.left_timestamp = left_timestamp

    def on_kicked(self, kick_timestamp: LocalUTCTimestamp) -> None:
        self.presence_info.kick_timestamp = kick_timestamp

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
        )

    def add_extra_grace_time(self, extra_grace_time: float) -> None:
        self.extra_grace_time += extra_grace_time

    def get_kick_at_timestamp(
        self, user_profile_params: UserProfileParams
    ) -> LocalUTCTimestamp | None:
        if not self.presence_info.is_present():
            return None
        if self.ichbin_message_timestamp is not None:
            return None
        if self.ichbin_request_timestamp is None:
            return None
        return LocalUTCTimestamp(
            self.ichbin_request_timestamp
            + user_profile_params.ichbin_waiting_time.total_seconds()
            + self.extra_grace_time
        )
