from dataclasses import dataclass
from typing import Optional


@dataclass
class UserKey:
    """User identifier. We treat same user in different chats as different users."""

    user_id: int
    chat_id: int


@dataclass
class UserProfile:
    """User profile, stored in Sqlite."""

    user_key: UserKey
    ichbin_message: Optional[str] = None
    ichbin_message_timestamp: Optional[float] = None
    ichbin_message_id: Optional[int] = None
    ichbin_request_timestamp: Optional[float] = None
    local_kicked_timestamp: Optional[float] = None


@dataclass
class BotApiUserInfo:
    is_bot: bool
    first_name: str


@dataclass
class Event:
    """Base class for all external events."""

    # UTC timestamp when this even was created
    local_timestamp: float


@dataclass
class PeriodicEvent(Event):
    """Periodic event."""

    pass


@dataclass
class BotApiNewTextMessage(Event):
    """New text message from user."""

    user_key: UserKey
    user_info: BotApiUserInfo
    text: str
    message_id: int
    tg_timestamp: float


@dataclass
class BotApiNewChatMember(Event):
    """New chat member."""

    user_key: UserKey
    tg_timestamp: float


@dataclass
class BotApiChatMemberLeft(Event):
    """Chat member left."""

    user_key: UserKey
    tg_timestamp: float


@dataclass
class StopEvent(Event):
    """Stop processing events and exit. Used only for testing."""

    pass


@dataclass
class TelethonNewMessage(Event):
    user_key: UserKey
    text: str
    message_id: int
    tg_timestamp: float
