from pydantic import BaseModel, Field, TypeAdapter
from typing import Literal, Annotated, Union
import aiogram.types
from welcome_bot_app.model import (
    LocalUTCTimestamp,
    TelethonUTCTimestamp,
    UserChatId,
    BotApiUTCTimestamp,
    BotApiMessageId,
    TelethonMessageId,
)


class BaseEvent(BaseModel):
    """Base class for all events."""

    event_type: str = "BaseEvent"
    recv_timestamp: LocalUTCTimestamp

    @classmethod
    def get_subclasses(cls) -> "tuple[type[BaseEvent], ...]":
        return tuple(cls.__subclasses__())


# Bot API events


class BasicUserInfo(BaseModel):
    is_bot: bool
    first_name: str
    last_name: str | None


class BotApiChatInfo(BaseModel):
    chat_type: str
    title: str | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None

    @classmethod
    def from_bot_api_chat(cls, chat: aiogram.types.Chat) -> "BotApiChatInfo":
        return cls(
            chat_type=chat.type,
            title=chat.title,
            username=chat.username,
            first_name=chat.first_name,
            last_name=chat.last_name,
        )

    def is_private(self) -> bool:
        return self.chat_type == "private"


class BotApiNewTextMessage(BaseEvent):
    """New text message from user."""

    event_type: Literal["BotApiNewTextMessage"] = "BotApiNewTextMessage"

    user_chat_id: UserChatId
    basic_user_info: BasicUserInfo
    text: str
    is_edited: bool
    message_id: BotApiMessageId
    tg_timestamp: BotApiUTCTimestamp
    chat_info: BotApiChatInfo


class BotApiChatMemberJoined(BaseEvent):
    """New chat member."""

    event_type: Literal["BotApiChatMemberJoined"] = "BotApiChatMemberJoined"

    user_chat_id: UserChatId
    basic_user_info: BasicUserInfo
    tg_timestamp: BotApiUTCTimestamp
    chat_info: BotApiChatInfo


class BotApiChatMemberLeft(BaseEvent):
    """Chat member left."""

    event_type: Literal["BotApiChatMemberLeft"] = "BotApiChatMemberLeft"

    user_chat_id: UserChatId
    tg_timestamp: BotApiUTCTimestamp
    chat_info: BotApiChatInfo


# Telethon events


class TelethonNewTextMessage(BaseEvent):
    """New text message from user."""

    event_type: Literal["TelethonNewTextMessage"] = "TelethonNewTextMessage"

    user_chat_id: UserChatId
    text: str
    message_id: TelethonMessageId
    tg_timestamp: TelethonUTCTimestamp


# Periodic event


class PeriodicEvent(BaseEvent):
    """Periodic event."""

    event_type: Literal["PeriodicEvent"] = "PeriodicEvent"


class StopEvent(BaseEvent):
    """Stops the event processor."""

    event_type: Literal["StopEvent"] = "StopEvent"


BaseEventSubclass = TypeAdapter(
    Annotated[Union[BaseEvent.get_subclasses()], Field(discriminator="event_type")]
)
