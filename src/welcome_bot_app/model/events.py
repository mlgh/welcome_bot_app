from pydantic import BaseModel, Field, TypeAdapter
from typing import Literal, Annotated, Union
from welcome_bot_app.model import (
    LocalUTCTimestamp,
    UserChatId,
    BotApiUTCTimestamp,
    BotApiMessageId,
    TelethonMessageId,
)


class BaseEvent(BaseModel):
    """Base class for all events."""

    type: Literal["BaseEvent"] = "BaseEvent"
    recv_timestamp: LocalUTCTimestamp

    @classmethod
    def get_subclasses(cls):
        return tuple(cls.__subclasses__())


# Bot API events


class BasicUserInfo(BaseModel):
    is_bot: bool
    first_name: str
    last_name: str | None


class BotApiNewTextMessage(BaseEvent):
    """New text message from user."""

    type: Literal["BotApiNewTextMessage"] = "BotApiNewTextMessage"

    user_chat_id: UserChatId
    basic_user_info: BasicUserInfo
    text: str
    is_edited: bool
    message_id: BotApiMessageId
    tg_timestamp: BotApiUTCTimestamp


class BotApiChatMemberJoined(BaseEvent):
    """New chat member."""

    type: Literal["BotApiChatMemberJoined"] = "BotApiChatMemberJoined"

    user_chat_id: UserChatId
    basic_user_info: BasicUserInfo
    tg_timestamp: BotApiUTCTimestamp


class BotApiChatMemberLeft(BaseEvent):
    """Chat member left."""

    type: Literal["BotApiChatMemberLeft"] = "BotApiChatMemberLeft"

    user_chat_id: UserChatId
    tg_timestamp: BotApiUTCTimestamp


# Telethon events


class TelethonNewTextMessage(BaseEvent):
    """New text message from user."""

    type: Literal["TelethonNewTextMessage"] = "TelethonNewTextMessage"

    user_chat_id: UserChatId
    text: str
    message_id: TelethonMessageId
    tg_timestamp: BotApiUTCTimestamp


# Periodic event


class PeriodicEvent(BaseEvent):
    """Periodic event."""

    type: Literal["PeriodicEvent"] = "PeriodicEvent"


BaseEventSubclass = TypeAdapter(
    Annotated[Union[BaseEvent.get_subclasses()], Field(discriminator="type")]
)
