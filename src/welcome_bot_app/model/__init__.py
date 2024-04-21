from typing import NewType
from pydantic import BaseModel

# Locally created UTC timestamp in seconds.
LocalUTCTimestamp = NewType("LocalUTCTimestamp", float)

# UTC timestamp from Telegram Bot API in seconds.
BotApiUTCTimestamp = NewType("BotApiUTCTimestamp", float)

# UTC timestamp from Telethon MTProto API in seconds.
TelethonUTCTimestamp = NewType("TelethonUTCTimestamp", float)

UserId = NewType("UserId", int)
ChatId = NewType("ChatId", int)


class UserChatId(BaseModel):
    """User identifier. We treat same user in different chats as different users."""

    user_id: UserId
    chat_id: ChatId

    def __hash__(self) -> int:
        return (self.user_id, self.chat_id).__hash__()


# message_id as seen by the bot API.
BotApiMessageId = NewType("BotApiMessageId", int)

# message_id as seen by Telethon MTProto API
TelethonMessageId = NewType("TelethonMessageId", int)
