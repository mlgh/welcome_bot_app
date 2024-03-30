import pytest
from unittest.mock import MagicMock
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.model import UserKey, StopEvent, BotApiNewChatMember, BotApiNewTextMessage, BotApiChatMemberLeft

@pytest.fixture
def event_processor():
    bot = MagicMock()
    telethon_client = MagicMock()
    event_storage = MagicMock()
    user_storage = MagicMock()
    return EventProcessor(bot, telethon_client, event_storage, user_storage)

@pytest.mark.asyncio
async def test_on_bot_api_new_chat_member(event_processor):
    await event_processor.put_event(BotApiNewChatMember(timestamp = 1, user_key = UserKey(user_id=1, chat_id=10)))
    await event_processor.put_event(StopEvent(timestamp=2))
    await event_processor.run()
    # Add your assertions here

# def test_on_bot_api_new_text_message(event_processor):
#     event = BotApiNewTextMessage()
#     event_processor._on_bot_api_new_text_message(event)
#     # Add your assertions here

# def test_on_bot_api_chat_member_left(event_processor):
#     event = BotApiChatMemberLeft()
#     event_processor.on_bot_api_chat_member_left(event)
#     # Add your assertions here

# Add more tests for other methods in the EventProcessor class