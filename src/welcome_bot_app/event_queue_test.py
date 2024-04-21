# import pytest
# from welcome_bot_app.event_queue import BaseEventQueue, SqliteEventQueue, EventData
# from welcome_bot_app.model.events import BotApiChatMemberJoined, BotApiChatMemberLeft

# @pytest.fixture
# def max_attempts() -> int:
#     return 2

# @pytest.fixture
# def event_queue(max_attempts : int):
#     # Create an instance of SqliteEventQueue using :memory: database
#     yield SqliteEventQueue(":memory:", options=SqliteEventQueue.Options(max_attempts=max_attempts))

# @pytest.mark.asyncio
# async def test_lock_for_processing_success(event_queue : BaseEventQueue):
#     await event_queue.put_events([BotApiChatMemberJoined(), EventData(recv_timestamp=2, event_type='B', event_data_json='"def"')])
#     async with event_queue.get_event_for_processing(timeout=1.0) as event_data:
#         assert event_data == EventData(recv_timestamp=1, event_type='A', event_data_json='"abc"')
#     async with event_queue.get_event_for_processing(timeout=1.0) as event_data:
#         assert event_data == EventData(recv_timestamp=2, event_type='B', event_data_json='"def"')

# class ExpectedException(Exception):
#     pass

# @pytest.mark.asyncio
# async def test_task_skipped_after_max_attempts(event_queue : BaseEventQueue, max_attempts : int):
#     await event_queue.put_events([EventData(recv_timestamp=1, event_type='A', event_data_json='"abc"'), EventData(recv_timestamp=2, event_type='B', event_data_json='"def"')])
#     for i in range(max_attempts):
#         try:
#             async with event_queue.get_event_for_processing(timeout=1.0) as event_data:
#                 assert event_data == EventData(recv_timestamp=1, event_type='A', event_data_json='"abc"')
#                 raise ExpectedException()
#         except ExpectedException:
#             pass
#     # Max attempts reached for task A, it's skipped now.
#     async with event_queue.get_event_for_processing(timeout=1.0) as event_data:
#         assert event_data == EventData(recv_timestamp=2, event_type='B', event_data_json='"def"')
