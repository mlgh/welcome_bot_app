import logging
import asyncio
import time
import telethon
from datetime import timezone
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.event_storage import SqliteEventStorage
from welcome_bot_app.model import UserKey, TelethonNewMessage


async def telethon_main(
    client: telethon.TelegramClient,
    event_processor: EventProcessor,
    event_storage: SqliteEventStorage,
) -> None:
    try:
        async with client:

            @client.on(telethon.events.NewMessage)  # type: ignore
            async def new_message_handler(event) -> None:
                local_timestamp = time.time()
                event_storage.log_raw_telethon_event(event, local_timestamp)
                if not isinstance(event.from_id, telethon.types.PeerUser):
                    return
                user_id = event.from_id.user_id
                chat_id = None
                if isinstance(event.peer_id, telethon.types.PeerUser):
                    # This is a private message to the bot user account, we should simply ignore this.
                    return
                elif isinstance(event.peer_id, telethon.types.PeerChat):
                    chat_id = event.peer_id.chat_id
                    # Aiogram, or bot API represent this number as negative.
                    assert chat_id >= 0
                    chat_id *= -1
                else:
                    return
                assert chat_id is not None
                user_key = UserKey(user_id=user_id, chat_id=chat_id)
                if event.message.message is None:
                    return
                await event_processor.put_event(
                    TelethonNewMessage(
                        local_timestamp=local_timestamp,
                        user_key=user_key,
                        text=event.message.message,
                        message_id=event.message.id,
                        tg_timestamp=event.date.astimezone(timezone.utc).timestamp(),
                    )
                )

            # @client.on(telethon.events.MessageEdited)
            # async def message_edited_handler(event):
            #     await event_queue.put(('TELETHON/MSGEDIT', time.time(), event))
            # @client.on(telethon.events.MessageDeleted)
            # async def message_deleted_handler(event):
            #     await event_queue.put(('TELETHON/MSGDEL', time.time(), event))
            # @client.on(telethon.events.ChatAction)
            # async def chat_action_handler(event):
            #     await event_queue.put(('TELETHON/CHATACTION', time.time(), event))
            try:
                await client.run_until_disconnected()
            except asyncio.CancelledError:
                logging.info("telethon_main cancelled")
    except:
        logging.error("Error in telethon_main", exc_info=True)
        raise
