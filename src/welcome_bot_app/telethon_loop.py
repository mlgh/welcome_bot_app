import logging
import asyncio
import time
import telethon
from datetime import timezone
from welcome_bot_app.model import (
    UserChatId,
    TelethonMessageId,
    TelethonUTCTimestamp,
    LocalUTCTimestamp,
)
from welcome_bot_app.model.events import TelethonNewTextMessage
from welcome_bot_app.event_log import EventLog
from welcome_bot_app.event_queue import BaseEventQueue


async def telethon_main(
    client: telethon.TelegramClient,
    event_queue: BaseEventQueue,
    event_log: EventLog,
) -> None:
    try:
        async with client:

            @client.on(telethon.events.NewMessage)  # type: ignore
            async def new_message_handler(telethon_event) -> None:
                recv_timestamp = LocalUTCTimestamp(time.time())
                event_log.log_telethon_event(recv_timestamp, telethon_event)
                if not isinstance(telethon_event.from_id, telethon.types.PeerUser):
                    return
                user_id = telethon_event.from_id.user_id
                chat_id = None
                if isinstance(telethon_event.peer_id, telethon.types.PeerUser):
                    # This is a private message to the bot user account, we should simply ignore this.
                    return
                elif isinstance(telethon_event.peer_id, telethon.types.PeerChat):
                    chat_id = telethon_event.peer_id.chat_id
                    # Aiogram, or bot API represent this number as negative.
                    assert chat_id >= 0
                    chat_id *= -1
                else:
                    return
                assert chat_id is not None
                user_chat_id = UserChatId(user_id=user_id, chat_id=chat_id)
                if telethon_event.message.message is None:
                    return
                event = TelethonNewTextMessage(
                    recv_timestamp=recv_timestamp,
                    user_chat_id=user_chat_id,
                    text=telethon_event.message.message,
                    message_id=TelethonMessageId(telethon_event.message.id),
                    tg_timestamp=TelethonUTCTimestamp(
                        telethon_event.date.astimezone(timezone.utc).timestamp()
                    ),
                )
                event_log.log_base_event(recv_timestamp, event)
                await event_queue.put_events([event])

            # TODO: Handle other types of updates.
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
