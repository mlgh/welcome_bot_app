import logging
import asyncio
import telethon
from welcome_bot_app.event_processor import EventProcessor


async def telethon_main(
    client: telethon.TelegramClient, event_processor: EventProcessor
):
    try:
        while True:
            # TODO: Think if we actually need this. It turns out that bot and client have different message_id's for same messages. So it could be of limited use...
            await asyncio.sleep(1)
        # async with client:
        #     @client.on(telethon.events.NewMessage)
        #     async def new_message_handler(event):
        #         await event_queue.put(('TELETHON/NEWMSG', time.time(), event))
        #     @client.on(telethon.events.MessageEdited)
        #     async def message_edited_handler(event):
        #         await event_queue.put(('TELETHON/MSGEDIT', time.time(), event))
        #     @client.on(telethon.events.MessageDeleted)
        #     async def message_deleted_handler(event):
        #         await event_queue.put(('TELETHON/MSGDEL', time.time(), event))
        #     @client.on(telethon.events.ChatAction)
        #     async def chat_action_handler(event):
        #         await event_queue.put(('TELETHON/CHATACTION', time.time(), event))
        #     try:
        #         await client.run_until_disconnected()
        #     except asyncio.CancelledError:
        #         logging.info("telethon_main cancelled")
    except:
        logging.error("Error in telethon_main", exc_info=True)
        raise
