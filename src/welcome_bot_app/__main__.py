import asyncio
import logging
import argparse
from aiogram import Bot
from aiogram.enums import ParseMode
from telethon import TelegramClient
from telethon.sessions import StringSession

from welcome_bot_app import bot_api_loop
from welcome_bot_app import telethon_loop
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.event_storage import SqliteEventStorage
from welcome_bot_app.user_storage import SqliteUserStorage


async def main() -> None:
    parser = argparse.ArgumentParser(description="Welcoming Telegram bot.")
    parser.add_argument(
        "--log-level",
        help="Set the logging level.",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
    )
    parser.add_argument(
        "--bot-token-file",
        help="Path to the file containing the bot token",
        required=True,
    )
    parser.add_argument(
        "--telethon-api-id-file", help="File with API ID for Telethon", required=True
    )
    parser.add_argument(
        "--telethon-api-hash-file",
        help="File with API Hash for Telethon",
        required=True,
    )
    parser.add_argument(
        "--telethon-session-file",
        help="File with session string for Telethon",
        required=True,
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s:%(lineno)s %(levelname)s - %(message)s",
    )
    logging.info("Starting bot")

    with open(args.bot_token_file, "r") as f:
        bot_token = f.read().strip()
    with open(args.telethon_api_id_file, "r") as f:
        telethon_api_id = int(f.read().strip())
    with open(args.telethon_api_hash_file, "r") as f:
        telethon_api_hash = f.read().strip()
    with open(args.telethon_session_file, "r") as f:
        telethon_session_str = f.read().strip()
    bot = Bot(bot_token, parse_mode=ParseMode.HTML)
    telethon_client = TelegramClient(
        StringSession(telethon_session_str), telethon_api_id, telethon_api_hash
    )

    # TODO: Choose a valid path.
    event_storage = SqliteEventStorage("/tmp/events.db")
    user_storage = SqliteUserStorage("/tmp/users.db")
    event_processor = EventProcessor(
        bot, telethon_client, event_storage=event_storage, user_storage=user_storage
    )

    bot_task = asyncio.create_task(
        bot_api_loop.bot_api_main(bot, event_processor, event_storage=event_storage)
    )
    telethon_task = asyncio.create_task(
        telethon_loop.telethon_main(
            telethon_client, event_processor, event_storage=event_storage
        )
    )
    event_processor_task = asyncio.create_task(event_processor.run())

    all_tasks = [bot_task, telethon_task, event_processor_task]
    _, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    # XXX: aiogram overrides SIGINT, so we rely on aiogram client stopping first, and then kill other tasks.
    asyncio.run(main())
