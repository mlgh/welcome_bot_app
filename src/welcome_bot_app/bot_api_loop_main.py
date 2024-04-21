"""A runner script to debug receiving messages from Telegram Bot API"""

import asyncio
import logging
import argparse
from aiogram import Bot
from aiogram.enums import ParseMode

from welcome_bot_app import bot_api_loop
from welcome_bot_app.event_queue import SqliteEventQueue
from welcome_bot_app.event_log import EventLog


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
        "--event-queue-file", help="Path to the event queue file", required=True
    )
    parser.add_argument(
        "--event-log-file", help="Path to the event log file", required=True
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s:%(lineno)s %(levelname)s - %(message)s",
    )
    logging.info("Starting bot")

    with open(args.bot_token_file, "r") as f:
        bot_token = f.read().strip()
    bot = Bot(bot_token, parse_mode=ParseMode.HTML)

    event_queue = SqliteEventQueue(
        db_path=args.event_queue_file, options=SqliteEventQueue.Options()
    )
    event_log = EventLog(args.event_log_file)
    await bot_api_loop.bot_api_main(bot, event_queue, event_log)


if __name__ == "__main__":
    asyncio.run(main())
