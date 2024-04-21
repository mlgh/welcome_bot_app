"""A runner script to debug receiving messages from Telethon MTProto API"""

import asyncio
import logging
import argparse
from telethon import TelegramClient
from telethon.sessions import StringSession

from welcome_bot_app import telethon_loop
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

    with open(args.telethon_api_id_file, "r") as f:
        telethon_api_id = int(f.read().strip())
    with open(args.telethon_api_hash_file, "r") as f:
        telethon_api_hash = f.read().strip()
    with open(args.telethon_session_file, "r") as f:
        telethon_session_str = f.read().strip()
    telethon_client = TelegramClient(
        StringSession(telethon_session_str), telethon_api_id, telethon_api_hash
    )

    event_queue = SqliteEventQueue(
        db_path=args.event_queue_file, options=SqliteEventQueue.Options()
    )
    event_log = EventLog(args.event_log_file)
    await telethon_loop.telethon_main(telethon_client, event_queue, event_log)


if __name__ == "__main__":
    asyncio.run(main())
