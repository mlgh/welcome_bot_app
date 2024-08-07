import asyncio
import logging
import signal
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from telethon import TelegramClient

from welcome_bot_app import bot_api_loop
from welcome_bot_app import telethon_loop
from welcome_bot_app.event_processor import EventProcessor
from welcome_bot_app.event_queue import SqliteEventQueue
from welcome_bot_app.bot_storage import BotStorage
from welcome_bot_app import args

args.parser().add_argument(
    "--log-level",
    help="Set the logging level.",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="INFO",
)
args.parser().add_argument(
    "--bot-token-file",
    help="Path to the file containing the bot token",
    required=True,
)
args.parser().add_argument(
    "--telethon-api-id-file", help="File with API ID for Telethon"
)
args.parser().add_argument(
    "--telethon-api-hash-file",
    help="File with API Hash for Telethon",
)
args.parser().add_argument(
    "--telethon-session-file-prefix",
    help="Path prefix for Telethon session file.",
)
args.parser().add_argument(
    "--event-queue-file", help="Path to the event queue file", required=True
)
args.parser().add_argument(
    "--event-log-file", help="Path to the event log file", required=True
)
args.parser().add_argument(
    "--storage-url",
    help="SqlAlchemy URL where data about users, chats will be stored.",
    required=True,
)
args.parser().add_argument(
    "--log-sql", help="Enable SqlAlchemy commands.", action="store_true"
)


async def main() -> None:
    logging.basicConfig(
        level=getattr(logging, args.args().log_level),
        format="%(asctime)s - %(name)s:%(lineno)s %(levelname)s - %(message)s",
    )
    logging.info("Starting bot")

    with open(args.args().bot_token_file, "r") as f:
        bot_token = f.read().strip()
    bot = Bot(bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    telethon_client = None
    if (
        args.args().telethon_api_id_file
        or args.args().telethon_api_hash_file
        or args.args().telethon_session_file_prefix
    ):
        with open(args.args().telethon_api_id_file, "r") as f:
            telethon_api_id = int(f.read().strip())
        with open(args.args().telethon_api_hash_file, "r") as f:
            telethon_api_hash = f.read().strip()
        telethon_client = TelegramClient(
            args.args().telethon_session_file_prefix, telethon_api_id, telethon_api_hash
        )

    event_queue = SqliteEventQueue(
        db_path=args.args().event_queue_file, options=SqliteEventQueue.Options()
    )
    event_log = bot_api_loop.EventLog(args.args().event_log_file)
    bot_storage = BotStorage(args.args().storage_url, enable_echo=args.args().log_sql)
    event_processor = EventProcessor(
        EventProcessor.Config(),
        bot,
        telethon_client,
        event_queue=event_queue,
        bot_storage=bot_storage,
    )

    all_tasks = []

    all_tasks.append(
        asyncio.create_task(
            bot_api_loop.bot_api_main(bot, event_queue=event_queue, event_log=event_log)
        )
    )
    all_tasks.append(asyncio.create_task(event_processor.run()))

    if telethon_client is not None:
        all_tasks.append(
            asyncio.create_task(
                telethon_loop.telethon_main(
                    telethon_client, event_queue=event_queue, event_log=event_log
                )
            )
        )

    def handle_stop_signal() -> None:
        asyncio.create_task(event_processor.stop())

    asyncio.get_event_loop().add_signal_handler(signal.SIGINT, handle_stop_signal)
    asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, handle_stop_signal)

    _, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
