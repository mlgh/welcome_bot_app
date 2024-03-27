import sys
import asyncio
import logging
import argparse
import sqlite3
from os import getenv
from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.types import Message
from aiogram.utils.markdown import hbold
from aiogram.filters import CommandStart
from aiogram.utils.formatting import Text, TextMention

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Welcoming Telegram bot.")
    parser.add_argument("--log-level", help="Set the logging level.", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO")
    parser.add_argument("--bot-token-file", help="Path to the file containing the bot token", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format='%(asctime)s - %(name)s %(levelname)s - %(message)s')
    logging.info("Starting bot")

    with open(args.bot_token_file, 'r') as f:
        bot_token = f.read().strip()

    logging.info("Bot token is %s", bot_token)

# # All handlers should be attached to the Router (or Dispatcher)
# dp = Dispatcher()

# conn = sqlite3.connect('bot_database.db')

# def initialize_database(conn : sqlite3.Connection):
#     conn.execute('''
#             CREATE TABLE IF NOT EXISTS UsersInLimbo (
#                 id INTEGER PRIMARY KEY,
#                 -- Name of the user
#                 first_name TEXT,
#                 -- User.id, the user who joined, little-endian uint64.
#                 user_id INTEGER,
#                 -- Chat.id, where the user joined, little-endian uint64.
#                 chat_id INTEGER
#             )
#     ''')

# def add_user(conn : sqlite3.Connection, first_name : str, user_id : int, chat_id : int):
#     try:
#         with conn:
#             conn.execute(
#                 'INSERT INTO UsersInLimbo (first_name, user_id, chat_id) VALUES (?, ?, ?)',
#                 (first_name, user_id, chat_id)
#             )
#     except sqlite3.IntegrityError:
#         logging.error("Error while adding user", exc_info=True)
#         return
#     for row in conn.execute('SELECT * FROM UsersInLimbo'):
#         logging.info(row)

# @dp.message(CommandStart())
# async def command_start_handler(message: Message) -> None:
#     """
#     This handler receives messages with `/start` command
#     """
#     # Most event objects have aliases for API methods that can be called in events' context
#     # For example if you want to answer to incoming message you can use `message.answer(...)` alias
#     # and the target chat will be passed to :ref:`aiogram.methods.send_message.SendMessage`
#     # method automatically or call API method directly via
#     # Bot instance: `bot.send_message(chat_id=message.chat.id, ...)`
#     await message.answer(f"Hello, {hbold(message.from_user.full_name)}!")


# @dp.message()
# async def echo_handler(message: types.Message) -> None:
#     """
#     Handler will forward receive a message back to the sender

#     By default, message handler will handle all message types (like a text, photo, sticker etc.)
#     """
#     user = message.from_user
#     logging.info("Message from user %r", user.id)
#     try:
#         if message.text.startswith('backup '):
#             fname = message.text[len('backup '):]
#             dst_conn = None
#             try:
#                 dst_conn = sqlite3.connect(fname)
#                 def progress(status, remaining, total):
#                     logging.info(f'Copied {total-remaining} of {total} pages.... Status is {status}')
#                 with dst_conn:
#                     conn.backup(dst_conn, pages=10, progress=progress)
#             finally:
#                 if dst_conn is not None:
#                     dst_conn.close()
#             return

#         content = Text("Hello, ", TextMention(user.first_name, user=user), "! If you don't write #ololo, I will kick you out.")
#         await message.reply(**content.as_kwargs())
#         add_user(conn, message.from_user.first_name, message.from_user.id, message.chat.id)
#     except TypeError:
#         logging.error("Error while sending reply", exc_info=True)
#         # But not all the types is supported to be copied so need to handle it
#         await message.answer("Nice try!")

# def NewChatMembersFilter(message: types.Message) -> bool:
#     return message.content_type == types.ContentType.NEW_CHAT_MEMBERS

# @dp.message(NewChatMembersFilter)
# async def on_new_chat_members(message: types.Message):
#     new_members = message.new_chat_members
#     for member in new_members:
#         # You can access member information like member.id, member.first_name, etc.
#         # Perform any actions you want with the new member
#         await message.reply(f"Welcome, {member.first_name}!")

# async def tick(bot: Bot):
#     pass

# async def periodic(bot: Bot, sleep_for: int = 1):
#     while True:
#         logging.info("Periodic task")
#         try:
#             await tick(bot)
#         except Exception:
#             logging.error("Error in periodic task", exc_info=True)
#         await asyncio.sleep(sleep_for)

# async def main() -> None:
#     # Initialize Bot instance with a default parse mode which will be passed to all API calls
#     bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
#     initialize_database(conn)
#     asyncio.create_task(periodic(bot, 5))
#     # And the run events dispatching
#     await dp.start_polling(bot)

# if __name__ == "__main__":
#     # Create logger
#     logger = logging.root
#     logger.setLevel(logging.DEBUG)

#     # Create formatter
#     formatter = logging.Formatter('%(asctime)s - %(name)s %(levelname)s - %(message)s')

#     # Create a stream handler to output to stderr
#     stream_handler = logging.StreamHandler(sys.stderr)
#     stream_handler.setLevel(logging.DEBUG)
#     stream_handler.setFormatter(formatter)

#     # Add handlers to the logger
#     logger.addHandler(stream_handler)

#     asyncio.run(main())