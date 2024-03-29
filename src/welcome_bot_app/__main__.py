import asyncio
import sqlite3
import time
import datetime
import logging
import argparse
import aiogram.types
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.types import Message
import telethon
from telethon import TelegramClient
from telethon.sessions import StringSession
from dataclasses import dataclass


async def telethon_main(client : TelegramClient, event_queue : asyncio.Queue):
    try:
        async with client:
            @client.on(telethon.events.NewMessage)
            async def new_message_handler(event):
                await event_queue.put(('TELETHON/NEWMSG', time.time(), event))
            @client.on(telethon.events.MessageEdited)
            async def message_edited_handler(event):
                await event_queue.put(('TELETHON/MSGEDIT', time.time(), event))
            @client.on(telethon.events.MessageDeleted)
            async def message_deleted_handler(event):
                await event_queue.put(('TELETHON/MSGDEL', time.time(), event))
            @client.on(telethon.events.ChatAction)
            async def chat_action_handler(event):
                await event_queue.put(('TELETHON/CHATACTION', time.time(), event))
            try:
                await client.run_until_disconnected()
            except asyncio.CancelledError:
                logging.info("telethon_main cancelled")
    except:
        logging.error("Error in telethon_main", exc_info=True)
        raise


async def bot_api_main(bot : Bot, event_queue : asyncio.Queue) -> None:
    try:
        dp = Dispatcher()

        @dp.message()
        async def message_handler(message: Message):
            # TODO: Veriy: if the message handler ends with exception, will Telegram 
            # resend it? If yes, we should probably retry an update a few time, and
            # then log the unrecoverable error and skip the update.
            await event_queue.put(('BOT_API/MSG', time.time(), message))
        try:
            await dp.start_polling(bot)
        except asyncio.CancelledError:
            logging.info("bot_api_main cancelled")
    except:
        logging.error("Error in bot_api_main", exc_info=True)
        raise

class SqliteEventStorage:
    def __init__(self, file_path):
        self._conn = sqlite3.connect(file_path)
        self._initialize_database()

    def _initialize_database(self):
        self._conn.execute('''
                CREATE TABLE IF NOT EXISTS Events (
                    id INTEGER PRIMARY KEY,
                    -- Type of the event.
                    event_type TEXT,
                    -- Timestamp when the event was received.
                    received_timestamp REAL,
                    -- User-readable event representation.
                    event_text TEXT
                )
        ''')

    @staticmethod
    def smart_str(obj, visited = None):
        if visited is None:
            visited = set()
        if id(obj) in visited:
            return '...'
        try:
            visited.add(id(obj))
            def is_not_empty(v):
                return v is not None and v != [] and v != {}
            if hasattr(obj, '__repr_args__'):
                fields_str = ', '.join(f'{k}={SqliteEventStorage.smart_str(v, visited)}' for k, v in obj.__repr_args__() if is_not_empty(v))
                return f'{obj.__class__.__name__}({fields_str})'
            if hasattr(obj, 'to_dict'):
                visited.add(id(obj))
                obj_dict = obj.to_dict()
                if '_' in obj_dict:
                    obj_name = obj_dict['_']
                    obj_fields = ', '.join(f'{k}: {SqliteEventStorage.smart_str(v, visited)}' for k, v in obj_dict.items() if is_not_empty(v) and k != '_')
                    return '%s(%s)' % (obj_name, obj_fields)
                else:
                    obj_fields = ', '.join(f'{k}: {SqliteEventStorage.smart_str(v, visited)}' for k, v in obj_dict.items())
                    return '{%s}' % obj_fields
            elif isinstance(obj, list):
                return '[' + ', '.join(SqliteEventStorage.smart_str(x, visited) for x in obj) + ']'
            elif isinstance(obj, dict):
                if '_' in obj:
                    obj_name = obj['_']
                    obj_fields = ', '.join(f'{k}: {SqliteEventStorage.smart_str(v, visited)}' for k, v in obj.items() if is_not_empty(v) and k != '_')
                    return '%s(%s)' % (obj_name, obj_fields)
                else:
                    obj_fields = ', '.join(f'{k}: {SqliteEventStorage.smart_str(v, visited)}' for k, v in obj.items())
                    return '{%s}' % obj_fields
        finally:
            visited.remove(id(obj))
        return repr(obj)

    def add_event(self, event_type, received_timestamp, event):
        event_text = SqliteEventStorage.smart_str(event)
        try:
            with self._conn:
                self._conn.execute(
                    'INSERT INTO Events (event_type, received_timestamp, event_text) VALUES (?, ?, ?)',
                    (event_type, received_timestamp, event_text)
                )
        except sqlite3.IntegrityError:
            logging.error("Error while adding event %s, with type %r and contents: %s", event_type, type(event), event_text, exc_info=True)

@dataclass
class UserProfile:
    user_id: int
    chat_id: int
    ichbin_message: str
    ichbin_message_timestamp: float
    ichbin_request_timestamp: float

class SqliteUserStorage:
    def __init__(self, file_path):
        self._conn = sqlite3.connect(file_path)
        self._initialize_database()

    def _initialize_database(self):
        self._conn.execute('''
                CREATE TABLE IF NOT EXISTS UserProfiles (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    chat_id INTEGER,
                    ichbin_message TEXT,
                    ichbin_message_timestamp REAL,
                    ichbin_request_timestamp REAL
                )
            ''')
        
    def get_all_profiles(self):
        cursor = self._conn.cursor()
        cursor.execute('SELECT * FROM UserProfiles')
        rows = cursor.fetchall()
        return [UserProfile(user_id = row[1], chat_id = row[2], ichbin_message = row[3], ichbin_message_timestamp = row[4], ichbin_request_timestamp = row[5]) for row in rows]
                           
    def get_profile(self, from_user_id : int, chat_id : int) -> UserProfile:
        cursor = self._conn.cursor()
        cursor.execute('SELECT * FROM UserProfiles WHERE user_id = ? AND chat_id = ?', (from_user_id, chat_id))
        row = cursor.fetchone()
        if row is None:
            return UserProfile(user_id = from_user_id, chat_id = chat_id, ichbin_message = None, ichbin_message_timestamp = None, ichbin_request_timestamp = None)
        return UserProfile(user_id = row[1], chat_id = row[2], ichbin_message = row[3], ichbin_message_timestamp = row[4], ichbin_request_timestamp = row[5])
                           
    def save_profile(self, profile : UserProfile):
        # TODO: Make this a transaction, so that we don't overwrite other updates. Also avoid async code inside transaction!
        # Raises sqlite3 integrity error, if can't save the update.
        with self._conn:
            self._conn.execute(
                'INSERT INTO UserProfiles (user_id, chat_id, ichbin_message, ichbin_message_timestamp, ichbin_request_timestamp) VALUES (?, ?, ?, ?, ?)',
                (profile.user_id, profile.chat_id, profile.ichbin_message, profile.ichbin_message_timestamp, profile.ichbin_request_timestamp)
            )

    def delete_profile(self, profile : UserProfile):
        # TODO: We shouldn't do this!
        with self._conn:
            self._conn.execute(
                'DELETE FROM UserProfiles WHERE user_id = ? AND chat_id = ?',
                (profile.user_id, profile.chat_id)
            )

class EventProcessor:
    def __init__(self, bot : Bot, telethon_client : TelegramClient, event_queue : asyncio.Queue, event_storage : SqliteEventStorage, user_storage : SqliteUserStorage):
        self._bot = bot
        self._telethon_client = telethon_client
        self._event_queue = event_queue
        self._event_storage = event_storage
        self._user_storage = user_storage
        self._periodic_task = asyncio.create_task(self._periodic())

    async def _periodic(self):
        while True:
            logging.info("Periodic task")
            try:
                await self._event_queue.put(('PERIODIC', time.time(), None))
            except asyncio.CancelledError:
                logging.info("periodic cancelled")
                break
            await asyncio.sleep(1)
    
    async def run(self):
        while True:
            try:
                event_type, received_timestamp, event = await self._event_queue.get()
            except asyncio.CancelledError:
                logging.info("EventProcessor cancelled")
                break
            except Exception:
                logging.error("Error in EventProcessor while getting event", exc_info=True)
                continue

            if event_type != 'PERIODIC':
                try:
                    self._event_storage.add_event(event_type, received_timestamp, event)
                except Exception:
                    logging.error("Error while storing event", exc_info=True)
            
            try:
                # TODO: Log all events here?
                if event_type == 'BOT_API/MSG':
                    await self.on_bot_api_message(event)
                elif event_type == 'BOT_API/MSG/NEW_MEMBER':
                    await self.on_bot_api_new_member(event)
                elif event_type == 'TELETHON/NEWMSG':
                    await self.on_telethon_new_message(event)
                elif event_type == 'TELETHON/MSGEDIT':
                    await self.on_telethon_message_edited(event)
                elif event_type == 'TELETHON/MSGDEL':
                    await self.on_telethon_message_deleted(event)
                elif event_type == 'TELETHON/CHATACTION':
                    await self.on_telethon_chat_action(event)
                elif event_type == 'PERIODIC':
                    await self.on_periodic()
                else:
                    logging.error("Unsupported event: %s of type %r and contents %s", event_type, type(event), event)
            except asyncio.CancelledError:
                logging.info("EventProcessor cancelled")
                break
            except Exception:
                logging.critical("Error in EventProcessor while processing event: %s of type %r, and contents: %s", event_type, type(event), event, exc_info=True)
                continue

    # TODO: Can separate into a different class for easier testing.
    async def on_bot_api_message(self, event):
        assert isinstance(event, Message)
        message : Message = event
        if message.content_type == aiogram.types.ContentType.NEW_CHAT_MEMBERS:
            for member in message.new_chat_members:
                try:
                    await self.on_bot_api_new_member(message, member)
                except Exception:
                    logging.critical("Error while processing new member. Message: %s, member: %s", message, member, exc_info=True)
        elif message.content_type == aiogram.types.ContentType.LEFT_CHAT_MEMBER:
            logging.info("Member %s left chat %s", message.left_chat_member.id, message.chat.id)
        elif message.text is not None:
            logging.info("Message from user %s, on chat %s, with text: %s", message.from_user.id, message.chat.id, message.text)
            if '#ichbin' in message.text:
                await self.on_ichbin_message(message)

    # TODO: Actually contains business logic.
    async def on_bot_api_new_member(self, message : Message, member : aiogram.types.User):
        logging.info("New member %s joined chat %s", member.id, message.chat.id)
        user_profile = self._user_storage.get_profile(from_user_id = member.id, chat_id = message.chat.id)
        if user_profile.ichbin_message is not None:
            logging.info("User %s already has an existing #ichbin message, in chat %s.", member.id, message.chat.id)
            return
        if user_profile.ichbin_request_timestamp is not None:
            logging.info("User %s already has an existing #ichbin request timestamp, in chat %s.", member.id, message.chat.id)
            # TODO: If you have like 10 minutes left, bump the timeout to 2 hours, so that people can have time to write about them.
            await self._bot.send_message(message.chat.id, f"Welcome, {member.first_name}! You still have to write the #ichbin, or I'll kick you.")
            return
        user_profile.ichbin_request_timestamp = time.time()
        logging.info("Updating #ichbin request timestamp of user %s in chat %s to %s", member.id, message.chat.id, user_profile.ichbin_request_timestamp)
        self._user_storage.save_profile(user_profile)
        logging.info("Updated #ichbin request timestamp of user %s in chat %s to %s", member.id, message.chat.id, user_profile.ichbin_request_timestamp)
        await self._bot.send_message(message.chat.id, f"Welcome, {member.first_name}! Write #ichbin during the next X days, or I'll kick you out.")
    
    # TODO: Actually contains business logic
    async def on_ichbin_message(self, message : Message):
        logging.info("User %s wrote #ichbin in chat %s", message.from_user.id, message.chat.id)
        user_profile = self._user_storage.get_profile(from_user_id = message.from_user.id, chat_id = message.chat.id)
        if user_profile.ichbin_message is not None:
            logging.info("User %s already has an existing #ichbin message, in chat %s.", message.from_user.id, message.chat.id)
            return
        user_profile.ichbin_message = message.text
        user_profile.ichbin_message_timestamp = message.date.timestamp()
        logging.info("Updating #ichbin message of user %s in chat %s to %s", message.from_user.id, message.chat.id, message.text)
        self._user_storage.save_profile(user_profile)
        logging.info("Successfully updated #ichbin message of user %s in chat %s to %s", message.from_user.id, message.chat.id, message.text)

        logging.info("Sending welcome message to user %s in chat %s as a reply to message %s", message.from_user.id, message.chat.id, message.message_id)
        # TODO: Make sure that we keep only a single ichbin-welcoming message?
        await self._bot.send_message(message.chat.id, f"Welcome, {message.from_user.first_name}! You are now a member of the chat.", reply_to_message_id = message.message_id)
        logging.info("Sent welcome message to user %s in chat %s", message.from_user.id, message.chat.id)

    async def on_telethon_new_message(self, event):
        logging.info("Telethon message from user %s, on chat %s", event.from_id, event.peer_id)

    async def on_telethon_message_edited(self, event):
        logging.info("Message edited from user %s, on chat %s", event.from_id, event.peer_id)

    async def on_telethon_message_deleted(self, event):
        for message_id in event.deleted_ids:
            logging.info("Telethon message %s deleted", message_id)

    async def on_telethon_chat_action(self, event):
        pass

    async def on_periodic(self):
        deleted_profiles = []
        for user_profile in self._user_storage.get_all_profiles():
            if user_profile.ichbin_message_timestamp is not None:
                continue
            if user_profile.ichbin_request_timestamp is None:
                continue
            if time.time() - user_profile.ichbin_request_timestamp > 5:
                logging.info("Kicking user %s from chat %s, as they didn't write #ichbin in time.", user_profile.user_id, user_profile.chat_id)
                # TODO: Adjust timedelta.
                await self._bot.ban_chat_member(user_profile.chat_id, user_profile.user_id, until_date = datetime.timedelta(minutes=5))
                # TODO: Test how the ban above works.
                # TODO: Find out when we should unban.
                # # Unban immediately, so that they could join again.
                # await self._bot.unban_chat_member(user_profile.chat_id, user_profile.user_id, only_if_banned=True)
                deleted_profiles.append(user_profile)
        for user_profile in deleted_profiles:
            # TODO: Probably better to set a kicked_timestamp;
            self._user_storage.delete_profile(user_profile)
                


async def main():
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
    event_queue = asyncio.Queue()
    bot = Bot(bot_token, parse_mode=ParseMode.HTML)
    bot_task = asyncio.create_task(bot_api_main(bot, event_queue = event_queue))
    telethon_client = TelegramClient(
        StringSession(telethon_session_str), telethon_api_id, telethon_api_hash
    )
    telethon_task = asyncio.create_task(
        telethon_main(telethon_client, event_queue=event_queue)
    )

    # TODO: Choose a valid path.
    event_storage = SqliteEventStorage('/tmp/events.db')
    user_storage = SqliteUserStorage('/tmp/users.db')
    event_processor = EventProcessor(bot, telethon_client, event_queue = event_queue, event_storage = event_storage, user_storage = user_storage)
    event_processor_task = asyncio.create_task(event_processor.run())

    all_tasks = [bot_task, telethon_task, event_processor_task]
    done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

if __name__ == "__main__":
    # XXX: aiogram overrides SIGINT, so we rely on aiogram client stopping first, and then kill other tasks.
    asyncio.run(main())


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
