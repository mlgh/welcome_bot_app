import sqlite3
import logging
from typing import Any, List
from welcome_bot_app.model.user_profile import (
    BotApiMessage,
    BotApiMessageType,
    PresenceInfo,
    UserProfile,
    UserProfileParams,
)
from welcome_bot_app.model import (
    BotApiMessageId,
    ChatId,
    UserChatId,
    LocalUTCTimestamp,
    UserId,
)


class SqliteUserStorage:
    def __init__(self, file_path: str, log_statements: bool = False) -> None:
        self._conn = sqlite3.connect(file_path)
        self._initialize_database(log_statements)

    def _trace_callback(self, statement: str) -> None:
        logging.info("SQL: %s", statement)

    def _initialize_database(self, log_statements: bool) -> None:
        if log_statements:
            self._conn.set_trace_callback(self._trace_callback)
        self._conn.execute("PRAGMA strict=ON")
        # User storage
        self._conn.execute("""
                CREATE TABLE IF NOT EXISTS UserProfiles (
                    id INTEGER PRIMARY KEY NOT NULL,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    -- User profile
                    user_profile_json TEXT,
                    -- local timestamp at which the user should be kicked.
                    kick_at_timestamp REAL
                )
            """)
        self._conn.execute("""
                -- Add index on (user_id, chat_id)
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_chat ON UserProfiles (user_id, chat_id);
            """)
        self._conn.execute("""
                -- Add index of users that should be kicked soon.
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_to_kick_soon
                           ON UserProfiles (kick_at_timestamp ASC, user_id, chat_id)
                           WHERE kick_at_timestamp is not NULL;
            """)
        # Tracks bot messages that need to be deleted.
        self._conn.execute("""
                CREATE TABLE IF NOT EXISTS BotMessages (
                    id INTEGER PRIMARY KEY NOT NULL,
                    -- User id to whom the message was related.
                    user_id INTEGER NOT NULL,
                    -- Chat id to in which the message was sent.
                    chat_id INTEGER NOT NULL,
                    -- Message id.
                    message_id INTEGER NOT NULL,
                    -- Type of the message (e.g. WELCOME, ICHBIN_REQUEST, etc), corresponding to BotApiMessageType enum.
                    message_type STRING NOT NULL,
                    -- Time when this message was sent.
                    sent_timestamp REAL NOT NULL,
                    -- Time when we (or someone else) deleted the message.
                    delete_timestamp REAL
                )
            """)
        # Index for existing messages per user, should not be large.
        self._conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_bot_messages_per_user
                           ON BotMessages (user_id, chat_id)
                           WHERE delete_timestamp IS NULL;
            """)
        # Index for existing messages per user+message_id, should not be large.
        self._conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_messages
                           ON BotMessages (message_id, user_id, chat_id)
                           WHERE delete_timestamp IS NULL;
            """)

    def _bot_message_from_row(
        self,
        user_id: Any,
        chat_id: Any,
        message_id: Any,
        message_type: Any,
        sent_timestamp: Any,
    ) -> BotApiMessage:
        # TODO: Handle exceptions during conversion?
        return BotApiMessage(
            user_chat_id=UserChatId(user_id=UserId(user_id), chat_id=ChatId(chat_id)),
            message_id=BotApiMessageId(message_id),
            message_type=BotApiMessageType(message_type),
            sent_timestamp=LocalUTCTimestamp(sent_timestamp),
        )

    def get_bot_messages(self) -> List[BotApiMessage]:
        """Returns bot messages related to the given user."""
        cursor = self._conn.cursor()
        cursor.execute(
            """
                       SELECT user_id, chat_id, message_id, message_type, sent_timestamp
                       FROM BotMessages
                       WHERE delete_timestamp is NULL;
                       """
        )
        return [self._bot_message_from_row(*row) for row in cursor.fetchall()]

    def mark_bot_message_as_deleted(
        self,
        user_chat_id: UserChatId,
        message_id: BotApiMessageId,
        delete_timestamp: LocalUTCTimestamp,
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                        UPDATE BotMessages
                        SET delete_timestamp = ?
                        WHERE message_id = ? AND user_id = ? AND chat_id = ? AND delete_timestamp IS NULL;
            """,
                (
                    delete_timestamp,
                    message_id,
                    user_chat_id.user_id,
                    user_chat_id.chat_id,
                ),
            )

    def add_bot_message(self, bot_api_message: BotApiMessage) -> None:
        with self._conn:
            self._conn.execute(
                """
                        INSERT OR IGNORE INTO BotMessages
                            (message_id, user_id, chat_id, message_type, sent_timestamp)
                        VALUES (?, ?, ?, ?, ?);
                       """,
                (
                    bot_api_message.message_id,
                    bot_api_message.user_chat_id.user_id,
                    bot_api_message.user_chat_id.chat_id,
                    bot_api_message.message_type.value,
                    bot_api_message.sent_timestamp,
                ),
            )

    def get_users_to_kick(
        self, current_timestamp: LocalUTCTimestamp
    ) -> List[UserChatId]:
        cursor = self._conn.cursor()
        cursor.execute(
            """
                       SELECT user_id, chat_id
                       FROM UserProfiles
                       WHERE kick_at_timestamp IS NOT NULL
                            AND kick_at_timestamp <= ?;
                       """,
            (current_timestamp,),
        )
        rows = cursor.fetchall()
        return [UserChatId(user_id=row[0], chat_id=row[1]) for row in rows]

    def get_profile(self, user_chat_id: UserChatId) -> UserProfile:
        cursor = self._conn.cursor()
        cursor.execute(
            """
                       SELECT user_profile_json
                       FROM UserProfiles
                       WHERE user_id = ? AND chat_id = ?;
                       """,
            (user_chat_id.user_id, user_chat_id.chat_id),
        )
        row = cursor.fetchone()
        user_profile_json = None if row is None else row[0]
        if user_profile_json is None:
            return UserProfile(user_chat_id=user_chat_id, presence_info=PresenceInfo())
        return UserProfile.model_validate_json(user_profile_json)

    def save_profile(
        self, profile: UserProfile, user_profile_params: UserProfileParams
    ) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO UserProfiles
                    (user_id, chat_id, user_profile_json, kick_at_timestamp)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (user_id, chat_id)
                DO UPDATE SET
                    user_profile_json = excluded.user_profile_json,
                    kick_at_timestamp = excluded.kick_at_timestamp;
                """,
                (
                    profile.user_chat_id.user_id,
                    profile.user_chat_id.chat_id,
                    profile.model_dump_json(indent=2),
                    profile.get_kick_at_timestamp(user_profile_params),
                ),
            )
