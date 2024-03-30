import sqlite3
from typing import List
from welcome_bot_app.model import UserKey, UserProfile


class SqliteUserStorage:
    def __init__(self, file_path):
        self._conn = sqlite3.connect(file_path)
        self._initialize_database()

    def _initialize_database(self):
        self._conn.execute("""
                CREATE TABLE IF NOT EXISTS UserProfiles (
                    id INTEGER PRIMARY KEY NOT NULL,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    ichbin_message TEXT,
                    ichbin_message_timestamp REAL,
                    ichbin_message_id INTEGER,
                    ichbin_request_timestamp REAL,
                    kicked_timestamp REAL
                )
            """)
        self._conn.execute("""
                -- Add index on (user_id, chat_id)
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_chat ON UserProfiles (user_id, chat_id);
            """)
        self._conn.execute("""
                -- Add index to fetch records with the smallest non-null ichbin_request_timestamp where ichbin_message_timestamp is null
                CREATE UNIQUE INDEX IF NOT EXISTS users_to_kick
                           ON UserProfiles (ichbin_request_timestamp ASC, user_id, chat_id)
                           WHERE ichbin_request_timestamp IS NOT NULL
                                AND ichbin_message_timestamp IS NULL AND kicked_timestamp IS NULL;
            """)

    def get_users_to_kick(self, max_ichbin_request_timestamp) -> List[UserKey]:
        cursor = self._conn.cursor()
        cursor.execute(
            """
                       SELECT user_id, chat_id
                       FROM UserProfiles
                       WHERE ichbin_request_timestamp IS NOT NULL
                            AND ichbin_message_timestamp IS NULL
                            AND kicked_timestamp IS NULL
                            AND ichbin_request_timestamp <= ?
                       """,
            (max_ichbin_request_timestamp,),
        )
        rows = cursor.fetchall()
        return [UserKey(user_id=row[0], chat_id=row[1]) for row in rows]

    def get_profile(self, user_key: UserKey) -> UserProfile:
        cursor = self._conn.cursor()
        cursor.execute(
            """
                       SELECT ichbin_message,
                              ichbin_message_timestamp,
                              ichbin_message_id,
                              ichbin_request_timestamp,
                              kicked_timestamp
                       FROM UserProfiles
                       WHERE user_id = ? AND chat_id = ?
                       """,
            (user_key.user_id, user_key.chat_id),
        )
        row = cursor.fetchone()
        result = UserProfile(user_key=user_key)
        if row is not None:
            result.ichbin_message, result.ichbin_message_timestamp, result.ichbin_message_id, result.ichbin_request_timestamp, result.kicked_timestamp = row
        return result

    def save_profile(self, profile: UserProfile):
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO UserProfiles
                    (user_id, chat_id, ichbin_message, ichbin_message_timestamp, ichbin_message_id, ichbin_request_timestamp, kicked_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (user_id, chat_id)
                DO UPDATE SET
                    ichbin_message=excluded.ichbin_message,
                    ichbin_message_timestamp=excluded.ichbin_message_timestamp,
                    ichbin_message_id=excluded.ichbin_message_id,
                    ichbin_request_timestamp=excluded.ichbin_request_timestamp,
                    kicked_timestamp=excluded.kicked_timestamp
                """,
                (
                    profile.user_key.user_id,
                    profile.user_key.chat_id,
                    profile.ichbin_message,
                    profile.ichbin_message_timestamp,
                    profile.ichbin_message_id,
                    profile.ichbin_request_timestamp,
                    profile.kicked_timestamp,
                ),
            )
