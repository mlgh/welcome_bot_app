import sqlalchemy as sa
import sqlite3
import sqlalchemy.event as sa_event
from typing import Any, List
from welcome_bot_app.model.chat_settings import BotReplyType, ChatSettings
from welcome_bot_app.model.events import BotApiChatInfo
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from welcome_bot_app.model.user_profile import (
    BotApiMessage,
    PresenceInfo,
    UserProfile,
    UserProfileParams,
    UserChatCapabilities,
)
from welcome_bot_app.model import (
    BotApiMessageId,
    ChatId,
    UserChatId,
    LocalUTCTimestamp,
    UserId,
)


class BotStorage:
    def __init__(self, storage_url: str, enable_echo: bool = False) -> None:
        self._engine = sa.create_engine(storage_url, echo=enable_echo)
        if self._engine.driver != "pysqlite":
            raise NotImplementedError(
                "We are using sqlite_where for indexes.", self._engine.driver
            )
        sa_event.listen(self._engine, "connect", self._set_conn_pragmas)

        self._sa_metadata = sa.MetaData()
        # Tracks users that need to write ichbin.
        self._user_profiles = sa.Table(
            "UserProfiles",
            self._sa_metadata,
            sa.Column(
                "id", sa.Integer, primary_key=True, autoincrement=True, nullable=False
            ),
            sa.Column("user_id", sa.Integer, nullable=False),
            sa.Column("chat_id", sa.Integer, nullable=False),
            sa.Column("user_profile_json", sa.Text),
            sa.Column("kick_at_timestamp", sa.Float),
        )
        sa.Index(
            "idx_user_chat",
            self._user_profiles.c.user_id,
            self._user_profiles.c.chat_id,
            unique=True,
        )
        sa.Index(
            "idx_chat",
            self._user_profiles.c.chat_id,
        )
        sa.Index(
            "idx_users_to_kick_soon",
            self._user_profiles.c.kick_at_timestamp.asc(),
            sqlite_where=(self._user_profiles.c.kick_at_timestamp.is_not(None)),
        )
        # Tracks bot messages that need to be deleted.
        self._bot_messages = sa.Table(
            "BotMessages",
            self._sa_metadata,
            sa.Column("id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("user_id", sa.Integer, nullable=False),
            sa.Column("chat_id", sa.Integer, nullable=False),
            sa.Column("message_id", sa.Integer, nullable=False),
            sa.Column("reply_type", sa.String, nullable=False),
            sa.Column("sent_timestamp", sa.Float, nullable=False),
            sa.Column("delete_timestamp", sa.Float),
        )
        sa.Index(
            "idx_bot_messages_per_user",
            self._bot_messages.c.user_id,
            self._bot_messages.c.chat_id,
            sqlite_where=(self._bot_messages.c.delete_timestamp.is_(None)),
        )
        sa.Index(
            "idx_bot_messages",
            self._bot_messages.c.message_id,
            self._bot_messages.c.user_id,
            self._bot_messages.c.chat_id,
            unique=True,
            sqlite_where=(self._bot_messages.c.delete_timestamp.is_(None)),
        )
        self._bot_chats = sa.Table(
            "BotChats",
            self._sa_metadata,
            sa.Column("chat_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("chat_info", sa.Text, nullable=False),
        )
        self._chat_settings = sa.Table(
            "ChatSettings",
            self._sa_metadata,
            sa.Column("chat_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("chat_settings", sa.Text, nullable=False),
        )
        self._user_chat_capabilities = sa.Table(
            "UserChatCapabilities",
            self._sa_metadata,
            sa.Column("user_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("chat_id", sa.Integer, primary_key=True, nullable=False),
            sa.Column("capabilities_json", sa.Text, nullable=False),
        )
        self._sa_metadata.create_all(self._engine)

    def _set_conn_pragmas(self, dbapi_con: sqlite3.Connection, con_record: Any) -> None:
        dbapi_con.execute("PRAGMA journal_mode=WAL")

    def add_chat(self, chat_id: ChatId, chat_info: BotApiChatInfo) -> None:
        with self._engine.connect() as conn:
            insert_stmt = (
                sqlite_insert(self._bot_chats)
                .prefix_with("OR IGNORE")
                .values(chat_id=chat_id, chat_info=chat_info.model_dump_json(indent=2))
            )
            conn.execute(
                insert_stmt.on_conflict_do_update(
                    index_elements=[self._bot_chats.c.chat_id],
                    set_=dict(chat_info=insert_stmt.excluded.chat_info),
                ),
            )
            conn.commit()

    def get_chats(self) -> dict[ChatId, BotApiChatInfo]:
        with self._engine.connect() as conn:
            result = conn.execute(self._bot_chats.select())
            return {
                ChatId(row.chat_id): BotApiChatInfo.model_validate_json(row.chat_info)
                for row in result
            }

    def remove_chat(self, chat_id: ChatId) -> None:
        with self._engine.connect() as conn:
            conn.execute(
                self._bot_chats.delete().where(self._bot_chats.c.chat_id == chat_id)
            )
            conn.commit()

    def get_chat_settings(self, chat_id: ChatId) -> ChatSettings:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._chat_settings.select().where(
                    self._chat_settings.c.chat_id == chat_id
                )
            )
            row = result.fetchone()
            if row is None:
                return ChatSettings.get_default()
            return ChatSettings.model_validate_json(row.chat_settings)

    def set_chat_settings(self, chat_id: ChatId, chat_settings: ChatSettings) -> None:
        with self._engine.connect() as conn:
            insert_stmt = sqlite_insert(self._chat_settings).values(
                chat_id=chat_id, chat_settings=chat_settings.model_dump_json(indent=2)
            )
            conn.execute(
                insert_stmt.on_conflict_do_update(
                    index_elements=[self._chat_settings.c.chat_id],
                    set_=dict(chat_settings=insert_stmt.excluded.chat_settings),
                ),
            )
            conn.commit()

    def _bot_message_from_row(self, row: sa.Row[Any]) -> BotApiMessage:
        return BotApiMessage(
            user_chat_id=UserChatId(
                user_id=UserId(row.user_id), chat_id=ChatId(row.chat_id)
            ),
            message_id=BotApiMessageId(row.message_id),
            reply_type=BotReplyType(row.reply_type),
            sent_timestamp=LocalUTCTimestamp(row.sent_timestamp),
        )

    def get_bot_messages(self) -> List[BotApiMessage]:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._bot_messages.select().where(
                    self._bot_messages.c.delete_timestamp.is_(None)
                )
            )
            return [self._bot_message_from_row(row) for row in result]

    def mark_bot_message_as_deleted(
        self,
        user_chat_id: UserChatId,
        message_id: BotApiMessageId,
        delete_timestamp: LocalUTCTimestamp,
    ) -> None:
        with self._engine.connect() as conn:
            conn.execute(
                self._bot_messages.update()
                .where(
                    sa.and_(
                        self._bot_messages.c.message_id == message_id,
                        self._bot_messages.c.user_id == user_chat_id.user_id,
                        self._bot_messages.c.chat_id == user_chat_id.chat_id,
                        self._bot_messages.c.delete_timestamp.is_(None),
                    )
                )
                .values(delete_timestamp=delete_timestamp)
            )
            conn.commit()

    def add_bot_message(self, bot_api_message: BotApiMessage) -> None:
        with self._engine.connect() as conn:
            conn.execute(
                self._bot_messages.insert().values(
                    user_id=bot_api_message.user_chat_id.user_id,
                    chat_id=bot_api_message.user_chat_id.chat_id,
                    message_id=bot_api_message.message_id,
                    reply_type=bot_api_message.reply_type.value,
                    sent_timestamp=bot_api_message.sent_timestamp,
                )
            )
            conn.commit()

    def get_users_to_kick(
        self, current_timestamp: LocalUTCTimestamp
    ) -> List[UserChatId]:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._user_profiles.select().where(
                    sa.and_(
                        self._user_profiles.c.kick_at_timestamp.is_not(None),
                        self._user_profiles.c.kick_at_timestamp <= current_timestamp,
                    )
                )
            )
            return [
                UserChatId(user_id=row.user_id, chat_id=row.chat_id) for row in result
            ]

    def get_profile(self, user_chat_id: UserChatId) -> UserProfile:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._user_profiles.select().where(
                    sa.and_(
                        self._user_profiles.c.user_id == user_chat_id.user_id,
                        self._user_profiles.c.chat_id == user_chat_id.chat_id,
                    )
                )
            )
            row = result.fetchone()
            if row is None:
                return UserProfile(
                    user_chat_id=user_chat_id, presence_info=PresenceInfo()
                )
            return UserProfile.model_validate_json(row.user_profile_json)

    def save_profile(
        self, profile: UserProfile, user_profile_params: UserProfileParams
    ) -> None:
        with self._engine.connect() as conn:
            insert_stmt = sqlite_insert(self._user_profiles).values(
                user_id=profile.user_chat_id.user_id,
                chat_id=profile.user_chat_id.chat_id,
                user_profile_json=profile.model_dump_json(indent=2),
                kick_at_timestamp=profile.get_kick_at_timestamp(user_profile_params),
            )
            conn.execute(
                insert_stmt.on_conflict_do_update(
                    index_elements=[
                        self._user_profiles.c.user_id,
                        self._user_profiles.c.chat_id,
                    ],
                    set_=dict(
                        user_profile_json=insert_stmt.excluded.user_profile_json,
                        kick_at_timestamp=insert_stmt.excluded.kick_at_timestamp,
                    ),
                ),
            )
            conn.commit()

    def get_chat_user_profiles(self, chat_id: ChatId) -> List[UserProfile]:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._user_profiles.select().where(
                    self._user_profiles.c.chat_id == chat_id
                )
            )
            return [
                UserProfile.model_validate_json(row.user_profile_json) for row in result
            ]

    def get_user_chat_capabilities(
        self, user_id: UserId, chat_id: ChatId
    ) -> UserChatCapabilities:
        with self._engine.connect() as conn:
            result = conn.execute(
                self._user_chat_capabilities.select().where(
                    sa.and_(
                        self._user_chat_capabilities.c.user_id == user_id,
                        self._user_chat_capabilities.c.chat_id == chat_id,
                    )
                )
            )
            row = result.fetchone()
            if row is None:
                return UserChatCapabilities()
            return UserChatCapabilities.model_validate_json(row.capabilities_json)

    def set_user_chat_capabilities(
        self,
        user_id: UserId,
        chat_id: ChatId,
        UserChatCapabilities: UserChatCapabilities,
    ) -> None:
        with self._engine.connect() as conn:
            insert_stmt = sqlite_insert(self._user_chat_capabilities).values(
                user_id=user_id,
                chat_id=chat_id,
                capabilities_json=UserChatCapabilities.model_dump_json(indent=2),
            )
            conn.execute(
                insert_stmt.on_conflict_do_update(
                    index_elements=[
                        self._user_chat_capabilities.c.user_id,
                        self._user_chat_capabilities.c.chat_id,
                    ],
                    set_=dict(capabilities_json=insert_stmt.excluded.capabilities_json),
                ),
            )
            conn.commit()
