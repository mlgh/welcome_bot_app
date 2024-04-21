import sqlite3
from welcome_bot_app.model import LocalUTCTimestamp
from welcome_bot_app.model.events import BaseEvent
import aiogram
from typing import Any
import logging
import json


def _is_not_empty(v: Any) -> bool:
    return v is not None and v != [] and v != {}


def _jsonify(obj: Any, visited: set[int] | None = None) -> Any:
    if visited is None:
        visited = set()
    if id(obj) in visited:
        return "..."
    try:
        visited.add(id(obj))

        if hasattr(obj, "to_dict"):
            return {
                k: _jsonify(v, visited)
                for k, v in obj.to_dict().items()
                if _is_not_empty(v)
            }
        elif isinstance(obj, dict):
            return {k: _jsonify(v, visited) for k, v in obj.items() if _is_not_empty(v)}
        elif isinstance(obj, list):
            return [_jsonify(elem) for elem in obj]
        else:
            try:
                json.dumps(obj)
            except TypeError:
                return repr(obj)
            else:
                return obj
    finally:
        visited.remove(id(obj))


class EventLog:
    """Simple event log storage."""

    def __init__(self, file_path: str) -> None:
        self._conn = sqlite3.connect(file_path, isolation_level=None)
        self._initialize_database()

    def _initialize_database(self) -> None:
        self._conn.execute("PRAGMA strict=ON")
        self._conn.execute("PRAGMA journal_mode=wal")
        self._conn.execute(
            """
                CREATE TABLE IF NOT EXISTS EventLog (
                    event_id INTEGER PRIMARY KEY NOT NULL,
                    recv_timestamp REAL NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT NOT NULL
                )
            """
        )

    def log_event(
        self, recv_timestamp: LocalUTCTimestamp, event_type: str, event_data: str
    ) -> None:
        try:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                    INSERT INTO EventLog (recv_timestamp, event_type, event_data)
                    VALUES (?, ?, ?)
                """,
                (recv_timestamp, event_type, event_data),
            )
        except Exception:
            logging.error(
                "Failed to log raw Bot API event: event_type: %r, event_data: %r",
                event_type,
                event_data,
                exc_info=True,
            )

    # Below go helper methods for keeping all logging logic in one place.
    def log_bot_api_event(
        self, recv_timestamp: LocalUTCTimestamp, event: aiogram.types.Message
    ) -> None:
        try:
            event_type = "BOT_API/MSG"
            event_data = event.model_dump_json(indent=2, exclude_none=True)
            self.log_event(recv_timestamp, event_type, event_data)
        except Exception:
            logging.error("Failed to log raw Bot API event: %s", event, exc_info=True)

    def log_telethon_event(self, recv_timestamp: LocalUTCTimestamp, event: Any) -> None:
        try:
            jsonified = _jsonify(event)
            if "_" in jsonified:
                event_name = jsonified["_"]
            else:
                event_name = type(event).__name__
            event_type = "TELETHON/" + event_name
            event_data = json.dumps(jsonified, indent=2)
            self.log_event(recv_timestamp, event_type, event_data)
        except Exception:
            logging.error("Failed to log raw Telethon event: %s", event, exc_info=True)

    def log_base_event(
        self, recv_timestamp: LocalUTCTimestamp, event: BaseEvent
    ) -> None:
        try:
            event_type = "BASE/" + type(event).__name__
            event_data = event.model_dump_json(indent=2)
            self.log_event(recv_timestamp, event_type, event_data)
        except Exception:
            logging.error("Failed to log event: %s", event, exc_info=True)
