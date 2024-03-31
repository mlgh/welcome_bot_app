import logging
import sqlite3
from typing import Any, Mapping
import aiogram.types
from welcome_bot_app.model import Event


def _full_type_name(obj : Any) -> str:
    return type(obj).__module__ + "." + type(obj).__qualname__


def _smart_str(obj : Any, visited : set[int] | None = None) -> str:
    if visited is None:
        visited = set()
    if id(obj) in visited:
        return "..."
    try:
        visited.add(id(obj))

        def is_not_empty(v : Any) -> bool:
            return v is not None and v != [] and v != {}

        if hasattr(obj, "__repr_args__"):
            fields_str = ", ".join(
                f"{k}={_smart_str(v, visited)}"
                for k, v in obj.__repr_args__()
                if is_not_empty(v)
            )
            return f"{obj.__class__.__name__}({fields_str})"
        if hasattr(obj, "to_dict"):
            visited.add(id(obj))
            obj_dict = obj.to_dict()
            if "_" in obj_dict:
                obj_name = obj_dict["_"]
                obj_fields = ", ".join(
                    f"{k}: {_smart_str(v, visited)}"
                    for k, v in obj_dict.items()
                    if is_not_empty(v) and k != "_"
                )
                return "%s(%s)" % (obj_name, obj_fields)
            else:
                obj_fields = ", ".join(
                    f"{k}: {_smart_str(v, visited)}" for k, v in obj_dict.items()
                )
                return "{%s}" % obj_fields
        elif isinstance(obj, list):
            return "[" + ", ".join(_smart_str(x, visited) for x in obj) + "]"
        elif isinstance(obj, dict):
            if "_" in obj:
                obj_name = obj["_"]
                obj_fields = ", ".join(
                    f"{k}: {_smart_str(v, visited)}"
                    for k, v in obj.items()
                    if is_not_empty(v) and k != "_"
                )
                return "%s(%s)" % (obj_name, obj_fields)
            else:
                obj_fields = ", ".join(
                    f"{k}: {_smart_str(v, visited)}" for k, v in obj.items()
                )
                return "{%s}" % obj_fields
    finally:
        visited.remove(id(obj))
    return repr(obj)


def _bot_api_msg_to_str(event: aiogram.types.Message) -> str:
    return _smart_str(event)


class SqliteEventStorage:
    def __init__(self, file_path : str):
        self._conn = sqlite3.connect(file_path)
        self._initialize_database()

    def _initialize_database(self) -> None:
        self._conn.execute("PRAGMA strict=ON")
        self._conn.execute("""
                CREATE TABLE IF NOT EXISTS Events (
                    id INTEGER PRIMARY KEY,
                    -- Type of the event.
                    event_type TEXT,
                    -- Timestamp when the event was received.
                    local_timestamp REAL,
                    -- User-readable event representation.
                    event_text TEXT
                )
        """)

    def _add_event(self, event_type: str, local_timestamp: float, event_text: str) -> None:
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT INTO Events (event_type, local_timestamp, event_text) VALUES (?, ?, ?)",
                    (event_type, local_timestamp, event_text),
                )
        except sqlite3.IntegrityError:
            logging.error(
                "Error while adding event (%r, %r, %r)",
                event_type,
                local_timestamp,
                event_text,
                exc_info=True,
            )

    def log_raw_bot_api_event(
        self, event: aiogram.types.Message, local_timestamp: float
    ) -> None:
        try:
            self._add_event(
                _full_type_name(event), local_timestamp, _bot_api_msg_to_str(event)
            )
        except Exception:
            logging.error("Failed to log raw Bot API event: %s", event, exc_info=True)

    def log_raw_telethon_event(self, event : Any, local_timestamp: float) -> None:
        try:
            self._add_event(
                _full_type_name(event), local_timestamp, _bot_api_msg_to_str(event)
            )
        except Exception:
            logging.error("Failed to log raw Telethon event: %s", event, exc_info=True)

    def log_event(self, event: Event) -> None:
        try:
            self._add_event(_full_type_name(event), event.local_timestamp, str(event))
        except Exception:
            logging.error("Failed to log event: %s", event, exc_info=True)
