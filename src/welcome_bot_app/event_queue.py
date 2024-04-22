from abc import ABC, abstractmethod
import sqlite3
import contextlib
from pydantic import BaseModel
from enum import Enum
from typing import AsyncIterator, List, NewType, Any, Generator
import logging
import time
import asyncio
from welcome_bot_app.model import LocalUTCTimestamp
from welcome_bot_app.model.events import BaseEvent, BaseEventSubclass

EventId = NewType("EventId", int)


class EventStateEnum(str, Enum):
    # Initially the task is in NEW state.
    NEW = "NEW"
    # Then it is acquired for processing - IN_PROGRESS state.
    IN_PROGRESS = "IN_PROGRESS"
    # If processing is successful, it is marked as DONE.
    DONE = "DONE"
    # If processing fails multiple times, it is marked as ERROR
    ERROR = "ERROR"


class EventProcessingAttempt(BaseModel):
    start_timestamp: LocalUTCTimestamp
    finish_timestamp: LocalUTCTimestamp | None
    error: str | None


class EventProcessingAttempts(BaseModel):
    attempts: List[EventProcessingAttempt]


class EventData(BaseModel):
    recv_timestamp: LocalUTCTimestamp
    event_type: str
    event_json: str


class EventExecutionState(BaseModel):
    state: EventStateEnum
    state_update_timestamp: LocalUTCTimestamp
    attempts: EventProcessingAttempts


class EventRow(BaseModel):
    event_id: EventId
    event_data: EventData
    execution_state: EventExecutionState


class BaseEventQueue(ABC):
    @abstractmethod
    @contextlib.asynccontextmanager
    async def get_event_for_processing(
        self, timeout: float
    ) -> AsyncIterator[BaseEvent | None]:
        # XXX: Hack to silence mypy.
        if False:
            yield

    @abstractmethod
    async def put_events(self, event_datas: List[BaseEvent]) -> None:
        pass


class SqliteEventQueue(BaseEventQueue):
    class Options(BaseModel):
        # Check Events table every `get_new_event_timeout` irregardless of whether we are notified about new events.
        get_new_event_timeout: float = 1.0
        # Maximum number of retries for processing the event. After that the event is marked as ERROR.
        max_attempts: int = 1

    class EventAcquireFailure(Exception):
        """Raised when we failed to acquire the event for processing."""

        pass

    class EventReleaseFailure(Exception):
        """Raised when we failed to release the event after processing."""

        pass

    def __init__(self, db_path: str, options: Options):
        self.db_path = db_path
        self._options = options
        # Set isolation_level to None, so that we could roll our own transactions.
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._create_table()

        # Condition variable to notify about new events.
        self._new_events_available_cond = asyncio.Condition()
        # Flag to indicate that new events are available. Guarded by self._new_events_available_cond
        self._new_events_available = False

    def _create_table(self) -> None:
        self._conn.execute("PRAGMA strict=ON")
        self._conn.execute("""CREATE TABLE IF NOT EXISTS Events (
                          -- Unique event id.
                          event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                          -- The timestamp when the event was received.
                          recv_timestamp REAL NOT NULL,
                          -- Type of this event.
                          event_type TEXT NOT NULL,
                          -- JSON representation of the BaseEvent.
                          event_json TEXT NOT NULL,
                          -- State of the event processing. corresponds to `EventStateEnum` type.
                          state TEXT NOT NULL,
                          state_update_timestamp REAL NOT NULL,
                          -- JSON
                          attempts_json TEXT NOT NULL
                       )""")
        self._conn.execute(
            f'''CREATE INDEX IF NOT EXISTS idx_NewEvents ON Events (state, recv_timestamp ASC) WHERE state = "{EventStateEnum.NEW}"'''
        )

    def _parse_event_row(self, row: Any) -> EventRow:
        return EventRow(
            event_id=row[0],
            event_data=EventData(
                recv_timestamp=row[1], event_type=row[2], event_json=row[3]
            ),
            execution_state=EventExecutionState(
                state=row[4],
                state_update_timestamp=row[5],
                attempts=EventProcessingAttempts.model_validate_json(row[6]),
            ),
        )

    def _get_new_event_ids(self, limit: int) -> List[EventId]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT event_id FROM events WHERE state = ? ORDER BY recv_timestamp ASC LIMIT ?",
            (EventStateEnum.NEW, limit),
        )
        rows = cursor.fetchall()
        # XXX: Breakage point here on broken attempts_json.
        return [row[0] for row in rows]

    def _acquire_event(self, event_id: EventId) -> EventRow:
        """Update the state of the event to IN_PROGRESS ONLY if it is in NEW state.

        Returns True, if the event was acquired successfully, False otherwise."""
        cursor = self._conn.cursor()
        cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            update_timestamp = LocalUTCTimestamp(time.time())
            cursor.execute(
                "UPDATE events SET state = ?, state_update_timestamp = ? WHERE event_id = ? AND state = ?",
                (
                    EventStateEnum.IN_PROGRESS,
                    update_timestamp,
                    event_id,
                    EventStateEnum.NEW,
                ),
            )
            if cursor.rowcount == 0:
                raise SqliteEventQueue.EventAcquireFailure(
                    f"Failed to acquire the event {event_id!r} for processing"
                )
            cursor.execute(
                "SELECT event_id, recv_timestamp, event_type, event_json, state, state_update_timestamp, attempts_json FROM events WHERE event_id = ?",
                (event_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise SqliteEventQueue.EventAcquireFailure(
                    f"Event not found after acquiring: {event_id!r}"
                )
            event_row = self._parse_event_row(row)
            if event_row.execution_state.state != EventStateEnum.IN_PROGRESS:
                raise SqliteEventQueue.EventAcquireFailure(
                    f"Event {event_id!r} state is not IN_PROGRESS after acquiring: {event_row.execution_state.state}"
                )
            if event_row.execution_state.state_update_timestamp != update_timestamp:
                raise SqliteEventQueue.EventAcquireFailure(
                    f"Event {event_id!r} state_update_timestamp mismatch after acquiring: {event_row.execution_state.state_update_timestamp} != {update_timestamp}"
                )
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        else:
            cursor.execute("COMMIT")
            return event_row

    def _release_event(
        self, event_id: EventId, event_execution_state: EventExecutionState
    ) -> bool:
        """Update the state of the event to DONE or ERROR from IN_PROGRESS, depending on the event processing result."""
        update_timestamp = LocalUTCTimestamp(time.time())
        cursor = self._conn.cursor()
        cursor.execute(
            """
            UPDATE events
            SET state = ?, state_update_timestamp = ?, attempts_json = ?
            WHERE event_id = ? AND state = ?""",
            (
                event_execution_state.state,
                update_timestamp,
                event_execution_state.attempts.model_dump_json(),
                event_id,
                EventStateEnum.IN_PROGRESS,
            ),
        )
        if cursor.rowcount == 0:
            # This is quite an unexpected situation, as we should have locked the event before processing it.
            return False
        return True

    @contextlib.contextmanager
    def _lock_for_processing(
        self, event_id: EventId
    ) -> Generator[EventData, None, None]:
        start_timestamp = None
        execution_state = None
        try:
            event_row = self._acquire_event(event_id)
            execution_state = event_row.execution_state
            event_data = event_row.event_data
            del event_row
            start_timestamp = LocalUTCTimestamp(time.time())
            yield event_data
        except Exception as exc:
            if execution_state is None:
                raise
            if start_timestamp is not None:
                execution_state.attempts.attempts.append(
                    EventProcessingAttempt(
                        start_timestamp=start_timestamp,
                        finish_timestamp=LocalUTCTimestamp(time.time()),
                        error=repr(exc),
                    )
                )
            # XXX: It's probably not a good idea to retry multiple times in a row without even some back-off strategy, but we want:
            # 1. To skip the event in case of error
            # 2. Process all events in order
            if len(execution_state.attempts.attempts) >= self._options.max_attempts:
                execution_state.state = EventStateEnum.ERROR
            else:
                execution_state.state = EventStateEnum.NEW
            try:
                self._release_event(event_id, event_execution_state=execution_state)
            except Exception:
                logging.critical(
                    "Failed to release the event %r after error",
                    event_id,
                    exc_info=True,
                )
            raise
        else:
            execution_state.attempts.attempts.append(
                EventProcessingAttempt(
                    start_timestamp=start_timestamp,
                    finish_timestamp=LocalUTCTimestamp(time.time()),
                    error=None,
                )
            )
            execution_state.state = EventStateEnum.DONE
            self._release_event(event_id, event_execution_state=execution_state)

    @contextlib.asynccontextmanager
    async def get_event_for_processing(
        self, timeout: float
    ) -> AsyncIterator[BaseEvent | None]:
        """Waits at most timeout seconds for the next event and acquires it for processing."""
        while True:
            new_event_ids = self._get_new_event_ids(1)
            logging.debug("get_new_events: %r", new_event_ids)
            if new_event_ids:
                event_id = new_event_ids[0]
                try:
                    with self._lock_for_processing(event_id) as event_data:
                        event = BaseEventSubclass.validate_json(event_data.event_json)
                        assert isinstance(event, BaseEvent)
                        yield event
                except SqliteEventQueue.EventAcquireFailure:
                    # The event was acquired by someone else. It shouldn't appear in the get_new_events in the next iteration.
                    continue
                else:
                    return
            try:
                # No events currently in the table.
                async with self._new_events_available_cond:
                    await asyncio.wait_for(
                        self._new_events_available_cond.wait_for(
                            lambda: self._new_events_available
                        ),
                        timeout=self._options.get_new_event_timeout,
                    )
                    self._new_events_available = False
            except TimeoutError:
                yield None
                break

    async def put_events(self, events: List[BaseEvent]) -> None:
        """Returns event_id of the newly inserted event."""
        state_update_timestamp = time.time()
        attempts = EventProcessingAttempts(attempts=[])
        cursor = self._conn.cursor()
        cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            for event in events:
                cursor.execute(
                    "INSERT INTO events (recv_timestamp, event_type, event_json, state, state_update_timestamp, attempts_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event.recv_timestamp,
                        event.event_type,
                        event.model_dump_json(indent=2),
                        EventStateEnum.NEW,
                        state_update_timestamp,
                        attempts.model_dump_json(indent=2),
                    ),
                )
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        else:
            cursor.execute("COMMIT")
        async with self._new_events_available_cond:
            self._new_events_available = True
            self._new_events_available_cond.notify()
