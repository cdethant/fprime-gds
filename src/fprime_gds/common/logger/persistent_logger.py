"""
persistent_logger.py:

A DataHandlerPlugin that persists decoded F Prime channel telemetry to a local
SQLite database.  Rows are buffered in a thread-safe queue and batch-committed
by a single background worker thread (SQLite connections must stay on one thread).

:author: cdethant
"""

import datetime
import logging
import os
import queue
import sqlite3
import threading
from typing import List, Tuple

from fprime_gds.common.handlers import DataHandlerPlugin
from fprime_gds.plugin.definitions import gds_plugin

LOGGER = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────────
_BATCH_SIZE = 64  # max rows per commit
_POLL_TIMEOUT = 0.5  # seconds the worker waits for new items


@gds_plugin(DataHandlerPlugin)
class PersistentLogger(DataHandlerPlugin):
    """Writes decoded F Prime channel telemetry to a local SQLite database.

    The plugin subscribes to ``FW_PACKET_TELEM`` and stores each channel reading
    as *(timestamp, channel_name, value)*.  Writes are performed asynchronously
    from a daemon thread to avoid blocking the GDS data pipeline.
    """

    @classmethod
    def get_name(cls) -> str:
        return "persistent-database-logger"

    @classmethod
    def get_arguments(cls) -> dict:
        """Expose ``--persistent-db`` on the GDS command line."""
        return {
            ("--persistent-db",): {
                "dest": "persistent_db",
                "type": str,
                "nargs": "?",
                "const": "./db/fprime_telem.db",
                "default": None,
                "help": "Enable persistent telemetry logging, optionally specifying the SQLite database path "
                        "(default path: ./db/fprime_telem.db)",
            },
        }

    def get_handled_descriptors(self) -> List[str]:
        """Subscribe to decoded channel (telemetry) and event data."""
        return ["FW_PACKET_TELEM", "FW_PACKET_LOG"]

    def data_callback(self, data, sender=None) -> None:
        """Enqueue a telemetry or event row for asynchronous persistence.

        Args:
            data: A decoded ``ChData`` or ``EventData`` object.
            sender: Optional sender identifier (unused).
        """
        if getattr(self, '_running', False) is False:
            return

        try:
            if hasattr(data, 'get_val_obj'):  # Telemetry
                self._queue.put_nowait((
                    "telemetry",
                    self._session_id,
                    data.get_time().to_readable(),
                    data.get_template().get_full_name(),
                    str(data.get_val_obj().val),
                ))
            elif hasattr(data, 'get_display_text'):  # Event
                self._queue.put_nowait((
                    "events",
                    self._session_id,
                    data.get_time().to_readable(),
                    data.get_template().get_full_name(),
                    str(data.get_display_text()),
                ))
        except Exception as exc:
            LOGGER.warning("data_callback error (skipping): %s", exc)

    def __init__(self, persistent_db: str = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._running: bool = False
        if not persistent_db:
            LOGGER.info("Persistent logger disabled. Use --persistent-db to enable.")
            return

        self._db_path: str = persistent_db
        self._session_id: str = datetime.datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
        self._queue: queue.Queue = queue.Queue()
        self._running = True

        # Create the database directory if it does not exist
        db_dir = os.path.dirname(self._db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        self._init_schema()

        self._worker = threading.Thread(
            target=self._drain_queue,
            name="PersistentLoggerWorker",
            daemon=True,
        )
        self._worker.start()
        LOGGER.info("started — session ID '%s' writing to '%s'", self._session_id, self._db_path)

    def stop(self) -> None:
        """Flush every queued row and join the worker thread.

        Called during GDS shutdown so the last packets are not lost.
        Safe to call more than once — subsequent calls are no-ops.
        """
        if getattr(self, '_running', False) is False:
            return
        self._running = False
        self._queue.join()
        self._worker.join(timeout=5)
        LOGGER.info("stopped.")

    def _init_schema(self) -> None:
        """Create the ``telemetry`` table if it does not already exist."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id   TEXT    NOT NULL,
                    timestamp    TEXT    NOT NULL,
                    channel_name TEXT    NOT NULL,
                    value        TEXT    NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id   TEXT    NOT NULL,
                    timestamp    TEXT    NOT NULL,
                    event_name   TEXT    NOT NULL,
                    event_text   TEXT    NOT NULL
                )
            """)
            # Check if session_id column exists (migration for existing DBs)
            cursor = conn.execute("PRAGMA table_info(telemetry)")
            columns = [info[1] for info in cursor.fetchall()]
            if "session_id" not in columns:
                LOGGER.info("Migrating database: adding 'session_id' column")
                conn.execute("ALTER TABLE telemetry ADD COLUMN session_id TEXT NOT NULL DEFAULT 'unknown'")
            conn.commit()
        finally:
            conn.close()

    def _drain_queue(self) -> None:
        """Background worker — owns its own SQLite connection.

        Rows are accumulated into a batch and committed together for better
        throughput.  The loop exits once ``_running`` is ``False`` **and** the
        queue is empty.
        """
        conn = sqlite3.connect(self._db_path)
        try:
            batch_telem = []
            batch_events = []
            while self._running or not self._queue.empty():
                try:
                    row = self._queue.get(timeout=_POLL_TIMEOUT)
                    if row[0] == "telemetry":
                        batch_telem.append(row[1:])
                    elif row[0] == "events":
                        batch_events.append(row[1:])
                except queue.Empty:
                    pass

                total_len = len(batch_telem) + len(batch_events)
                if total_len >= _BATCH_SIZE or (
                    total_len > 0 and (not self._running or self._queue.empty())
                ):
                    if batch_telem:
                        self._commit_batch(conn, "telemetry", batch_telem)
                        for _ in range(len(batch_telem)):
                            self._queue.task_done()
                        batch_telem = []
                    if batch_events:
                        self._commit_batch(conn, "events", batch_events)
                        for _ in range(len(batch_events)):
                            self._queue.task_done()
                        batch_events = []
        finally:
            # Flush any remaining rows on unexpected exit
            if batch_telem:
                self._commit_batch(conn, "telemetry", batch_telem)
                for _ in range(len(batch_telem)):
                    self._queue.task_done()
            if batch_events:
                self._commit_batch(conn, "events", batch_events)
                for _ in range(len(batch_events)):
                    self._queue.task_done()
            conn.close()

    @staticmethod
    def _commit_batch(
        conn: sqlite3.Connection, table: str, batch: list
    ) -> None:
        """Write a batch of rows in a single transaction."""
        try:
            if table == "telemetry":
                conn.executemany(
                    "INSERT INTO telemetry (session_id, timestamp, channel_name, value)"
                    " VALUES (?, ?, ?, ?)",
                    batch,
                )
            elif table == "events":
                conn.executemany(
                    "INSERT INTO events (session_id, timestamp, event_name, event_text)"
                    " VALUES (?, ?, ?, ?)",
                    batch,
                )
            conn.commit()
        except Exception as exc:
            LOGGER.error("batch write error (%d rows lost) for %s: %s", len(batch), table, exc)
