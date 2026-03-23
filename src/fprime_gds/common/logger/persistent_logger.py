import queue
import sqlite3
import threading

from fprime_gds.common.handlers import DataHandlerPlugin
from fprime_gds.plugin.definitions import gds_plugin

@gds_plugin(DataHandlerPlugin)
class PersistentLogger(DataHandlerPlugin):
    """
    Writes decoded F Prime telemetry to a local SQLite database. 
    """

    @classmethod
    def get_name(cls):
        return "persistent-database-logger"

    def get_handled_descriptors(self) -> list[str]:
        """
        Tell the GDS which data streams we want to subscribe to.
        FW_PACKET_TELEM = Telemetry (Channels)
        """
        # TODO: FW_PACKET_LOG = Events

        return ["FW_PACKET_TELEM"]

    def data_callback(self, data, source=None):
        try:
            self._queue.put_nowait((
                data.get_time().to_readable(),
                data.get_template().get_name(),
                str(data.get_val_obj().val),
            ))
        except Exception as exc:
            print(f"[PersistentLogger] data_callback error (skipping): {exc}")

    def __init__(self, db_path: str = "fprime_telem.db"):
        import queue as _queue
        import sqlite3 as _sqlite3
        import threading as _threading
 
        self._db_path = db_path
        self._queue = _queue.Queue()
        self._running = True
 
        self._init_schema(_sqlite3)
 
        self._worker = _threading.Thread(
            target=self._drain_queue,
            name="PersistentLoggerWorker",
            daemon=True,
        )
        self._worker.start()
        print(f"[PersistentLogger] started — writing to '{self._db_path}'")
 
    def stop(self):
        """
        Flush all queued rows and stop the worker.
        Hook into GDS shutdown so the last packets are never dropped.
        """
        self._running = False
        self._queue.join()
        self._worker.join(timeout=5)
        print("[PersistentLogger] stopped.")

    def _init_schema(self, sqlite3):
        """Create the telemetry table once, then close the connection."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp    TEXT NOT NULL,
                    channel_name TEXT NOT NULL,
                    value        TEXT NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()
 
    def _drain_queue(self):
        """
        Background worker — creates and owns its own SQLite connection.
        (SQLite connections must not be shared across threads.)
        """
        import sqlite3
        conn = sqlite3.connect(self._db_path)
        try:
            while self._running or not self._queue.empty():
                try:
                    row = self._queue.get(timeout=0.5)
                    conn.execute(
                        "INSERT INTO telemetry (timestamp, channel_name, value)"
                        " VALUES (?, ?, ?)",
                        row,
                    )
                    conn.commit()
                    self._queue.task_done()
                except queue.Empty:
                    continue
                except Exception as exc:
                    print(f"[PersistentLogger] write error: {exc}")
                    self._queue.task_done()
        finally:
            conn.close()




# ---------------------------------------------------------------------------
# non-live testing
# ---------------------------------------------------------------------------

class _FakeTime:
    def to_readable(self): return "2026-03-23T10:00:00.000"
 
class _FakeTemplate:
    def get_name(self): return "battery.voltage"
 
class _FakeVal:
    val = 0.0
 
class _FakeChData:
    """Mirrors the ChData API surface used by data_callback."""
    def get_time(self):     return _FakeTime()
    def get_template(self): return _FakeTemplate()
    def get_val_obj(self):  return _FakeVal()
 
 
if __name__ == "__main__":
    import os, time
 
    DB = "smoke_test.db"

    # Clear old entries for testing
    if os.path.exists(DB):
        os.remove(DB)
 
    # Instantiate directly, bypassing the @gds_plugin decorator
    # (decorator is a no-op at runtime; it only registers with pluggy)
    logger = object.__new__(PersistentLogger)
    PersistentLogger.__init__(logger, db_path=DB)
 
    print("Pushing 5 fake telemetry packets...")
    for i in range(5):
        pkt = _FakeChData()
        pkt.get_val_obj().val = round(3.7 + i * 0.1, 2)
        logger.data_callback(pkt)
        time.sleep(0.02)
 
    logger.stop()
 
    conn = sqlite3.connect(DB)
    rows = conn.execute("SELECT * FROM telemetry").fetchall()
    conn.close()
 
    print(f"\nRows written to '{DB}':")
    for r in rows:
        print(f"  {r}")
 
    assert len(rows) == 5, f"Expected 5 rows, got {len(rows)}"
    print("\ntest PASSED.")
