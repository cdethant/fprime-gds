"""Test suite for PersistentLogger DataHandlerPlugin.

Exercises the plugin in isolation — no live TCP or F Prime deployment required.
Uses lightweight mock objects that replicate the ChData API surface.

@author ethant
@date   2026-03-23
"""

import os
import sqlite3
import tempfile
import time

import pytest

from fprime_gds.common.logger.persistent_logger import PersistentLogger


# ── Lightweight fakes mirroring ChData's public API ───────────────────────────

class _FakeTime:
    """Mimics ChData.get_time()."""
    def __init__(self, readable: str = "2026-03-23T10:00:00.000"):
        self._readable = readable

    def to_readable(self, tz=None):
        return self._readable


class _FakeTemplate:
    """Mimics ChData.get_template()."""
    def __init__(self, name: str = "battery.voltage"):
        self._name = name

    def get_name(self):
        return self._name


class _FakeValObj:
    """Mimics ChData.get_val_obj()."""
    def __init__(self, val=0.0):
        self.val = val


class _FakeChData:
    """Minimal stand-in for ``fprime_gds.common.data_types.ch_data.ChData``."""
    def __init__(self, name: str = "battery.voltage", val=3.7,
                 time_str: str = "2026-03-23T10:00:00.000"):
        self._time = _FakeTime(time_str)
        self._template = _FakeTemplate(name)
        self._val = _FakeValObj(val)

    def get_time(self):
        return self._time

    def get_template(self):
        return self._template

    def get_val_obj(self):
        return self._val


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def logger(tmp_path):
    """Yield a ``PersistentLogger`` backed by a temporary SQLite database."""
    db_path = str(tmp_path / "test_telem.db")
    lgr = PersistentLogger(persistent_db=db_path)
    yield lgr
    lgr.stop()


@pytest.fixture()
def db_path(logger):
    """Return the database path for the current logger fixture."""
    return logger._db_path


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_get_name():
    """Plugin reports expected name."""
    assert PersistentLogger.get_name() == "persistent-database-logger"


def test_get_handled_descriptors(logger):
    """Plugin subscribes to telemetry only."""
    assert logger.get_handled_descriptors() == ["FW_PACKET_TELEM"]


def test_get_arguments():
    """Plugin exposes --persistent-db argument."""
    args = PersistentLogger.get_arguments()
    keys = [flag for flags in args for flag in flags]
    assert "--persistent-db" in keys


def test_telemetry_write(logger, db_path):
    """Single telemetry row is persisted correctly."""
    pkt = _FakeChData(name="cpu.temp", val=42.5, time_str="2026-03-23T12:00:00.000")
    logger.data_callback(pkt)
    logger.stop()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT timestamp, channel_name, value FROM telemetry").fetchall()
    conn.close()

    assert len(rows) == 1
    ts, ch, val = rows[0]
    assert ts == "2026-03-23T12:00:00.000"
    assert ch == "cpu.temp"
    assert val == "42.5"


def test_batch_commit(logger, db_path):
    """Multiple rows are all flushed after stop()."""
    count = 100
    for i in range(count):
        pkt = _FakeChData(val=round(3.0 + i * 0.01, 2))
        logger.data_callback(pkt)

    logger.stop()

    conn = sqlite3.connect(db_path)
    (n,) = conn.execute("SELECT COUNT(*) FROM telemetry").fetchone()
    conn.close()

    assert n == count, f"Expected {count} rows, got {n}"


def test_stop_flushes(logger, db_path):
    """Calling stop() guarantees every queued row is written."""
    for i in range(5):
        pkt = _FakeChData(val=float(i))
        logger.data_callback(pkt)

    logger.stop()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT value FROM telemetry ORDER BY id").fetchall()
    conn.close()

    assert [r[0] for r in rows] == [str(float(i)) for i in range(5)]


def test_schema_created(logger, db_path):
    """The telemetry table is created on construction."""
    conn = sqlite3.connect(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='telemetry'"
    ).fetchall()
    conn.close()
    assert len(tables) == 1
