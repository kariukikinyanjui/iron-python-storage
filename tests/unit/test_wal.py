import os
import pytest
from unittest.mock import MagicMock, patch
from src.storage_engine.wal.logger import WALLogger


@pytest.fixture
def wal_file(tmp_path):
    """Creates a temporaty path for the WAL file."""
    return tmp_path / "test.wal"

def test_wal_append_writes_data(wal_file):
    """Verify data is physically wirtten to the file."""
    wal = WALLogger(str(wal_file))
    wal.append("key1", "value1")
    wal.close()

    # Read raw bytes back to verify our binary format
    with open(str(wal_file), "rb") as f:
        data = f.read()

    # Check for presence of data (basic check)
    assert b"key1" in data
    assert b"value" in data

def test_wal_strict_durability_calls_fsync(wal_file):
    """
    Critical Test: Ensure os.fsync is called when fsync=True
    This confirms we are bypassing the OS cache for safety.
    """
    with patch("os.fsync") as mock_fsync:
        wal = WALLogger(str(wal_file))

        # Append with strict durability
        wal.append("k", "v", fsync=True)

        # Verify fsync was called on the file descriptor
        mock_fsync.assert_called_once_with(wal.file.fileno())
        wal.close()

def test_wal_batched_durability_skips_fsync(wal_file):
    """Test that we can write FAST without forcing a disk seek every time."""
    with patch("os.fsync") as mock_fsync:
        wal = WALLogger(str(wal_file))

        # Append with loose durability
        wal.append("k", "v", fsync=False)

        mock_fsync.assert_not_called()
        wal.close()
