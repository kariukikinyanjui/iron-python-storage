import os
import pytest
from src.storage_engine.engine import StorageEngine

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "data")

def test_engine_write_path_integration(db_path):
    """
    Verifies that 'put' writes to BOTH the MemTable and the WAL file.
    """
    engine = StorageEngine(db_path)

    # 1. Write data
    engine.put("user:101", "Alice")

    # 2. Check RAM (MemTable)
    assert engine.get("user:101") == "Alice"

    # 3. Check Disk (WAL File)
    # The file should exist and contain the data
    assert os.path.exists(engine.wal_path)
    with open(engine.wal_path, "rb") as f:
        content = f.read()
        assert b"user:101" in content
        assert b"Alice" in content

    engine.close()

def test_engine_data_persistence_scenario(db_path):
    """
    Simulates a 'crash' by closing the engine and checking artifacts.
    (Note: Full recovery logic isn't implemented yet, but the data must be on disk)>
    """
    # 1. Start Engine and Write
    engine = StorageEngine(db_path)
    engine.put("config:mode", "production")
    engine.close() # Simulate shutdown

    # 2. Verify the WAL file persists after shutdown
    wal_path = os.path.join(db_path, "recovery.wal")
    assert os.path.getsize(wal_path) > 0
