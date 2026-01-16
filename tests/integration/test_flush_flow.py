import pytest
import os
from src.storage_engine.engine import StorageEngine


def test_engine_auto_flush(tmp_path):
    """
    Verifies that the engine automatically flushes to disk when full,
    and that data remains readable afterwards.
    """
    db_path = str(tmp_path / "data")
    # Set max size to 2 items to force frequent flushes.
    engine = StorageEngine(db_path, memtable_max_size=2)

    # 1. Insert 2 items (MemTable is now full)
    engine.put("key1", "val1")
    engine.put("key2", "val2")

    # 2. Insert 3rd item -> Triggers Flush!
    # "key1" and "key2" should move to .sst file
    # "key3" should be in new MemTable
    engine.put("key3", "val3")

    # 3. Verify files exist
    sst_files = [f for f in os.listdir(db_path) if f.endswith(".sst")]
    assert len(sst_files) >= 1

    # 4. Verify we can read ALL data
    assert engine.get("key1") == "val1" # From Disk (SSTable)
    assert engine.get("key2") == "val2" # From Disk (SSTable)
    assert engine.get("key3") == "val3" # From RAM (MemTable)

    engine.close()
