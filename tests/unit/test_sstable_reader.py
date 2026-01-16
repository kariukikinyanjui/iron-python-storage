import pytest
from src.storage_engine.memtable.skiplist import SkipList
from src.storage_engine.sstable.writer import SSTableWriter
from src.storage_engine.sstable.reader import SSTableReader


def test_sstable_reader_finds_data(tmp_path):
    """
    End-to-End Test for the SSTable module.
    1. Write data using Writer.
    2. Read data using Reader (mmap).
    """
    # 1. Create content
    sst_path = tmp_path / "data.sst"
    mem = SkipList()
    mem.insert("key1", "value1")
    mem.insert("key2", "value2") # Sorted order is key1, key2

    writer = SSTableWriter()
    writer.write(mem, str(sst_path))

    # 2. Read back
    reader = SSTableReader(str(sst_path))

    # Verify hits
    assert reader.search("key1") == "value1"
    assert reader.search("key2") == "value2"

    # Verify miss
    assert reader.search("key3") is None

    reader.close()
