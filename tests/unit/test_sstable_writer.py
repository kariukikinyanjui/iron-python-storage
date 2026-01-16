import struct
import pytest
from src.storage_engine.memtable.skiplist import SkipList
from src.storage_engine.sstable.writer import SSTableWriter


def test_sstable_write_preserves_order(tmp_path):
    """
    Verifies that the writer respects the MemTable'sorted order
    when flushing to disk.
    """

    # 1. Populate MemTable with random inserts
    mem = SkipList()
    mem.insert("charlie", "data-c")
    mem.insert("alice", "data-a")
    mem.insert("bob", "data-b")

    # 2. Flush to disk
    sst_path = tmp_path / "output.sst"
    writer = SSTableWriter()
    writer.write(mem, str(sst_path))

    # 3. Read back raw bytes to verify order
    # Format: [len][alice][len][val] ...
    with open(sst_path, "rb") as f:
        # Helper to read a string based on 4-byte length prefix
        def read_str():
            len_bytes = f.read(4)
            if not len_bytes: return None
            length = struct.unpack('>I', len_bytes)[0]
            return f.read(length).decode('utf-8')

        # First key must be "alice"
        k1 = read_str()
        assert k1 == "alice"
        _ = read_str() # skip value

        # Second key must be "bob"
        k2 = read_str()
        assert k2 == "bob"
        _ = read_str()

        # Third key must be "charlie"
        k3 = read_str()
        assert k3 == "charlie"
