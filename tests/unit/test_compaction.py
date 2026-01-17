import pytest
from src.storage_engine.memtable.skiplist import SkipList
from src.storage_engine.sstable.writer import SSTableWriter
from src.storage_engine.sstable.reader import SSTableReader
from src.storage_engine.compaction.merger import Compactor

def test_compaction_merges_and_deduplicates(tmp_path):
    """
    Scenario:
    File 1 (Old): set("user:1", "Alice"), set("user:2", "Bob")
    File 2 (New): set("user:1", "Alice_Updated"), set("user:3", "Charlie")
    
    Result should be:
    "user:1" -> "Alice_Updated" (Overwrite)
    "user:2" -> "Bob" (Preserved)
    "user:3" -> "Charlie" (New)
    """
    
    # 1. Create File 1
    mem1 = SkipList()
    mem1.insert("user:1", "Alice")
    mem1.insert("user:2", "Bob")
    path1 = str(tmp_path / "1.sst")
    SSTableWriter().write(mem1, path1)
    
    # 2. Create File 2
    mem2 = SkipList()
    mem2.insert("user:1", "Alice_Updated")
    mem2.insert("user:3", "Charlie")
    path2 = str(tmp_path / "2.sst")
    SSTableWriter().write(mem2, path2)
    
    # 3. Run Compaction
    out_path = str(tmp_path / "merged.sst")
    compactor = Compactor()
    compactor.merge([path1, path2], out_path)
    
    # 4. Verify Result
    reader = SSTableReader(out_path)
    
    # Check overwrite
    assert reader.search("user:1") == "Alice_Updated"
    # Check preservation
    assert reader.search("user:2") == "Bob"
    # Check new data
    assert reader.search("user:3") == "Charlie"
    
    reader.close()
