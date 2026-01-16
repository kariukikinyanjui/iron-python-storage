import os
import glob
import time
from typing import Optional, List
from src.storage_engine.memtable.skiplist import SkipList
from src.storage_engine.wal.logger import WALLogger
from src.storage_engine.sstable.writer import SSTableWriter
from src.storage_engine.sstable.reader import SSTableReader


class StorageEngine:
    """
    The main entry point for the database.
    Coordinates the MemTable (RAM) and WAL (Disk).
    """
    def __init__(self, dir_path: str = "data", memtable_max_size: int = 3):
        self.dir_path = dir_path
        self.memtable_max_size = memtable_max_size
        os.makedirs(dir_path, exist_ok=True)

        # Active components
        self.memtable = SkipList()
        self.wal_path = os.path.join(dir_path, "recovery.wal")
        self.wal = WALLogger(self.wal_path)

        # 2. Immutable Components (SSTables)
        # In a real reboot scenario, we would load existing .sst files here.
        self.sst_readers: List[SSTableReader] = []

    def put(self, key: str, value: str) -> None:
        """
        Writes data. Flushes to disk if MemTable is full.
        """
        self.wal.append(key, value, fsync=True)
        self.memtable.insert(key, value)

        # Check size threshold (simplification: using node count instead of bytes)
        # In production, we would track estimated byte size.
        if self._get_memtable_size() >= self.memtable_max_size:
            self.flush()

    def get(self, key: str) -> Optional[str]:
        """Read Path: MemTable -> SSTable (Newest -> Oldest)"""
        # 1. Check Volatile Memory
        val = self.memtable.search(key)
        if val is not None:
            return val

        # 2. Check Disk (Immutable SSTable)
        # Iterate in reverse to find the most recent version of the key
        for reader in reversed(self.sst_readers):
            val = reader.search(key)
            if val is not None:
                return val

            return None

    def flush(self) -> None:
        """freezes MemTable -> writes to SSTable -> clears MemTable"""
        if self._get_memtable_size() == 0:
            return

        # 1. Generate filename (timestamp based)
        timestamp = int(time.time() * 1000)
        filename = f"{timestamp}.sst"
        filepath = os.path.join(self.dir_path, filename)

        # 2. Write to disk
        writer = SSTableWriter()
        writer.write(self.memtable, filepath)

        # 3. Open a reader for the new file and add to list
        reader = SSTableReader(filepath)
        self.sst_readers.append(reader)

        # 4. Clear MemTable and WAL
        # (In production, we would truncate the WAL here)
        self.memtable = SkipList()
        # Re-open WAL to clear it
        self.wal.close()
        open(self.wal_path, 'w').close()
        self.wal = WALLogger(self.wal_path)

    def _get_memtable_size(self):
        """Helper to count nodes in SkipList (O(N) for now, usually 0(1) with counter)."""
        count = 0
        for _ in self.memtable:
            count += 1
        return count

    def close(self):
        """Cleanly closes resources."""
        self.wal.close()
        for reader in self.sst_readers:
            reader.close()
