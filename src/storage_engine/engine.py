import os
from typing import Optional
from src.storage_engine.memtable.skiplist import SkipList
from src.storage_engine.wal.logger import WALLogger


class StorageEngine:
    """
    The main entry point for the database.
    Coordinates the MemTable (RAM) and WAL (Disk).
    """
    def __init__(self, dir_path: str = "data"):
        self.dir_path = dir_path
        os.makedirs(dir_path, exist_ok=True)

        # Initialize components
        self.memtable = SkipList()
        self.wal_path = os.path.join(dir_path, "recovery.wal")
        self.wal = WALLogger(self.wal_path)

    def put(self, key: str, value: str) -> None:
        """
        Writes a key-value pair to the database.
        Sequence:
        1. Write to WAL (Disk Durability)
        2. Update MemTable (In-Memory Speed)
        """
        # 1. Durability: Append to WAL immediately
        # We default to fsync=True for safety, though this could be configurable.
        self.wal.append(key, value, fsync=True)

        # 2. Volatile: Update the in-memory structure
        self.memtable.insert(key, value)

    def get(self, key: str) -> Optional[str]:
        """
        Retrieves a value by key.
        Currently, this only checks the MemTable.
        """
        return self.memtable.search(key)
    
    def close(self):
        """Cleanly closes resources."""
        self.wal.close()
