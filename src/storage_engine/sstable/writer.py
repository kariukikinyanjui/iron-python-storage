import struct
from typing import Any
from src.storage_engine.memtable.skiplist import SkipList


class SSTableWriter:
    """
    Flushes a MemTable (SkipList) to disk as an immutable SSTable.
    """

    def write(self, memtable: SkipList, filepath: str) -> None:
        """
        Iterates through the MemTable and writes key-value pairs to the file.

        Args:
            memtable: The populated SkipList.
            filepath: The destination path (e.g., "data/001.set").
        """
        with open(filepath, "wb") as f:
            # The memtable iterator yields (key, value) in strictly sorted order
            for key, value in memtable:
                self._write_pair(f, key, value)

    def _write_pair(self, file_obj, key: str, value: str) -> None:
        """Helper to serialize and write a single pair."""
        # Convert to bytes
        key_bytes = key.encode('utf-8')
        val_bytes = value.encode('utf-8')

        # Pack integers (4 bytes, Big Endian)
        k_len = struct.pack('>I', len(key_bytes))
        v_len = struct.pack('>I', len(val_bytes))

        # Sequential write: Lengths first, then Data
        file_obj.write(k_len)
        file_obj.write(key_bytes)
        file_obj.write(v_len)
        file_obj.write(val_bytes)
