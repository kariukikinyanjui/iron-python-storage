import mmap
import os
import struct
from typing import Optional


class SSTableReader:
    """Reads SSTable using Memory-Mapped I/O for zero-copy access."""
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.file = open(filepath, "rb")
        # Map the entire file into memory
        # access=mmap.ACCESS_READ ensures we don't accidentally modify immutable data
        self.mm = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
        self.file_size = os.path.getsize(filepath)

    def search(self, search_key: str) -> Optional[str]:
        """
        Scans the SSTable for the key.
        Note: A production system would use a Sparse Index (Bloom Filter) here
        to avoid scanning, but for this milestone, we implement a linear scan
        over the memory-mapped buffer.
        """
        offset = 0
        search_key_bytes = search_key.encode('utf-8')

        while offset < self.file_size:
            # 1. Read Key Length (4 bytes)
            # slicing the mmap object returns bytes without copying if used correctly,
            # but struct.unpack requires a bytes-like object.
            key_len_bytes = self.mm[offset : offset + 4]
            if not key_len_bytes: break
            key_len = struct.unpack('>I', key_len_bytes)[0]
            offset += 4

            # 2. Read Key
            current_key = self.mm[offset : offset + key_len]
            offset += key_len

            # 3. Read Value Length
            val_len_bytes = self.mm[offset : offset + 4]
            val_len = struct.unpack('>I', val_len_bytes)[0]
            offset += 4

            # 4. Read Value (only if key matches, otherwise just skip)
            if current_key == search_key_bytes:
                val_bytes = self.mm[offset : offset + val_len]
                return val_bytes.decode('utf-8')

            # Skip value to get to next record
            offset += val_len

        return None
    
    def close(self):
        self.mm.close()
        self.file.close()
