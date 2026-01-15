import os
import struct
from typing import BinaryIO, Optional


class WALLogger:
    """
    Handles appending logs to disk with durability guarantees
    Format: [Key Size (4B)][Key][Value Size (4B)][Value]
    """

    def __init__(self, path: str):
        self.path = path
        # Open in Append Binary mode.
        # buffering=0 means we manage our own buffering (mostly).
        self.file: BinaryIO = open(path, "ab", buffering=0)

    def append(self, key: str, value: str, fsync: bool = True) -> None:
        """
        Serializes and appends a record to the log.

        Args:
            key: The data key.
            value: The data value.
            fsync: If True, forces a flush to physical disk immediately (Strict Durability)
        """
        # Encode strings to bytes
        key_bytes = key.encode('utf-8')
        val_bytes = value.encode('utf-8')

        # Pack length headers (Unsigned Int, 4 bytes, Big Endian)
        header = struct.pack('>I', len(key_bytes))
        val_header = struct.pack('>I', len(val_bytes))

        # Write to OS Page Cache
        self.file.write(header)
        self.file.write(key_bytes)
        self.file.write(val_header)
        self.file.write(val_bytes)

        if fsync:
            self.flush()

    def flush(self) -> None:
        """Forces the OS to write the buffer to the physical disk."""
        # 1. Flush Python's internal buffer to the OS
        self.file.flush()
        # 2. Force OS to flush Page Cache to Disk Hardware
        os.fsync(self.file.fileno())

    def close(self) -> None:
        self.file.close()
