import heapq
import os
from typing import List
from src.storage_engine.sstable.reader import SSTableReader
from src.storage_engine.sstable.writer import SSTableWriter
from src.storage_engine.memtable.skiplist import SkipList # Used only for typing if needed


class Compactor:
    """
    Merges multiple immutable SSTable into a single new SSTable,
    discarding overwritten keys (garbage collection).
    """

    def merge(self, input_paths: List[str], output_path: str) -> None:
        """
        Args:
            input_paths: List of paths to existing .sst files (ordered oldest to newest).
            output_path: Destination for the merged file.
        """
        readers = []
        try:
            # 1. Open all files as iterators
            for path in input_paths:
                readers.append(SSTableReader(path))

            # 2. Create a merged iterator
            # We assume input_paths are sorted by age.
            # Ideally, we would attach a timestamp to the record, but for this
            # version, we rely on the stable sort order of heapq.merge.
            # However, standard heapq.merge is just value-based.
            # To handle updates correctly, we need to iterate carefully.

            # Simple approach: Load all iteratores into a generator that yields sorted (key, value)
            merged_iter = heapq.merge(*readers, key=lambda x: x[0])

            self._write_merged(merged_iter, output_path)

        finally:
            for r in readers:
                r.close()
    
    def _write_merged(self, iterator, output_path):
        """Consumes the sorted iterator, deduplicates keys, and writes to disk."""
        # We can reuse the logic from SSTableWriter if we adapt the interface,
        # or write raw bytes here. Reusing Writer is cleaner but Writer expects a SkipList.
        # Let's write raw for efficiency/simplicity here.

        # Temporary in-memory buffer is NOT used. We stream directly.

        # We use a trick: We wrap the raw writing logic.
        # But wait, SSTableWriter._write_pair is available.
        writer = SSTableWriter()

        with open(output_path, "wb") as f:
            last_key = None
            last_val = None

            for key, val in iterator:
                if key == last_key:
                    # Found a duplicate!
                    # Since we are iterating, subsequent values for the same key
                    # replace the previous ones (assuming stable merge of chronological files).
                    last_val = val
                else:
                    # New key encountered. Flush the previous one if it exists.
                    if last_key is not None:
                        writer._write_pair(f, last_key, last_val)

                    last_key = key
                    last_val = val

            # Don't forget the final key
            if last_key is not None:
                writer._write_pair(f, last_key, last_val)
