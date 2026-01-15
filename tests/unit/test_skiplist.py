import pytest
from src.storage_engine.memtable.skiplist import SkipList


def test_skiplist_insert_and_search():
    """Test basic insertion and retrieval."""
    sl = SkipList()
    sl.insert("user:1", "Alice")
    sl.insert("user:2", "Bob")


    assert sl.search("user:1") == "Alice"
    assert sl.search("user:2") == "Bob"
    assert sl.search("user:3") is None

def test_skiplist_update_existing():
    """Test that inserting a duplicate key updates the value."""
    sl = SkipList()
    sl.insert("config:timeout", "10s")
    assert sl.search("config:timeout") == "10s"

    # Update
    sl.insert("config:timeout", "30s")
    assert sl.search("config:timeout") == "30s"

def test_skiplist_sorted_iteration():
    """
    Critical Test: The SSTable flush relies on this being sorted.
    Insert keys in random order and verify they come out sorted.
    """
    sl = SkipList()
    keys = ["zebra", "apple", "mango", "banana"]
    for k in keys:
        sl.insert(k, f"data-{k}")

    # Colect all items
    items = list(sl)

    assert len(items) == 4
    assert items[0][0] == "apple"
    assert items[1][0] == "banana"
    assert items[2][0] == "mango"
    assert items[3][0] == "zebra"
