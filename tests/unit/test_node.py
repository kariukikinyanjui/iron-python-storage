import pytest
from src.storage_engine.memtable.node import Node


def test_node_initialization():
    """Test that a node is initialized with correct attributes."""
    node = Node(key="user:101", value='{"name": "Alice"}', level=4)

    assert node.key == "user:101"
    assert node.value == '{"name": "Alice"}'
    # The forward list should have 4 slots (indices 0 t0 3)
    assert len(node.forward) == 4
    # All slots should initially be None
    assert all(ptr is None for ptr in node.forward)

def test_memory_optimization_slots():
    """
    Critical Test: Verify that __slots__ is working.
    Standard Python objects have a __dict__ attribute.
    Our optimized Node should NOT have a __dict__.
    """
    node = Node(key="test", value="data")

    # Assert that accessing __dict__ raises an AttributeError
    with pytest.raises(AttributeError):
        _ = node.__dict__

def test_node_forward_update():
    """Test that we can update forward pointers (simulation of linking)."""
    node1 = Node("a", 1, level=2)
    node2 = Node("b", 2, level=2)

    # Link node1 to node2 at level 0
    node1.forward[0] = node2

    assert node1.forward[0] is node2
    assert node1.forward[1] is None # Level 1 remains unconnectedd
