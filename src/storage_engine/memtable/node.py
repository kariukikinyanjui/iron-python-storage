
from typing import Any, List, Optional


class Node:
    """
    A node in the Skip List.

    Attributes:
        key: The sortable key.
        value: The data payload.
        forward: A list of pionters to subsequent nodes at various levels
                 forward[i] points to the next node at level 1.
    """
    __slots__ = ('key', 'value', 'forward')

    def __init__(self, key: Any, value: Any, level: int = 1):
        self.key = key
        self.value = value
        # Initialize forward pointers for the specified number of levels (height)
        self.forward: List[Optional['Node']] = [None] * level

    def __repr__(self):
        return f"Node(key={self.key}, value={self.value}, level={len(self.forward)})"
