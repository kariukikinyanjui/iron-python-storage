import random
from typing import Optional, Generator, Tuple, Any, List
from .node import Node


class SkipList:
    """
    A probablistic Skip List implementation for the MemTable.
    Supports O(log N) insertion, search, and ordered iteration.
    """

    def __init__(self, p: float = 0.5, max_level: int = 16):
        """
        Args:
            p: Probability of promoting a node to the next level (default 0.5).
            max_level: Maximum height of the skip list (default 16, covers ~65k items efficiently).
        """
        self.head = Node(None, None, level=max_level)
        self.p = p
        self.max_level = max_level
        self.level = 1 # Current highest level actually in use

    def _random_level(self) -> int:
        """Determines the height of a new node."""
        lvl = 1
        while random.random() < self.p and lvl < self.max_level:
            lvl += 1
        return lvl

    def insert(self, key: Any, value: Any) -> None:
        """Inserts a key-value pair or updates if key exists."""
        # 'update' tracks the node that comes *before* the insertion point at each level
        update: List[Optional[Node]] = [None] * self.max_level
        current = self.head

        # 1. Traverse down from the highest level to find insertion point
        for i in range(self.level -1, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        # 2. Check if key already exists (update scenario)
        current = current.forward[0]
        if current and current.key == key:
            current.value = value
            return

        # 3. Determine level for new node
        lvl = self._random_level()

        # If new level is higher than current max, update 'head' to point to it
        if lvl > self.level:
            for i in range(self.level, lvl):
                update[i] = self.head
            self.level = lvl

        # 4. Create Node and stitch pointers
        new_node = Node(key, value, level=lvl)
        for i in range(lvl):
            # new_node points to what 'update' was pointing to
            new_node.forward[i] = update[i].forward[i]
            # 'update' now points to new_node
            update[i].forward[i] = new_node

    def search(self, key: Any) -> Optional[Any]:
        """Return the value for the key, or None if not found."""
        current = self.head
        for i in range(self.level -1, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]

        current = current.forward[0]
        if current and current.key == key:
            return current.value
        return None

    def __iter__(self) -> Generator[Tuple[Any, Any], None, None]:
        """Yields (key, value) pairs in sorted order."""
        node = self.head.forward[0]
        while node:
            yield node.key, node.value
            node = node.forward[0]
