"""
B+ Tree Implementation for Indexing
Supports range queries and efficient lookups.
"""

from typing import List, Optional, Tuple, Iterator
from .index_page import IndexPage, IndexPageType
from ..file_manager import FileManager
from ...storage.page import Page, PageType
from ...constants import PAGE_SIZE, PAGE_HEADER_SIZE, PAGE_FREE_START_OFFSET, PAGE_FREE_END_OFFSET
from ...types.value import Value
import bisect


class BPlusTree:
    """B+ Tree index for efficient key-based lookups"""

    def __init__(self, file_manager: FileManager, root_page_id: int = 0,
                 order: int = 4, key_type: str = "INTEGER"):
        """
        Initialize B+ Tree

        Args:
            file_manager: File manager for page access
            root_page_id: Page ID of root node (0 for new tree)
            order: Maximum number of children per internal node
            key_type: Type of keys stored (for serialization)
        """
        self.file_manager = file_manager
        self.root_page_id = root_page_id
        self.order = order  # Maximum children for internal nodes
        self.leaf_order = order - 1  # Maximum keys for leaf nodes
        self.key_type = key_type

        if root_page_id == 0:
            self._create_new_tree()

    def _create_new_tree(self):
        """Create new B+ Tree with empty root leaf"""
        # Create root leaf page
        root_page = self.file_manager.allocate_page()
        root_page.page_type = PageType.INDEX
        root_page = IndexPage(root_page.page_id)
        root_page.initialize_index_header(IndexPageType.ROOT)

        self.root_page_id = root_page.page_id
        self.file_manager.write_page_with_wal(root_page)

    def insert(self, key: Value, value: int) -> bool:
        """
        Insert key-value pair into index

        Args:
            key: Key to insert
            value: Associated value (RID or page ID)

        Returns:
            True if inserted, False if duplicate (for unique indexes)
        """
        # Search for existing key (for uniqueness check)
        existing = self.search(key)
        if existing:
            # Key exists - uniqueness violation for primary/unique constraints
            return False

        # Start insertion from root
        root_page = self._get_page(self.root_page_id)
        result = self._insert_recursive(root_page, key, value)

        # Handle root split
        if result is not None:
            # Old root was split, create new root
            new_root = self.file_manager.allocate_page()
            new_root_page = IndexPage(new_root.page_id)
            new_root_page.initialize_index_header(IndexPageType.ROOT)

            # Insert split key and pointers
            new_root_page.insert_key_value(result[0], self.root_page_id, 0)
            new_root_page.insert_key_value(Value(None, None), result[1], 1)
            new_root_page.set_key_count(1)  # One key, two children

            # Update child parent pointers
            self._update_parent(self.root_page_id, new_root.page_id)
            self._update_parent(result[1], new_root.page_id)

            # Update root
            self.root_page_id = new_root.page_id
            self.file_manager.write_page_with_wal(new_root_page)

        return True

    def _insert_recursive(self, page: IndexPage, key: Value, value: int) -> Optional[Tuple[Value, int]]:
        """
        Recursively insert key-value pair

        Returns:
            None if no split, (split_key, new_page_id) if split occurred
        """
        if page.get_index_type() in [IndexPageType.LEAF, IndexPageType.ROOT]:
            # Check if leaf
            if page.get_index_type() != IndexPageType.INTERNAL:
                # Leaf node insertion
                return self._insert_leaf(page, key, value)

        # Internal node - find child to insert into
        child_index = self._find_child_index(page, key)
        child_page_id = self._get_child_pointer(page, child_index)
        child_page = self._get_page(child_page_id)

        # Recursive insert into child
        result = self._insert_recursive(child_page, key, value)

        if result is not None:
            # Child was split, need to insert split key into this node
            split_key, new_child_page_id = result

            if page.get_key_count() < self.order - 1:
                # Space available in this internal node
                self._insert_internal(page, split_key, new_child_page_id, child_index)
                return None
            else:
                # Internal node is full, need to split
                return self._split_internal(page, split_key, new_child_page_id, child_index)

        return None

    def _insert_leaf(self, page: IndexPage, key: Value, value: int) -> Optional[Tuple[Value, int]]:
        """
        Insert into leaf node

        Returns:
            None if no split, (split_key, new_page_id) if split
        """
        # Find insertion position
        key_count = page.get_key_count()
        insert_pos = 0

        # Simple linear search (could optimize with binary search)
        for i in range(key_count):
            existing_key, _ = page.get_key_value(i)
            if key.compare(existing_key, '<'):
                break
            insert_pos = i + 1

        # Check if leaf has space
        if key_count < self.leaf_order:
            # Insert in current leaf
            page.insert_key_value(key, value, insert_pos)
            self.file_manager.write_page_with_wal(page)
            return None
        else:
            # Leaf is full, need to split
            return self._split_leaf(page, key, value, insert_pos)

    def _split_leaf(self, page: IndexPage, key: Value, value: int, insert_pos: int) -> Tuple[Value, int]:
        """
        Split leaf node

        Returns:
            (split_key, new_page_id)
        """
        # Create new leaf
        new_leaf_page = self.file_manager.allocate_page()
        new_leaf = IndexPage(new_leaf_page.page_id)
        new_leaf.initialize_index_header(IndexPageType.LEAF)

        # Copy half of keys to new leaf
        keys_to_move = self.leaf_order // 2

        # Collect all keys including new one
        all_keys = []
        all_values = []
        old_key_count = page.get_key_count()

        for i in range(old_key_count):
            k, v = page.get_key_value(i)
            all_keys.append(k)
            all_values.append(v)

        # Insert new key at correct position
        all_keys.insert(insert_pos, key)
        all_values.insert(insert_pos, value)

        # Clear old leaf
        for i in range(old_key_count):
            page.delete_key(0)  # Always delete first (simplified)

        # Redistribute keys
        split_point = len(all_keys) // 2
        split_key = all_keys[split_point]

        # Put first half in old leaf
        for i in range(split_point):
            page.insert_key_value(all_keys[i], all_values[i], i)

        # Put second half in new leaf
        for i in range(split_point, len(all_keys)):
            new_leaf.insert_key_value(all_keys[i], all_values[i], i - split_point)

        # Update linked list of leaves
        old_next = page.get_next_leaf()
        page.set_next_leaf(new_leaf.page_id)
        new_leaf.set_prev_leaf(page.page_id)
        new_leaf.set_next_leaf(old_next)

        if old_next != 0:
            old_next_page = self._get_page(old_next)
            old_next_page.set_prev_leaf(new_leaf.page_id)
            self.file_manager.write_page_with_wal(old_next_page)

        # Set parent for new leaf
        new_leaf.set_parent(page.get_parent())

        # Write pages
        self.file_manager.write_page_with_wal(page)
        self.file_manager.write_page_with_wal(new_leaf)

        return split_key, new_leaf.page_id

    def _insert_internal(self, page: IndexPage, key: Value, child_page_id: int, after_index: int):
        """Insert key and child pointer into internal node"""
        # Find position to insert
        key_count = page.get_key_count()
        insert_pos = 0

        for i in range(key_count):
            existing_key, _ = page.get_key_value(i)
            if key.compare(existing_key, '<'):
                break
            insert_pos = i + 1

        # Insert key and update child pointers
        # Implementation depends on how child pointers are stored
        # Simplified: we store keys and children separately

        # For now, use the page's insert method (simplified)
        page.insert_key_value(key, child_page_id, insert_pos)
        self.file_manager.write_page_with_wal(page)

    def _split_internal(self, page: IndexPage, key: Value, child_page_id: int,
                        after_index: int) -> Tuple[Value, int]:
        """Split internal node when full"""
        # Similar to leaf split but with child pointers
        # Create new internal node
        new_internal_page = self.file_manager.allocate_page()
        new_internal = IndexPage(new_internal_page.page_id)
        new_internal.initialize_index_header(IndexPageType.INTERNAL)

        # Collect all keys and child pointers
        all_keys = []
        all_children = []

        key_count = page.get_key_count()

        # Simplified: store keys and children
        # Note: This is a simplified implementation

        # For demo, we'll use a simple approach
        # In real B+ tree, internal nodes have n keys and n+1 children

        # Find median for split
        split_point = self.order // 2

        # The split_key is the key at split_point
        # Left gets keys[0:split_point], right gets keys[split_point+1:]
        # Implementation details omitted for brevity

        # For now, return the middle key and new page
        # This is a placeholder for the full implementation
        return key, new_internal.page_id

    def search(self, key: Value) -> Optional[int]:
        """
        Search for key in index

        Args:
            key: Key to search for

        Returns:
            Value (RID) if found, None otherwise
        """
        page = self._get_page(self.root_page_id)
        return self._search_recursive(page, key)

    def _search_recursive(self, page: IndexPage, key: Value) -> Optional[int]:
        """Recursive search helper"""
        if page.get_index_type() != IndexPageType.INTERNAL:
            # Leaf node - search for key
            for i in range(page.get_key_count()):
                page_key, value = page.get_key_value(i)
                if key.compare(page_key, '='):
                    return value
            return None

        # Internal node - find child to search
        child_index = self._find_child_index(page, key)
        child_page_id = self._get_child_pointer(page, child_index)
        child_page = self._get_page(child_page_id)

        return self._search_recursive(child_page, key)

    def range_search(self, start_key: Value, end_key: Value) -> List[int]:
        """
        Find all values with keys in range [start_key, end_key]

        Args:
            start_key: Inclusive start key
            end_key: Inclusive end key

        Returns:
            List of values (RIDs)
        """
        # Find leaf containing start key
        leaf_page = self._find_leaf(self.root_page_id, start_key)
        results = []

        while leaf_page is not None:
            # Scan leaf
            for i in range(leaf_page.get_key_count()):
                key, value = leaf_page.get_key_value(i)

                # Check if key >= start_key
                if key.compare(start_key, '>='):
                    # Check if key <= end_key
                    if key.compare(end_key, '<='):
                        results.append(value)
                    else:
                        # Beyond range, stop
                        return results

            # Move to next leaf
            next_leaf_id = leaf_page.get_next_leaf()
            if next_leaf_id == 0:
                break

            leaf_page = self._get_page(next_leaf_id)

        return results

    def _find_leaf(self, page_id: int, key: Value) -> Optional[IndexPage]:
        """Find leaf node that should contain key"""
        page = self._get_page(page_id)

        if page.get_index_type() != IndexPageType.INTERNAL:
            return page

        # Internal node - find child
        child_index = self._find_child_index(page, key)
        child_page_id = self._get_child_pointer(page, child_index)

        return self._find_leaf(child_page_id, key)

    def _find_child_index(self, page: IndexPage, key: Value) -> int:
        """Find index of child pointer for given key"""
        # For internal node with n keys, there are n+1 children
        # Child i contains keys <= key_i, child i+1 contains keys > key_i

        key_count = page.get_key_count()

        for i in range(key_count):
            page_key, _ = page.get_key_value(i)
            if key.compare(page_key, '<'):
                return i

        # If key is greater than all keys, return last child
        return key_count  # Last child index

    def _get_child_pointer(self, page: IndexPage, child_index: int) -> int:
        """Get child page ID at index"""
        # This depends on how child pointers are stored
        # Simplified: for now, assume page stores (key, child) pairs

        if child_index < page.get_key_count():
            _, child_id = page.get_key_value(child_index)
            return child_id
        else:
            # Last child pointer is stored separately
            # For now, return 0 (placeholder)
            return 0

    def _get_page(self, page_id: int) -> IndexPage:
        """Get index page wrapper"""
        page = self.file_manager.read_page(page_id)
        # Convert to IndexPage
        index_page = IndexPage(page_id)
        index_page.data = page.data
        return index_page

    def _update_parent(self, child_page_id: int, parent_page_id: int):
        """Update parent pointer of child page"""
        child_page = self._get_page(child_page_id)
        child_page.set_parent(parent_page_id)
        self.file_manager.write_page_with_wal(child_page)

    def delete(self, key: Value) -> bool:
        """Delete key from index (for future implementation)"""
        # TODO: Implement B+ tree deletion with rebalancing
        # For now, return False
        return False

    def get_tree_info(self) -> dict:
        """Get B+ Tree statistics"""
        return {
            'root_page_id': self.root_page_id,
            'order': self.order,
            'key_type': self.key_type,
            'leaf_order': self.leaf_order
        }