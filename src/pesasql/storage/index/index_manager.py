"""
Index Manager - Manages all indexes in the database
"""

from typing import Dict, List, Optional, Tuple
from .bptree import BPlusTree
from ..file_manager import FileManager
from ...types.value import Value, Type
import struct


class IndexEntry:
    """Entry in index catalog"""

    def __init__(self, index_id: int, table_name: str, column_name: str,
                 is_primary: bool, is_unique: bool, root_page_id: int):
        self.index_id = index_id
        self.table_name = table_name
        self.column_name = column_name
        self.is_primary = is_primary
        self.is_unique = is_unique
        self.root_page_id = root_page_id

    def serialize(self) -> bytes:
        """Serialize index entry"""
        return struct.pack('>I64s64s??I',
                           self.index_id,
                           self.table_name.encode('utf-8').ljust(64, b'\x00'),
                           self.column_name.encode('utf-8').ljust(64, b'\x00'),
                           self.is_primary,
                           self.is_unique,
                           self.root_page_id)

    @classmethod
    def deserialize(cls, data: bytes) -> 'IndexEntry':
        """Deserialize index entry"""
        index_id = struct.unpack_from('>I', data, 0)[0]
        table_name = data[4:68].decode('utf-8').rstrip('\x00')
        column_name = data[68:132].decode('utf-8').rstrip('\x00')
        is_primary = bool(struct.unpack_from('?', data, 132)[0])
        is_unique = bool(struct.unpack_from('?', data, 133)[0])
        root_page_id = struct.unpack_from('>I', data, 134)[0]

        return cls(index_id, table_name, column_name, is_primary, is_unique, root_page_id)


class IndexManager:
    """Manages creation, deletion, and lookup of indexes"""

    INDEX_CATALOG_PAGE_ID = 2  # Fixed page for index catalog

    def __init__(self, file_manager: FileManager):
        self.file_manager = file_manager
        self.indexes: Dict[str, BPlusTree] = {}  # index_name -> BPlusTree
        self.index_catalog: Dict[str, IndexEntry] = {}  # index_name -> entry

        # Load existing indexes
        self._load_index_catalog()

    def _load_index_catalog(self):
        """Load index catalog from disk"""
        try:
            page = self.file_manager.read_page(self.INDEX_CATALOG_PAGE_ID)

            # Check if it's an index catalog page
            if page.page_type != 2:  # CATALOG type
                # Initialize empty catalog
                return

            # Read number of indexes
            num_indexes = page.read_short(13)  # After page header

            offset = 15  # Start of index entries
            entry_size = 138  # Size of IndexEntry serialized

            for i in range(num_indexes):
                if offset + entry_size > 4096:
                    break

                data = page.read_bytes(offset, entry_size)
                entry = IndexEntry.deserialize(data)

                # Create B+ Tree
                index_name = f"{entry.table_name}.{entry.column_name}"
                bptree = BPlusTree(self.file_manager, entry.root_page_id)

                self.indexes[index_name] = bptree
                self.index_catalog[index_name] = entry

                offset += entry_size

        except Exception as e:
            print(f"Error loading index catalog: {e}")
            # Start with empty catalog

    def create_index(self, table_name: str, column_name: str,
                     is_primary: bool, is_unique: bool) -> bool:
        """
        Create new index

        Args:
            table_name: Name of table
            column_name: Name of column to index
            is_primary: Whether this is a primary key index
            is_unique: Whether this is a unique index

        Returns:
            True if created, False if error
        """
        index_name = f"{table_name}.{column_name}"

        if index_name in self.indexes:
            print(f"Index '{index_name}' already exists")
            return False

        try:
            # Create new B+ Tree
            bptree = BPlusTree(self.file_manager)

            # Create index entry
            index_id = len(self.index_catalog) + 1
            entry = IndexEntry(
                index_id=index_id,
                table_name=table_name,
                column_name=column_name,
                is_primary=is_primary,
                is_unique=is_unique,
                root_page_id=bptree.root_page_id
            )

            # Store in memory
            self.indexes[index_name] = bptree
            self.index_catalog[index_name] = entry

            # Update catalog on disk
            self._update_index_catalog()

            print(f"Created index '{index_name}' (root page: {bptree.root_page_id})")
            return True

        except Exception as e:
            print(f"Error creating index: {e}")
            return False

    def drop_index(self, table_name: str, column_name: str) -> bool:
        """Drop index"""
        index_name = f"{table_name}.{column_name}"

        if index_name not in self.indexes:
            print(f"Index '{index_name}' not found")
            return False

        # Remove from memory
        del self.indexes[index_name]
        del self.index_catalog[index_name]

        # Update catalog on disk
        self._update_index_catalog()

        print(f"Dropped index '{index_name}'")
        return True

    def get_index(self, table_name: str, column_name: str) -> Optional[BPlusTree]:
        """Get index for table column"""
        index_name = f"{table_name}.{column_name}"
        return self.indexes.get(index_name)

    def has_index(self, table_name: str, column_name: str) -> bool:
        """Check if index exists for column"""
        index_name = f"{table_name}.{column_name}"
        return index_name in self.indexes

    def insert_into_index(self, table_name: str, column_name: str,
                          key: Value, row_id: int) -> bool:
        """
        Insert key into index

        Args:
            table_name: Table name
            column_name: Column name
            key: Key value
            row_id: Row identifier

        Returns:
            True if inserted, False if unique constraint violation
        """
        index_name = f"{table_name}.{column_name}"

        if index_name not in self.indexes:
            # No index for this column
            return True

        bptree = self.indexes[index_name]
        return bptree.insert(key, row_id)

    def lookup(self, table_name: str, column_name: str, key: Value) -> Optional[int]:
        """Look up key in index"""
        index_name = f"{table_name}.{column_name}"

        if index_name not in self.indexes:
            return None

        bptree = self.indexes[index_name]
        return bptree.search(key)

    def range_lookup(self, table_name: str, column_name: str,
                     start_key: Value, end_key: Value) -> List[int]:
        """Range lookup in index"""
        index_name = f"{table_name}.{column_name}"

        if index_name not in self.indexes:
            return []

        bptree = self.indexes[index_name]
        return bptree.range_search(start_key, end_key)

    def get_table_indexes(self, table_name: str) -> List[IndexEntry]:
        """Get all indexes for a table"""
        return [entry for name, entry in self.index_catalog.items()
                if name.startswith(f"{table_name}.")]

    def _update_index_catalog(self):
        """Write index catalog to disk"""
        # Create or get index catalog page
        try:
            page = self.file_manager.read_page(self.INDEX_CATALOG_PAGE_ID)
        except:
            # Allocate new page
            page = self.file_manager.allocate_page()
            page.page_type = 2  # CATALOG type

        # Write header
        page.write_short(13, len(self.index_catalog))  # Number of indexes

        # Write entries
        offset = 15
        entry_size = 138

        for entry in self.index_catalog.values():
            if offset + entry_size > 4096:
                print("Warning: Index catalog page full")
                break

            data = entry.serialize()
            page.write_bytes(offset, data)
            offset += entry_size

        page.is_dirty = True
        self.file_manager.write_page_with_wal(page)

    def get_index_info(self) -> dict:
        """Get index manager statistics"""
        return {
            'total_indexes': len(self.indexes),
            'indexes': list(self.indexes.keys()),
            'catalog_size': len(self.index_catalog)
        }