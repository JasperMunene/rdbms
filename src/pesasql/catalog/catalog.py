"""
Catalog Module - System catalog management
Stores and retrieves table schemas from catalog pages.
"""

import struct
from typing import Dict, List, Optional, Tuple
from ..storage.page import Page, PageType
from ..storage.file_manager import FileManager
from .schema import TableSchema, Column, DataType, ColumnConstraint
from ..constants import PAGE_HEADER_SIZE, CATALOG_PAGE_ID, PAGE_SIZE, CATALOG_SLOT_COUNT


class Catalog:
    """Manages database catalog (system tables metadata)"""

    def __init__(self, file_manager: FileManager):
        """
        Initialize catalog manager

        Args:
            file_manager: FileManager instance for page access
        """
        self.file_manager = file_manager
        self.tables: Dict[str, TableSchema] = {}
        self.table_locations: Dict[str, Tuple[int, int]] = {}  # table -> (page_id, slot_id)

        # Load existing catalog
        self._load_catalog()

    def _load_catalog(self) -> None:
        """Load catalog data from disk"""
        try:
            # Read catalog page (page 1)
            catalog_page = self.file_manager.read_page(CATALOG_PAGE_ID)

            # Check if it's a catalog page
            if catalog_page.page_type != PageType.CATALOG:
                print("Warning: Page 1 is not a catalog page")
                return

            # Read catalog header (after page header)
            offset = PAGE_HEADER_SIZE
            table_count = catalog_page.read_short(offset)
            offset += 2

            # First overflow page for catalog
            next_overflow = catalog_page.read_int(offset)
            offset += 4

            # Read table entries
            tables_read = 0
            current_page = catalog_page
            current_page_id = CATALOG_PAGE_ID
            next_overflow_page = next_overflow

            while tables_read < table_count and current_page:
                # Use fixed slot count
                slots_in_page = CATALOG_SLOT_COUNT

                for slot_id in range(slots_in_page):
                    if tables_read >= table_count:
                        break

                    # Read slot directory entry
                    slot_offset = self._get_slot_directory_offset() + (slot_id * 8)
                    entry_offset = current_page.read_int(slot_offset)
                    entry_length = current_page.read_int(slot_offset + 4)

                    if entry_offset == 0:  # Empty slot
                        continue

                    # Read table entry
                    entry_data = current_page.read_bytes(entry_offset, entry_length)
                    table_schema = TableSchema.deserialize(entry_data)

                    # Store in memory
                    self.tables[table_schema.name] = table_schema
                    self.table_locations[table_schema.name] = (current_page_id, slot_id)

                    tables_read += 1

                # Move to overflow page if needed
                if tables_read < table_count and next_overflow_page > 0:
                    current_page = self.file_manager.read_page(next_overflow_page)
                    current_page_id = next_overflow_page
                    next_overflow_page = current_page.read_int(PAGE_HEADER_SIZE)
                else:
                    current_page = None

            print(f"Loaded {len(self.tables)} tables from catalog")

        except Exception as e:
            print(f"Error loading catalog: {e}")
            # Start with empty catalog

    def _get_slots_in_page(self, page: Page) -> int:
        """Calculate number of slots in a catalog page"""
        return CATALOG_SLOT_COUNT

    def _get_slot_directory_offset(self) -> int:
        """Get starting offset for slot directory"""
        # Slot directory starts after catalog header
        # Catalog header: table_count(2) + next_overflow(4)
        return PAGE_HEADER_SIZE + 6

    def create_table(self, table_schema: TableSchema) -> bool:
        """
        Create new table in catalog

        Args:
            table_schema: Table schema definition

        Returns:
            True if successful, False otherwise
        """
        # Check if table already exists
        if table_schema.name in self.tables:
            print(f"Table '{table_schema.name}' already exists")
            return False

        try:
            # Serialize table schema
            table_data = table_schema.serialize()
            data_length = len(table_data)

            # Find space in catalog pages
            page_id, slot_id, offset = self._find_catalog_space(data_length)

            if page_id is None:
                print("No space in catalog for new table")
                return False

            # Read the catalog page
            page = self.file_manager.read_page(page_id)

            # Write table data
            page.write_bytes(offset, table_data)

            # Update slot directory
            slot_offset = self._get_slot_directory_offset() + (slot_id * 8)
            page.write_int(slot_offset, offset)
            page.write_int(slot_offset + 4, data_length)

            # Update free space pointer if needed
            # With fixed slot region, free_start points to next free byte for data
            free_start = page.read_short(9)
            if offset + data_length > free_start:
                page.write_short(9, offset + data_length)

            # Mark page as dirty and write
            page.is_dirty = True
            self.file_manager.write_page_with_wal(page)

            # Update catalog header (table count)
            self._increment_table_count()

            # Update in-memory catalog
            self.tables[table_schema.name] = table_schema
            self.table_locations[table_schema.name] = (page_id, slot_id)

            print(f"Created table '{table_schema.name}'")
            return True

        except Exception as e:
            print(f"Error creating table: {e}")
            return False

    def _find_catalog_space(self, required_size: int) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Find space in catalog pages for table data

        Returns:
            Tuple of (page_id, slot_id, offset) or (None, None, None) if no space
        """
        # Start with main catalog page
        current_page_id = CATALOG_PAGE_ID
        next_overflow = 0

        while True:
            try:
                page = self.file_manager.read_page(current_page_id)

                # Check if this is a catalog page
                if page.page_type != PageType.CATALOG:
                    break

                # Read catalog header
                offset = PAGE_HEADER_SIZE
                offset += 2 # table count
                next_overflow = page.read_int(offset)

                # Find free slot in directory
                slots = CATALOG_SLOT_COUNT
                slot_dir_start = self._get_slot_directory_offset()

                for slot_id in range(slots):
                    slot_offset = slot_dir_start + (slot_id * 8)
                    entry_offset = page.read_int(slot_offset)

                    if entry_offset == 0:  # Empty slot
                        # Check if we have enough space
                        free_start = page.read_short(9)

                        # We can use space from free_start onward
                        if free_start + required_size < PAGE_SIZE:
                            return (current_page_id, slot_id, free_start)

                # Try overflow page
                if next_overflow > 0:
                    current_page_id = next_overflow
                else:
                    # No space, need to allocate new overflow page
                    new_page = self.file_manager.allocate_page()
                    new_page.page_type = PageType.CATALOG
                    
                    # Initialize catalog headers
                    new_page.write_int(PAGE_HEADER_SIZE, 0)  # Next overflow
                    # new_page.write_short(PAGE_HEADER_SIZE + 4, 0)  # 0 tables - NOT USED in overflow
                    
                    # Reserve space for slots
                    reserved_size = 6 + (CATALOG_SLOT_COUNT * 8) # Using slightly wrong constant for overflow but keeping consistent layout logic
                    # Actually overflow pages don't need 'table count' but maintaining structure simplifies reading
                    
                    new_free_start = PAGE_HEADER_SIZE + reserved_size
                    new_page.write_short(9, new_free_start)
                    
                    new_page.is_dirty = True

                    # Link from previous page
                    prev_page = self.file_manager.read_page(current_page_id)
                    
                    # If prev is Page 1, next_overflow is at offset 15
                    # If prev is NOT Page 1, next_overflow is at offset 13 (PAGE_HEADER_SIZE)
                    # Wait, our layout logic assumes consistency.
                    # Page 1: Header(13) + TableCount(2) + NextOverflow(4)
                    # Overflow Page: Header(13) + NextOverflow(4) + (Space?)
                    # Let's standardize: Overflow pages also mimic Page 1 layout but ignore TableCount
                    
                    if current_page_id != CATALOG_PAGE_ID:
                        # Standardize: Overflow pages match memory layout of Page 1
                        # [TableCount(2)][NextOverflow(4)]
                        # So next_overflow is at offset 15 (13+2)
                        link_offset = PAGE_HEADER_SIZE + 2
                    
                    prev_page.write_int(link_offset, new_page.page_id)
                    prev_page.is_dirty = True
                    self.file_manager.write_page_with_wal(prev_page)

                    # Write new page
                    self.file_manager.write_page_with_wal(new_page)

                    # Return first slot in new page
                    return (new_page.page_id, 0, new_free_start)

            except Exception as e:
                print(f"Error searching catalog space: {e}")
                break

        return (None, None, None)

    def _increment_table_count(self) -> None:
        """Increment table count in catalog header"""
        try:
            page = self.file_manager.read_page(CATALOG_PAGE_ID)
            offset = PAGE_HEADER_SIZE
            table_count = page.read_short(offset)
            page.write_short(offset, table_count + 1)
            page.is_dirty = True
            self.file_manager.write_page_with_wal(page)
        except Exception as e:
            print(f"Error incrementing table count: {e}")

    def get_table(self, table_name: str) -> Optional[TableSchema]:
        """
        Get table schema by name

        Args:
            table_name: Name of table to retrieve

        Returns:
            TableSchema or None if not found
        """
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        """Get list of all table names"""
        return list(self.tables.keys())

    def drop_table(self, table_name: str) -> bool:
        """
        Remove table from catalog

        Args:
            table_name: Name of table to drop

        Returns:
            True if successful, False otherwise
        """
        if table_name not in self.tables:
            print(f"Table '{table_name}' not found")
            return False

        try:
            # Get table location
            page_id, slot_id = self.table_locations[table_name]

            # Read the page
            page = self.file_manager.read_page(page_id)

            # Clear slot directory entry
            slot_offset = self._get_slot_directory_offset() + (slot_id * 8)
            page.write_int(slot_offset, 0)
            page.write_int(slot_offset + 4, 0)

            # Mark page as dirty
            page.is_dirty = True
            self.file_manager.write_page_with_wal(page)

            # Decrement table count
            self._decrement_table_count()

            # Remove from memory
            del self.tables[table_name]
            del self.table_locations[table_name]

            print(f"Dropped table '{table_name}'")
            return True

        except Exception as e:
            print(f"Error dropping table: {e}")
            return False

    def _decrement_table_count(self) -> None:
        """Decrement table count in catalog header"""
        try:
            page = self.file_manager.read_page(CATALOG_PAGE_ID)
            offset = PAGE_HEADER_SIZE
            table_count = page.read_short(offset)
            if table_count > 0:
                page.write_short(offset, table_count - 1)
                page.is_dirty = True
                self.file_manager.write_page_with_wal(page)
        except Exception as e:
            print(f"Error decrementing table count: {e}")

    def describe_table(self, table_name: str) -> Optional[str]:
        """
        Get formatted description of table schema

        Args:
            table_name: Name of table to describe

        Returns:
            Formatted string or None if table not found
        """
        table = self.get_table(table_name)
        if table:
            return str(table)
        return None

    def get_catalog_info(self) -> dict:
        """Get catalog statistics"""
        return {
            "table_count": len(self.tables),
            "tables": list(self.tables.keys()),
            "catalog_pages": self._count_catalog_pages(),
            "memory_size": sum(table.get_serialized_size() for table in self.tables.values())
        }

    def _count_catalog_pages(self) -> int:
        """Count number of catalog pages (including overflow)"""
        count = 1  # Always at least page 1
        current_page_id = CATALOG_PAGE_ID

        try:
            while True:
                page = self.file_manager.read_page(current_page_id)
                next_overflow = page.read_int(PAGE_HEADER_SIZE + 2)
                if next_overflow == 0:
                    break
                count += 1
                current_page_id = next_overflow
        except:
            pass

        return count