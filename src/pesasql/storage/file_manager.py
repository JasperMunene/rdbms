"""
File Manager Module - Handles database file I/O and WAL operations
Responsible for disk persistence, page allocation, and transactional safety.
"""

import os
import struct
import time
from pathlib import Path
from .page import Page, PageType
from ..constants import (PAGE_SIZE, HEADER_PAGE_ID, CATALOG_PAGE_ID, PAGE_HEADER_SIZE,
                        DB_MAGIC_OFFSET, DB_VERSION_OFFSET, DB_PAGE_COUNT_OFFSET,
                        DB_FREE_LIST_HEAD_OFFSET, PESA_MAGIC, MAX_MAGIC_LENGTH,
                        CATALOG_SLOT_COUNT)


class FileManager:
    """Manages database file operations with WAL support"""

    def __init__(self, db_path: str):
        """
        Initialize file manager for database

        Args:
            db_path: Path to database file
        """
        self.db_path = Path(db_path)
        self.wal_path = self.db_path.with_suffix('.wal')
        self.page_size = PAGE_SIZE
        self._ensure_data_dir()

    def _ensure_data_dir(self) -> None:
        """Ensure database directory exists"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def create_database(self) -> None:
        """Initialize new database file with proper headers"""
        if self.db_path.exists():
            raise FileExistsError(f"Database {self.db_path} already exists")

        with open(self.db_path, 'wb') as f:
            # Create header page (page 0)
            header = self._create_database_header()
            f.write(header.data)

            # Create catalog page (page 1)
            catalog = self._create_catalog_page()
            f.write(catalog.data)

        # Initialize empty WAL
        self.wal_path.write_bytes(b'')

        print(f"Created database: {self.db_path}")

    def _create_database_header(self) -> Page:
        """Create database header page (page 0)"""
        header = Page(HEADER_PAGE_ID, PageType.HEADER)

        # Generic page header already initialized by Page.__init__
        # Now add database-specific metadata

        # Write magic string
        header.write_string(DB_MAGIC_OFFSET, PESA_MAGIC, MAX_MAGIC_LENGTH)

        # Schema version
        header.write_int(DB_VERSION_OFFSET, 1)

        # Initial page count (header + catalog)
        header.write_int(DB_PAGE_COUNT_OFFSET, 2)

        # Free list head (0 means empty)
        header.write_int(DB_FREE_LIST_HEAD_OFFSET, 0)

        # Update checksum with all data
        header.update_checksum()

        return header

    def _create_catalog_page(self) -> Page:
        """Create initial catalog page (page 1)"""
        catalog = Page(CATALOG_PAGE_ID, PageType.CATALOG)

        # Catalog-specific header (starts after page header)
        # Number of tables (initially 0)
        catalog.write_int(PAGE_HEADER_SIZE, 0)

        # Next catalog page (0 means none)
        catalog.write_int(PAGE_HEADER_SIZE + 4, 0)

        # Reserve space for fixed slot directory
        # Slot dir starts at PAGE_HEADER_SIZE + 6 (19)
        # Each slot is 8 bytes
        reserved_size = 6 + (CATALOG_SLOT_COUNT * 8)
        
        # Advance free start pointer
        # free_start is offset 9 in Page Header
        new_free_start = PAGE_HEADER_SIZE + reserved_size
        catalog.write_short(9, new_free_start)

        catalog.update_checksum()
        return catalog

    def read_page(self, page_id: int) -> Page:
        """
        Read page from disk

        Args:
            page_id: Page identifier

        Returns:
            Page object with data from disk

        Raises:
            IOError: If page cannot be read or is corrupted
            IndexError: If page_id is out of bounds
        """
        if page_id < 0:
            raise IndexError(f"Invalid page ID: {page_id}")

        offset = page_id * self.page_size

        try:
            with open(self.db_path, 'rb') as f:
                # Check file size
                f.seek(0, os.SEEK_END)
                file_size = f.tell()

                if offset >= file_size:
                    raise IndexError(f"Page {page_id} beyond file size {file_size}")

                # Read page data
                f.seek(offset)
                data = f.read(self.page_size)

                if len(data) < self.page_size:
                    # Partial read - pad with zeros
                    data = data + bytes(self.page_size - len(data))

        except FileNotFoundError:
            raise IOError(f"Database file not found: {self.db_path}")

        # Parse page type from data
        page_type_val = data[0]
        try:
            page_type = PageType(page_type_val)
        except ValueError:
            # Unknown type - treat as FREE
            page_type = PageType.FREE

        # Reconstruct page
        page = Page(page_id, page_type)
        page.data = bytearray(data)
        page.is_dirty = False

        # Validate checksum if page is not FREE
        if page_type != PageType.FREE and not page.validate_checksum():
            print(f"Warning: Page {page_id} has invalid checksum")

        return page

    def _read_page_from_disk(self, page_id: int) -> bytes:
        """
        Read raw page data from disk for WAL

        Args:
            page_id: Page identifier

        Returns:
            Raw page bytes
        """
        offset = page_id * self.page_size
        with open(self.db_path, 'rb') as f:
            f.seek(offset)
            return f.read(self.page_size)

    def write_page(self, page: Page) -> None:
        """
        Write page to disk without WAL (for internal use)

        Args:
            page: Page object to write
        """
        if not page.is_dirty:
            return

        # Update checksum before writing
        page.update_checksum()

        offset = page.page_id * self.page_size

        with open(self.db_path, 'r+b') as f:
            f.seek(offset)
            f.write(page.data)
            f.flush()  # Ensure data reaches disk
            os.fsync(f.fileno())  # Force OS to write to disk

        page.is_dirty = False

    def write_page_with_wal(self, page: Page) -> None:
        """
        Write page to disk with WAL protection

        Args:
            page: Page object to write
        """
        if not page.is_dirty:
            return

        # Read old data for WAL
        old_data = self._read_page_from_disk(page.page_id)

        # Write to WAL first
        self._write_to_wal(page.page_id, old_data, page.data)

        # Then write to database
        self.write_page(page)

        # Update LSN in page
        page.lsn += 1
        page.write_int(5, page.lsn)  # LSN offset is 5

    def _write_to_wal(self, page_id: int, old_data: bytes, new_data: bytes) -> None:
        """
        Write change to Write-Ahead Log

        Args:
            page_id: Page identifier
            old_data: Previous page content
            new_data: New page content
        """
        with open(self.wal_path, 'ab') as wal:
            # Log entry format: [page_id:4][timestamp:8][old_len:2][new_len:2][old_data][new_data]
            timestamp = int(time.time() * 1000)  # Milliseconds

            # Write header
            header = struct.pack('>IQHH', page_id, timestamp,
                                 len(old_data), len(new_data))
            wal.write(header)

            # Write data
            wal.write(old_data)
            wal.write(new_data)

            wal.flush()
            os.fsync(wal.fileno())

    def allocate_page(self) -> Page:
        """
        Allocate a new page for database use

        Returns:
            Newly allocated Page object

        Note:
            Tries to reuse pages from free list first, then expands file
        """
        # Read header to check free list
        header = self.read_page(HEADER_PAGE_ID)
        free_head = header.read_int(DB_FREE_LIST_HEAD_OFFSET)

        if free_head != 0:
            # Reuse page from free list
            free_page = self.read_page(free_head)
            next_free = free_page.read_int(PAGE_HEADER_SIZE)  # First field in free page

            # Update header to point to next free page
            header.write_int(DB_FREE_LIST_HEAD_OFFSET, next_free)
            header.is_dirty = True
            self.write_page_with_wal(header)

            # Reset the freed page
            free_page.page_type = PageType.TABLE
            free_page._initialize_header()
            free_page.is_dirty = True

            return free_page
        else:
            # No free pages - expand file
            page_count = header.read_int(DB_PAGE_COUNT_OFFSET)
            new_page_id = page_count

            # Update header
            header.write_int(DB_PAGE_COUNT_OFFSET, page_count + 1)
            header.is_dirty = True
            self.write_page_with_wal(header)

            # Create and append new page
            new_page = Page(new_page_id, PageType.TABLE)
            new_page.is_dirty = True

            with open(self.db_path, 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(new_page.data)
                f.flush()

            return new_page

    def deallocate_page(self, page_id: int) -> None:
        """
        Return page to free list

        Args:
            page_id: Page identifier to deallocate
        """
        if page_id < 2:  # Never deallocate header or catalog
            raise ValueError(f"Cannot deallocate system page {page_id}")

        header = self.read_page(HEADER_PAGE_ID)
        old_free_head = header.read_int(DB_FREE_LIST_HEAD_OFFSET)

        # Read page to deallocate
        page = self.read_page(page_id)

        # Convert page to FREE type
        page.page_type = PageType.FREE
        page._initialize_header()

        # Store old free list head in this page
        page.write_int(PAGE_HEADER_SIZE, old_free_head)
        page.is_dirty = True

        # Write the freed page
        self.write_page_with_wal(page)

        # Update header to point to this page as new free list head
        header.write_int(DB_FREE_LIST_HEAD_OFFSET, page_id)
        header.is_dirty = True
        self.write_page_with_wal(header)

    def checkpoint(self) -> None:
        """
        Create a checkpoint by flushing WAL to database

        Note: In a full implementation, this would:
        1. Ensure all dirty pages are written
        2. Write checkpoint record to WAL
        3. Truncate or archive old WAL segments
        """
        # For now, we just truncate WAL since we're writing pages directly
        # In a real system, we'd need proper WAL replay
        self.wal_path.write_bytes(b'')
        print(f"Checkpoint created - WAL cleared")

    def begin_transaction(self) -> None:
        """Mark transaction boundary in WAL"""
        with open(self.wal_path, 'ab') as wal:
            wal.write(b'TX_START')
            wal.flush()

    def commit_transaction(self) -> None:
        """Commit transaction and optionally checkpoint"""
        with open(self.wal_path, 'ab') as wal:
            wal.write(b'TX_COMMIT')
            wal.flush()

        # Optional: checkpoint after commit
        # self.checkpoint()

    def rollback_transaction(self) -> None:
        """Mark transaction rollback in WAL"""
        with open(self.wal_path, 'ab') as wal:
            wal.write(b'TX_ROLLBACK')
            wal.flush()

    def get_database_info(self) -> dict:
        """Get basic database information"""
        if not self.db_path.exists():
            return {"status": "Database does not exist"}

        header = self.read_page(HEADER_PAGE_ID)

        info = {
            "file_size": self.db_path.stat().st_size,
            "page_size": self.page_size,
            "total_pages": header.read_int(DB_PAGE_COUNT_OFFSET),
            "free_list_head": header.read_int(DB_FREE_LIST_HEAD_OFFSET),
            "wal_size": self.wal_path.stat().st_size if self.wal_path.exists() else 0,
            "magic_string": header.read_string(DB_MAGIC_OFFSET)
        }

        return info