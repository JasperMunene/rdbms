"""
Page Module - Core storage unit for PesaSQL
Handles fixed-size page structure with header metadata and typed data access.
"""

import struct
from enum import Enum
from typing import Optional
from ..constants import PAGE_SIZE, PAGE_HEADER_SIZE, PAGE_TYPE_OFFSET, PAGE_CHECKSUM_OFFSET
from ..constants import PAGE_LSN_OFFSET, PAGE_FREE_START_OFFSET, PAGE_FREE_END_OFFSET


class PageType(Enum):
    """Types of pages in the database"""
    FREE = 0
    HEADER = 1
    CATALOG = 2
    TABLE = 3
    INDEX = 4


class Page:
    """Fixed-size database page with typed data access methods"""

    def __init__(self, page_id: int, page_type: PageType = PageType.FREE):
        """
        Initialize a new page

        Args:
            page_id: Unique identifier for this page
            page_type: Type of page (default: FREE)
        """
        self.page_id = page_id
        self.page_type = page_type
        self.data = bytearray(PAGE_SIZE)
        self.is_dirty = False
        self.pin_count = 0
        self.lsn = 0  # Log Sequence Number

        # Initialize header with default values
        self._initialize_header()

    def _initialize_header(self) -> None:
        """Initialize page header with default values"""
        # Write page type (1 byte)
        self.write_byte(PAGE_TYPE_OFFSET, self.page_type.value)

        # Initialize checksum to 0 (will be calculated when page is written)
        self.write_int(PAGE_CHECKSUM_OFFSET, 0)

        # Initialize LSN (Log Sequence Number)
        self.write_int(PAGE_LSN_OFFSET, self.lsn)

        # Free space pointers - initially all space is free
        self.write_short(PAGE_FREE_START_OFFSET, PAGE_HEADER_SIZE)
        self.write_short(PAGE_FREE_END_OFFSET, PAGE_SIZE)

        self.is_dirty = True

    def calculate_checksum(self) -> int:
        """
        Calculate simple checksum for page data validation

        Returns:
            Checksum value (32-bit integer)
        """
        # Simple checksum: sum of all bytes after the checksum field
        checksum_bytes = self.data[PAGE_CHECKSUM_OFFSET + 4:]
        return sum(checksum_bytes) & 0xFFFFFFFF

    def update_checksum(self) -> None:
        """Update checksum field with current page data"""
        checksum = self.calculate_checksum()
        self.write_int(PAGE_CHECKSUM_OFFSET, checksum)

    def validate_checksum(self) -> bool:
        """
        Validate page checksum

        Returns:
            True if checksum is valid, False otherwise
        """
        stored_checksum = self.read_int(PAGE_CHECKSUM_OFFSET)
        calculated_checksum = self.calculate_checksum()
        return stored_checksum == calculated_checksum

    # --------------------------------------------------------------------
    # Typed Data Access Methods
    # --------------------------------------------------------------------

    def write_byte(self, offset: int, value: int) -> None:
        """Write single byte at offset"""
        if offset < 0 or offset >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        self.data[offset] = value & 0xFF
        self.is_dirty = True

    def read_byte(self, offset: int) -> int:
        """Read single byte from offset"""
        if offset < 0 or offset >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        return self.data[offset]

    def write_short(self, offset: int, value: int) -> None:
        """Write 2-byte unsigned integer (big-endian)"""
        if offset < 0 or offset + 1 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        struct.pack_into('>H', self.data, offset, value)
        self.is_dirty = True

    def read_short(self, offset: int) -> int:
        """Read 2-byte unsigned integer (big-endian)"""
        if offset < 0 or offset + 1 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        return struct.unpack_from('>H', self.data, offset)[0]

    def write_int(self, offset: int, value: int) -> None:
        """Write 4-byte unsigned integer (big-endian)"""
        if offset < 0 or offset + 3 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        struct.pack_into('>I', self.data, offset, value)
        self.is_dirty = True

    def read_int(self, offset: int) -> int:
        """Read 4-byte unsigned integer (big-endian)"""
        if offset < 0 or offset + 3 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        return struct.unpack_from('>I', self.data, offset)[0]

    def write_double(self, offset: int, value: float) -> None:
        """Write 8-byte double precision float (big-endian)"""
        if offset < 0 or offset + 7 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        struct.pack_into('>d', self.data, offset, value)
        self.is_dirty = True

    def read_double(self, offset: int) -> float:
        """Read 8-byte double precision float (big-endian)"""
        if offset < 0 or offset + 7 >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")
        return struct.unpack_from('>d', self.data, offset)[0]

    def write_string(self, offset: int, value: str, max_length: int) -> None:
        """
        Write length-prefixed UTF-8 string

        Args:
            offset: Starting position in page
            value: String to write
            max_length: Maximum bytes allowed (including 1-byte length prefix)

        Raises:
            ValueError: If string exceeds maximum length
        """
        if offset < 0 or offset + max_length >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")

        encoded = value.encode('utf-8')
        if len(encoded) > max_length - 1:  # Reserve 1 byte for length
            raise ValueError(f"String exceeds {max_length - 1} bytes")

        # Store length (1 byte) followed by data
        self.write_byte(offset, len(encoded))
        self.data[offset + 1:offset + 1 + len(encoded)] = encoded
        self.is_dirty = True

    def read_string(self, offset: int) -> str:
        """
        Read length-prefixed UTF-8 string

        Args:
            offset: Starting position in page

        Returns:
            Decoded string
        """
        if offset < 0 or offset >= PAGE_SIZE:
            raise IndexError(f"Offset {offset} out of page bounds")

        length = self.read_byte(offset)
        if offset + 1 + length > PAGE_SIZE:
            raise IndexError(f"String extends beyond page bounds")

        data = self.data[offset + 1:offset + 1 + length]
        return data.decode('utf-8', errors='replace')

    def write_bytes(self, offset: int, data: bytes) -> None:
        """Write raw bytes at offset"""
        if offset < 0 or offset + len(data) > PAGE_SIZE:
            raise IndexError(f"Data exceeds page bounds")
        self.data[offset:offset + len(data)] = data
        self.is_dirty = True

    def read_bytes(self, offset: int, length: int) -> bytes:
        """Read raw bytes from offset"""
        if offset < 0 or offset + length > PAGE_SIZE:
            raise IndexError(f"Read exceeds page bounds")
        return bytes(self.data[offset:offset + length])

    # --------------------------------------------------------------------
    # Free Space Management
    # --------------------------------------------------------------------

    def get_free_space(self) -> int:
        """Get amount of free space in page"""
        free_start = self.read_short(PAGE_FREE_START_OFFSET)
        free_end = self.read_short(PAGE_FREE_END_OFFSET)
        return free_end - free_start

    def allocate_space(self, size: int) -> Optional[int]:
        """
        Allocate space within the page

        Args:
            size: Number of bytes to allocate

        Returns:
            Starting offset of allocated space, or None if insufficient space
        """
        free_start = self.read_short(PAGE_FREE_START_OFFSET)
        free_end = self.read_short(PAGE_FREE_END_OFFSET)

        if free_end - free_start < size:
            return None

        # Allocate from beginning of free space
        allocated_offset = free_start
        free_start += size

        self.write_short(PAGE_FREE_START_OFFSET, free_start)
        self.is_dirty = True

        return allocated_offset

    def free_space(self, offset: int, size: int) -> None:
        """
        Mark space as free (simplified - actual compaction would be more complex)

        Note: This is a placeholder - real free space management requires
        compaction or free list within the page.
        """
        # For now, we just mark page as having more free space
        # Real implementation would track individual free slots
        self.is_dirty = True

    # --------------------------------------------------------------------
    # Utility Methods
    # --------------------------------------------------------------------

    def get_used_space(self) -> int:
        """Get amount of used space in page"""
        free_start = self.read_short(PAGE_FREE_START_OFFSET)
        return free_start - PAGE_HEADER_SIZE

    def __repr__(self) -> str:
        """String representation of page"""
        return (f"Page(id={self.page_id}, type={self.page_type.name}, "
                f"dirty={self.is_dirty}, pin={self.pin_count}, "
                f"free={self.get_free_space()}B)")