"""
Index Page - Specialized pages for B+ Tree nodes
Implements Slotted Page architecture for variable-length keys.
"""

from enum import Enum
import struct
from typing import List, Optional, Tuple
from ...storage.page import Page, PageType
from ...constants import PAGE_SIZE, PAGE_HEADER_SIZE, PAGE_FREE_START_OFFSET, PAGE_FREE_END_OFFSET
from ...types.value import Value, Type


class IndexPageType(Enum):
    """Types of index pages"""
    LEAF = 0
    INTERNAL = 1
    ROOT = 2


class IndexPage(Page):
    """
    Specialized page for B+ Tree index nodes.
    
    Structure:
    [Header]
    [Slot 0][Slot 1]... (Grows Down ->)
    ... Free Space ...
    (<- Grows Up) ...[Data 1][Data 0]
    
    Slot: 2-byte offset to Data.
    Data: [Key][Value] (Variable length)
    """

    # Index page header layout (after base page header)
    INDEX_TYPE_OFFSET = PAGE_HEADER_SIZE
    KEY_COUNT_OFFSET = INDEX_TYPE_OFFSET + 1
    PARENT_POINTER_OFFSET = KEY_COUNT_OFFSET + 2
    NEXT_LEAF_OFFSET = PARENT_POINTER_OFFSET + 4
    PREV_LEAF_OFFSET = NEXT_LEAF_OFFSET + 4
    # Start of slots
    INDEX_HEADER_SIZE = PREV_LEAF_OFFSET + 4
    
    SLOT_SIZE = 2

    def __init__(self, page_id: int, page_type: PageType = PageType.INDEX):
        """Initialize index page"""
        super().__init__(page_id, page_type)
        self.index_type = IndexPageType.LEAF
        self.key_count = 0
        self.parent_pointer = 0
        self.next_leaf = 0
        self.prev_leaf = 0

    def initialize_index_header(self, index_type: IndexPageType, parent: int = 0):
        """Initialize index-specific header"""
        self.index_type = index_type
        self.write_byte(self.INDEX_TYPE_OFFSET, index_type.value)
        self.write_short(self.KEY_COUNT_OFFSET, 0)
        self.write_int(self.PARENT_POINTER_OFFSET, parent)
        self.write_int(self.NEXT_LEAF_OFFSET, 0)
        self.write_int(self.PREV_LEAF_OFFSET, 0)
        
        # Initialize free space pointers for Slotted Page
        # Start: After Index Header (Slots start here)
        self.write_short(PAGE_FREE_START_OFFSET, self.INDEX_HEADER_SIZE)
        # End: End of page (Data starts here)
        self.write_short(PAGE_FREE_END_OFFSET, PAGE_SIZE)
        
        self.is_dirty = True

    # ----------------------------------------------------------------
    # Header Accessors
    # ----------------------------------------------------------------

    def get_key_count(self) -> int:
        return self.read_short(self.KEY_COUNT_OFFSET)

    def set_key_count(self, count: int):
        self.write_short(self.KEY_COUNT_OFFSET, count)
        self.key_count = count
        self.is_dirty = True

    def get_parent(self) -> int:
        return self.read_int(self.PARENT_POINTER_OFFSET)

    def set_parent(self, parent_id: int):
        self.write_int(self.PARENT_POINTER_OFFSET, parent_id)
        self.parent_pointer = parent_id
        self.is_dirty = True

    def get_next_leaf(self) -> int:
        return self.read_int(self.NEXT_LEAF_OFFSET)

    def set_next_leaf(self, next_id: int):
        self.write_int(self.NEXT_LEAF_OFFSET, next_id)
        self.next_leaf = next_id
        self.is_dirty = True

    def get_prev_leaf(self) -> int:
        return self.read_int(self.PREV_LEAF_OFFSET)

    def set_prev_leaf(self, prev_id: int):
        self.write_int(self.PREV_LEAF_OFFSET, prev_id)
        self.prev_leaf = prev_id
        self.is_dirty = True

    def get_index_type(self) -> IndexPageType:
        type_val = self.read_byte(self.INDEX_TYPE_OFFSET)
        return IndexPageType(type_val)

    # ----------------------------------------------------------------
    # Slotted Page Operations
    # ----------------------------------------------------------------

    def insert_key_value(self, key: Value, value: int, index: int) -> bool:
        """
        Insert key-value pair at specified index using slot directory.
        Supports variable length keys.
        """
        # 1. Serialize Data
        key_data = key.serialize()
        # [Key][Value(4 bytes)]
        entry_data = key_data + struct.pack('>I', value)
        entry_size = len(entry_data)
        
        # 2. Check Space
        # Need space for: New Data + New Slot (2 bytes)
        required_space = entry_size + self.SLOT_SIZE
        if self.get_free_space() < required_space:
            return False
            
        # 3. Allocate Data Space (from End)
        free_end = self.read_short(PAGE_FREE_END_OFFSET)
        data_offset = free_end - entry_size
        
        # 4. Write Data
        self.write_bytes(data_offset, entry_data)
        self.write_short(PAGE_FREE_END_OFFSET, data_offset) # Update free_end
        
        # 5. Insert Slot
        # Slots are at [HEADER ... free_start]
        # We need to shift slots at [index ... count] to make room
        current_count = self.get_key_count()
        slots_start = self.INDEX_HEADER_SIZE
        
        if index < current_count:
            # Shift slots right by 2 bytes
            shift_start = slots_start + (index * self.SLOT_SIZE)
            slots_len = (current_count - index) * self.SLOT_SIZE
            slots_data = self.read_bytes(shift_start, slots_len)
            self.write_bytes(shift_start + self.SLOT_SIZE, slots_data)
            
        # Write new slot (offset to data)
        new_slot_pos = slots_start + (index * self.SLOT_SIZE)
        self.write_short(new_slot_pos, data_offset)
        
        # Update free_start (Slot area grew)
        free_start = self.read_short(PAGE_FREE_START_OFFSET)
        self.write_short(PAGE_FREE_START_OFFSET, free_start + self.SLOT_SIZE)
        
        # Update count
        self.set_key_count(current_count + 1)
        
        return True

    def get_key_value(self, index: int) -> Tuple[Value, int]:
        """Get key-value pair at index"""
        # Read Offset from Slot
        slots_start = self.INDEX_HEADER_SIZE
        slot_pos = slots_start + (index * self.SLOT_SIZE)
        data_offset = self.read_short(slot_pos)
        
        # Read Data at Offset
        return self._read_entry(data_offset)

    def delete_key(self, index: int):
        """Delete key at index (Compact slots, leak data space for now)"""
        current_count = self.get_key_count()
        if index >= current_count:
            return
            
        # 1. Shift Slots Left
        slots_start = self.INDEX_HEADER_SIZE
        if index < current_count - 1:
            # Move [index+1 ... end] to [index]
            shift_src = slots_start + ((index + 1) * self.SLOT_SIZE)
            slots_len = (current_count - index - 1) * self.SLOT_SIZE
            slots_data = self.read_bytes(shift_src, slots_len)
            self.write_bytes(shift_src - self.SLOT_SIZE, slots_data)
            
        # 2. Update pointers
        free_start = self.read_short(PAGE_FREE_START_OFFSET)
        self.write_short(PAGE_FREE_START_OFFSET, free_start - self.SLOT_SIZE)
        
        self.set_key_count(current_count - 1)
        
        # Note: Data space at 'data_offset' is now "leaked" (fragmented).
        # A real implementation would compact data or use a free list.
        # For this implementation, we assume page splits/merges handle reorganization eventualy.

    def update_value(self, index: int, new_value: int):
        """Update value at index (keeping key same)"""
        # Read Offset
        slots_start = self.INDEX_HEADER_SIZE
        slot_pos = slots_start + (index * self.SLOT_SIZE)
        data_offset = self.read_short(slot_pos)
        
        # Read Key to skip it
        # Note: This is inefficient, we should have stored separate offsets or lengths?
        # But our entry format is [Key][Value]. We must parse key to find value offset.
        key, _ = self._read_entry(data_offset)
        
        key_size = len(key.serialize())
        value_offset = data_offset + key_size
        
        self.write_int(value_offset, new_value)

    # ----------------------------------------------------------------
    # Internal Helpers
    # ----------------------------------------------------------------

    def _read_entry(self, offset: int) -> Tuple[Value, int]:
        """Read key-value entry from offset"""
        # Read key type first byte
        key_type_byte = self.read_byte(offset)
        key_type = Type(key_type_byte)
        
        # Deserialize key
        # Value.deserialize needs just the bytes. We need to know length to find Value part.
        # Value.deserialize handles the reading if we give it the buffer.
        # But we only have `read_bytes`.
        
        # We need to peek/calculate size.
        if key_type == Type.STRING:
            # String: [Type][Len:2][Chars...]
            length = self.read_short(offset + 1)
            key_size = 3 + length
        elif key_type == Type.INTEGER:
            key_size = 5
        elif key_type == Type.DOUBLE:
            key_size = 9
        elif key_type == Type.BOOLEAN:
            key_size = 2
        elif key_type == Type.NULL:
            key_size = 1
        else:
            key_size = 1 # Should not happen for keys
            
        key_data = self.read_bytes(offset, key_size)
        key = Value.deserialize(key_data)
        
        # Read Value (RID/PageID)
        value = self.read_int(offset + key_size)
        
        return key, value