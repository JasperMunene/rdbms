"""
PesaSQL - Database Constants
Constants for page layout, data types, and system configuration.
"""

# Page Configuration
PAGE_SIZE = 4096  # 4KB, standard page size matching OS block size
HEADER_PAGE_ID = 0  # First page always contains database metadata
CATALOG_PAGE_ID = 1  # Second page for system catalog
MAX_TABLE_NAME = 64
MAX_COLUMN_NAME = 64
CATALOG_SLOT_COUNT = 20  # Fixed slots per catalog page

# Page Header Offsets (for all pages)
PAGE_TYPE_OFFSET = 0  # 1 byte
PAGE_CHECKSUM_OFFSET = 1  # 4 bytes
PAGE_LSN_OFFSET = 5  # 4 bytes (Log Sequence Number)
PAGE_FREE_START_OFFSET = 9  # 2 bytes
PAGE_FREE_END_OFFSET = 11  # 2 bytes
PAGE_HEADER_SIZE = 13  # Total header size in bytes

# Database Header Offsets (page 0 only, starts after generic header)
DB_MAGIC_OFFSET = PAGE_HEADER_SIZE
DB_VERSION_OFFSET = DB_MAGIC_OFFSET + 16
DB_PAGE_COUNT_OFFSET = DB_VERSION_OFFSET + 4
DB_FREE_LIST_HEAD_OFFSET = DB_PAGE_COUNT_OFFSET + 4
DB_CATALOG_ROOT_OFFSET = DB_FREE_LIST_HEAD_OFFSET + 4

# Magic number for PesaSQL database files
PESA_MAGIC = "PESA_DB_v1.0"
MAX_MAGIC_LENGTH = 16

# Data Type Codes
DATA_TYPE_INT = 1
DATA_TYPE_FLOAT = 2
DATA_TYPE_DOUBLE = 3
DATA_TYPE_STRING = 4
DATA_TYPE_BOOL = 5