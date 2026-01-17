"""
Schema Module - Table and column metadata definitions
Defines the in-memory representation of database schemas.
"""

from enum import Enum
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from ..constants import MAX_TABLE_NAME, MAX_COLUMN_NAME, PAGE_SIZE, PAGE_HEADER_SIZE
import struct


class DataType(Enum):
    """Supported data types in PesaSQL"""
    INTEGER = 1
    FLOAT = 2
    DOUBLE = 3
    STRING = 4
    BOOLEAN = 5
    TIMESTAMP = 6  # For transaction timestamps


class ColumnConstraint(Enum):
    """Column constraints"""
    PRIMARY_KEY = 1
    UNIQUE = 2
    NOT_NULL = 3
    FOREIGN_KEY = 4


@dataclass
class ForeignKey:
    """Foreign key constraint definition"""
    column_name: str
    ref_table: str
    ref_column: str

    def get_serialized_size(self) -> int:
        return (1 + len(self.column_name.encode('utf-8')) +
                1 + len(self.ref_table.encode('utf-8')) +
                1 + len(self.ref_column.encode('utf-8')))

    def serialize(self) -> bytes:
        col_bytes = self.column_name.encode('utf-8')
        table_bytes = self.ref_table.encode('utf-8')
        ref_col_bytes = self.ref_column.encode('utf-8')
        
        return (struct.pack('B', len(col_bytes)) + col_bytes +
                struct.pack('B', len(table_bytes)) + table_bytes +
                struct.pack('B', len(ref_col_bytes)) + ref_col_bytes)
                
    @classmethod
    def deserialize(cls, data: bytes) -> 'ForeignKey':
        offset = 0
        col_len = struct.unpack_from('B', data, offset)[0]
        offset += 1
        col_name = data[offset:offset+col_len].decode('utf-8')
        offset += col_len
        
        table_len = struct.unpack_from('B', data, offset)[0]
        offset += 1
        ref_table = data[offset:offset+table_len].decode('utf-8')
        offset += table_len
        
        ref_col_len = struct.unpack_from('B', data, offset)[0]
        offset += 1
        ref_col_name = data[offset:offset+ref_col_len].decode('utf-8')
        
        return cls(col_name, ref_table, ref_col_name)


@dataclass
class Column:
    """Column metadata definition"""
    name: str
    data_type: DataType
    max_length: int = 0  # For STRING types
    constraints: List[ColumnConstraint] = field(default_factory=list)
    default_value: Any = None

    def __post_init__(self):
        """Validate column definition"""
        if len(self.name) > MAX_COLUMN_NAME:
            raise ValueError(f"Column name too long (max {MAX_COLUMN_NAME} chars)")

        if self.data_type == DataType.STRING and self.max_length <= 0:
            # We allow it now for some cases, but cleaner to enforce
            # Wait, our parser defaults to 255.
            if self.max_length == 0: self.max_length = 255
            # raise ValueError("STRING type requires max_length > 0")

        # Auto-add NOT_NULL for PRIMARY_KEY
        if ColumnConstraint.PRIMARY_KEY in self.constraints:
            if ColumnConstraint.NOT_NULL not in self.constraints:
                self.constraints.append(ColumnConstraint.NOT_NULL)

    def has_constraint(self, constraint: ColumnConstraint) -> bool:
        """Check if column has specific constraint"""
        return constraint in self.constraints

    def get_serialized_size(self) -> int:
        """Calculate serialized size in bytes"""
        # Base: name_len(1) + name + type(1) + constraints(1) + max_length(2) + default_len(2)
        name_len = len(self.name.encode('utf-8'))
        default_len = len(str(self.default_value).encode('utf-8')) if self.default_value is not None else 0
        return 1 + name_len + 1 + 1 + 2 + 2 + default_len

    def serialize(self) -> bytes:
        """Serialize column to bytes"""
        import struct

        name_bytes = self.name.encode('utf-8')
        name_len = len(name_bytes)

        # Convert constraints to bitmask
        constraint_mask = 0
        for constraint in self.constraints:
            constraint_mask |= 1 << (constraint.value - 1)

        # Default value as string
        default_str = str(self.default_value) if self.default_value is not None else ""
        default_bytes = default_str.encode('utf-8')
        default_len = len(default_bytes)

        # Pack: name_len(1B), name, type(1B), constraints(1B), max_length(2B), default_len(2B), default
        parts = [
            struct.pack('B', name_len),
            name_bytes,
            struct.pack('B', self.data_type.value),
            struct.pack('B', constraint_mask),
            struct.pack('>H', self.max_length),
            struct.pack('>H', default_len),
            default_bytes
        ]

        return b''.join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> 'Column':
        """Deserialize column from bytes"""
        import struct

        offset = 0
        name_len = struct.unpack_from('B', data, offset)[0]
        offset += 1

        name = data[offset:offset + name_len].decode('utf-8')
        offset += name_len

        data_type_val = struct.unpack_from('B', data, offset)[0]
        data_type = DataType(data_type_val)
        offset += 1

        constraint_mask = struct.unpack_from('B', data, offset)[0]
        offset += 1

        constraints = []
        for i in range(8):  # Up to 8 constraint types
            if constraint_mask & (1 << i):
                try:
                    constraints.append(ColumnConstraint(i + 1))
                except ValueError:
                    pass

        max_length = struct.unpack_from('>H', data, offset)[0]
        offset += 2

        default_len = struct.unpack_from('>H', data, offset)[0]
        offset += 2

        default_value = None
        if default_len > 0:
            default_str = data[offset:offset + default_len].decode('utf-8')
            # Try to convert to appropriate type
            try:
                if data_type == DataType.INTEGER:
                    default_value = int(default_str)
                elif data_type == DataType.FLOAT:
                    default_value = float(default_str)
                elif data_type == DataType.DOUBLE:
                    default_value = float(default_str)
                elif data_type == DataType.BOOLEAN:
                    default_value = default_str.lower() == 'true'
                else:
                    default_value = default_str
            except:
                default_value = default_str # Fallback

        return cls(
            name=name,
            data_type=data_type,
            max_length=max_length,
            constraints=constraints,
            default_value=default_value
        )


@dataclass
class TableSchema:
    """Table metadata definition"""
    name: str
    columns: List[Column]
    primary_key: Optional[str] = None
    next_overflow_page: int = 0  # For large schemas
    foreign_keys: List[ForeignKey] = field(default_factory=list)

    def __post_init__(self):
        """Validate table definition"""
        if len(self.name) > MAX_TABLE_NAME:
            raise ValueError(f"Table name too long (max {MAX_TABLE_NAME} chars)")

        if not self.columns:
            raise ValueError("Table must have at least one column")

        # Find primary key
        pk_columns = [col for col in self.columns
                      if ColumnConstraint.PRIMARY_KEY in col.constraints]

        if pk_columns:
            if len(pk_columns) > 1:
                # In future, support composite keys
                raise ValueError("Composite primary keys not yet supported")
            self.primary_key = pk_columns[0].name

        # Ensure column names are unique
        column_names = [col.name for col in self.columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError("Column names must be unique")

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name"""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_index(self, name: str) -> int:
        """Get column index by name"""
        for i, col in enumerate(self.columns):
            if col.name == name:
                return i
        return -1

    def get_primary_key_column(self) -> Optional[Column]:
        """Get primary key column"""
        if self.primary_key:
            return self.get_column(self.primary_key)
        return None

    def get_serialized_size(self) -> int:
        """Calculate serialized size in bytes"""
        # Base: name_len(1) + name + col_count(2) + pk_name_len(1) + pk_name + overflow_page(4) + fk_count(1)
        name_len = len(self.name.encode('utf-8'))
        pk_name_len = len(self.primary_key.encode('utf-8')) if self.primary_key else 0
        base_size = 1 + name_len + 2 + 1 + pk_name_len + 4 + 1

        # Add column sizes
        col_size = sum(col.get_serialized_size() for col in self.columns)
        
        # Add FK sizes
        fk_size = sum(fk.get_serialized_size() for fk in self.foreign_keys)

        return base_size + col_size + fk_size

    def serialize(self) -> bytes:
        """Serialize table schema to bytes"""
        import struct

        name_bytes = self.name.encode('utf-8')
        name_len = len(name_bytes)

        pk_bytes = self.primary_key.encode('utf-8') if self.primary_key else b''
        pk_len = len(pk_bytes)

        # Pack header
        parts = [
            struct.pack('B', name_len),
            name_bytes,
            struct.pack('>H', len(self.columns)),
            struct.pack('B', pk_len),
            pk_bytes,
            struct.pack('>I', self.next_overflow_page),
            struct.pack('B', len(self.foreign_keys)) # FK count
        ]

        # Pack columns
        for col in self.columns:
            parts.append(col.serialize())
            
        # Pack FKs
        for fk in self.foreign_keys:
            parts.append(fk.serialize())

        return b''.join(parts)

    @classmethod
    def deserialize(cls, data: bytes) -> 'TableSchema':
        """Deserialize table schema from bytes"""
        import struct

        offset = 0

        # Read table name
        name_len = struct.unpack_from('B', data, offset)[0]
        offset += 1

        name = data[offset:offset + name_len].decode('utf-8')
        offset += name_len

        # Read column count
        col_count = struct.unpack_from('>H', data, offset)[0]
        offset += 2

        # Read primary key
        pk_len = struct.unpack_from('B', data, offset)[0]
        offset += 1

        primary_key = None
        if pk_len > 0:
            primary_key = data[offset:offset + pk_len].decode('utf-8')
            offset += pk_len

        # Read overflow page
        next_overflow_page = struct.unpack_from('>I', data, offset)[0]
        offset += 4
        
        # Read FK count
        fk_count = struct.unpack_from('B', data, offset)[0]
        offset += 1

        # Read columns
        columns = []
        for _ in range(col_count):
            col_name_len = struct.unpack_from('B', data, offset)[0]
            # Manual lookahead for total length
            # name_len(1)+name+type(1)+constraints(1)+max_len(2)+default_len(2)
            base_col_len = 1 + col_name_len + 1 + 1 + 2 + 2
            
            default_len_offset = offset + base_col_len - 2
            default_len = struct.unpack_from('>H', data, default_len_offset)[0]
            col_total_len = base_col_len + default_len
            
            col_data = data[offset:offset + col_total_len]
            column = Column.deserialize(col_data)
            columns.append(column)

            offset += col_total_len
            
        # Read FKs
        foreign_keys = []
        for _ in range(fk_count):
            # Parse FK
            fk_col_len = struct.unpack_from('B', data, offset)[0]
            offset_tmp = offset + 1 + fk_col_len
            
            ref_table_len = struct.unpack_from('B', data, offset_tmp)[0]
            offset_tmp += 1 + ref_table_len
            
            ref_col_len = struct.unpack_from('B', data, offset_tmp)[0]
            
            total_fk_len = 1 + fk_col_len + 1 + ref_table_len + 1 + ref_col_len
            
            fk_data = data[offset:offset + total_fk_len]
            fk = ForeignKey.deserialize(fk_data)
            foreign_keys.append(fk)
            
            offset += total_fk_len

        return cls(
            name=name,
            columns=columns,
            primary_key=primary_key,
            next_overflow_page=next_overflow_page,
            foreign_keys=foreign_keys
        )

    def __repr__(self) -> str:
        """String representation of table schema"""
        cols = []
        for col in self.columns:
            constraints = []
            if ColumnConstraint.PRIMARY_KEY in col.constraints:
                constraints.append("PRIMARY KEY")
            if ColumnConstraint.UNIQUE in col.constraints:
                constraints.append("UNIQUE")
            if ColumnConstraint.NOT_NULL in col.constraints:
                constraints.append("NOT NULL")
            if col.default_value is not None:
                constraints.append(f"DEFAULT {col.default_value}")

            constraint_str = f" {' '.join(constraints)}" if constraints else ""

            if col.data_type == DataType.STRING:
                type_str = f"STRING({col.max_length})"
            else:
                type_str = col.data_type.name

            cols.append(f"  {col.name} {type_str}{constraint_str}")
            
        if self.foreign_keys:
            cols.append("  -- Foreign Keys --")
            for fk in self.foreign_keys:
                cols.append(f"  FOREIGN KEY ({fk.column_name}) REFERENCES {fk.ref_table}({fk.ref_column})")

        cols_str = ",\n".join(cols)
        return f"Table: {self.name}\nColumns:\n{cols_str}"