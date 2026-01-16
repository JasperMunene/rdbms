"""
Value Types - Type system for PesaSQL
Handles type conversion, comparison, and serialization.
"""

from enum import Enum
from typing import Any
import struct
import datetime


class Type(Enum):
    """SQL data types"""
    INTEGER = 1
    FLOAT = 2
    DOUBLE = 3
    STRING = 4
    BOOLEAN = 5
    TIMESTAMP = 6
    NULL = 7


class Value:
    """Type-safe value container with serialization"""

    def __init__(self, value_type: Type, value: Any = None):
        self.type = value_type
        self._value = self._coerce(value, value_type)

    def _coerce(self, value: Any, target_type: Type) -> Any:
        """Coerce value to target type"""
        if value is None:
            return None

        try:
            if target_type == Type.INTEGER:
                return int(value)
            elif target_type == Type.FLOAT:
                return float(value)
            elif target_type == Type.DOUBLE:
                return float(value)
            elif target_type == Type.STRING:
                return str(value)
            elif target_type == Type.BOOLEAN:
                if isinstance(value, str):
                    return value.lower() in ('true', 't', '1', 'yes')
                return bool(value)
            elif target_type == Type.TIMESTAMP:
                if isinstance(value, datetime.datetime):
                    return value
                elif isinstance(value, str):
                    # Simple format: YYYY-MM-DD HH:MM:SS
                    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                elif isinstance(value, (int, float)):
                    return datetime.datetime.fromtimestamp(value)
                else:
                    raise ValueError(f"Cannot convert {type(value)} to timestamp")
            else:
                raise ValueError(f"Unsupported type: {target_type}")
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert {repr(value)} to {target_type.name}: {e}")

    @property
    def value(self) -> Any:
        return self._value

    def get_serialized_size(self) -> int:
        """Calculate serialized size in bytes"""
        if self._value is None:
            return 1  # Just type byte

        if self.type == Type.INTEGER:
            return 5  # type(1) + value(4)
        elif self.type == Type.FLOAT:
            return 5  # type(1) + value(4)
        elif self.type == Type.DOUBLE:
            return 9  # type(1) + value(8)
        elif self.type == Type.BOOLEAN:
            return 2  # type(1) + value(1)
        elif self.type == Type.TIMESTAMP:
            return 9  # type(1) + timestamp(8)
        elif self.type == Type.STRING:
            # type(1) + length(2) + data
            return 3 + len(self._value.encode('utf-8'))
        else:
            return 1

    def serialize(self) -> bytes:
        """Serialize value to bytes"""
        if self._value is None:
            return struct.pack('B', Type.NULL.value)

        if self.type == Type.INTEGER:
            return struct.pack('>BI', self.type.value, self._value)
        elif self.type == Type.FLOAT:
            return struct.pack('>Bf', self.type.value, self._value)
        elif self.type == Type.DOUBLE:
            return struct.pack('>Bd', self.type.value, self._value)
        elif self.type == Type.BOOLEAN:
            return struct.pack('>BB', self.type.value, 1 if self._value else 0)
        elif self.type == Type.TIMESTAMP:
            timestamp = self._value.timestamp() if hasattr(self._value, 'timestamp') else float(self._value)
            return struct.pack('>Bd', self.type.value, timestamp)
        elif self.type == Type.STRING:
            encoded = self._value.encode('utf-8')
            return struct.pack(f'>BH{len(encoded)}s', self.type.value, len(encoded), encoded)
        else:
            raise ValueError(f"Cannot serialize type: {self.type}")

    @classmethod
    def deserialize(cls, data: bytes) -> 'Value':
        """Deserialize value from bytes"""
        if not data:
            return cls(Type.NULL, None)

        type_byte = data[0]

        try:
            value_type = Type(type_byte)
        except ValueError:
            raise ValueError(f"Unknown type byte: {type_byte}")

        if value_type == Type.NULL:
            return cls(Type.NULL, None)

        if value_type == Type.INTEGER:
            value = struct.unpack_from('>I', data, 1)[0]
        elif value_type == Type.FLOAT:
            value = struct.unpack_from('>f', data, 1)[0]
        elif value_type == Type.DOUBLE:
            value = struct.unpack_from('>d', data, 1)[0]
        elif value_type == Type.BOOLEAN:
            value = bool(struct.unpack_from('B', data, 1)[0])
        elif value_type == Type.TIMESTAMP:
            timestamp = struct.unpack_from('>d', data, 1)[0]
            value = datetime.datetime.fromtimestamp(timestamp)
        elif value_type == Type.STRING:
            length = struct.unpack_from('>H', data, 1)[0]
            value = struct.unpack_from(f'{length}s', data, 3)[0].decode('utf-8')
        else:
            raise ValueError(f"Cannot deserialize type: {value_type}")

        return cls(value_type, value)

    def compare(self, other: 'Value', operator: str) -> bool:
        """Compare two values with operator"""
        if self.type == Type.NULL or other.type == Type.NULL:
            # NULL comparisons
            if operator == '=':
                return self.type == Type.NULL and other.type == Type.NULL
            elif operator == '!=':
                return not (self.type == Type.NULL and other.type == Type.NULL)
            else:
                return False

        # Type coercion for comparison
        if self.type != other.type:
            # Try to coerce to common type
            try:
                if self.type in (Type.INTEGER, Type.FLOAT, Type.DOUBLE) and \
                        other.type in (Type.INTEGER, Type.FLOAT, Type.DOUBLE):
                    # Numeric comparison
                    v1 = float(self.value)
                    v2 = float(other.value)
                else:
                    return False
            except (ValueError, TypeError):
                return False
        else:
            v1 = self.value
            v2 = other.value

        # Apply operator
        if operator == '=':
            return v1 == v2
        elif operator == '!=':
            return v1 != v2
        elif operator == '<':
            return v1 < v2
        elif operator == '<=':
            return v1 <= v2
        elif operator == '>':
            return v1 > v2
        elif operator == '>=':
            return v1 >= v2
        else:
            raise ValueError(f"Unknown operator: {operator}")

    def __eq__(self, other):
        if not isinstance(other, Value):
            return False
        return self.type == other.type and self.value == other.value

    def __repr__(self):
        if self.type == Type.NULL:
            return "NULL"
        elif self.type == Type.TIMESTAMP and isinstance(self.value, datetime.datetime):
            return f"'{self.value.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif self.type == Type.STRING:
            return f"'{self.value}'"
        else:
            return str(self.value)

    def __str__(self):
        return repr(self)