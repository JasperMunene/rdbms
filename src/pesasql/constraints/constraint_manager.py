"""
Constraint Manager - Enforces PRIMARY KEY and UNIQUE constraints
"""

from typing import Optional, List
from ..storage.index.index_manager import IndexManager
from ..catalog.schema import TableSchema, Column
from ..types.value import Value
from ..storage.file_manager import FileManager


class ConstraintManager:
    """Manages and enforces database constraints"""

    def __init__(self, file_manager: FileManager, index_manager: IndexManager):
        self.file_manager = file_manager
        self.index_manager = index_manager

    def enforce_primary_key(self, table_schema: TableSchema, row_values: List[Value]) -> bool:
        """Enforce PRIMARY KEY constraint"""
        pk_column = table_schema.get_primary_key_column()
        if not pk_column:
            return True  # No primary key

        # Get primary key value
        pk_index = None
        for i, col in enumerate(table_schema.columns):
            if col.name == pk_column.name:
                pk_index = i
                break

        if pk_index is None or pk_index >= len(row_values):
            return False

        pk_value = row_values[pk_index]

        # Check for NULL
        if pk_value.type.name == 'NULL':
            return False  # PRIMARY KEY cannot be NULL

        # Check uniqueness using index
        existing = self.index_manager.lookup(table_schema.name, pk_column.name, pk_value)
        return existing is None  # True if no existing row with this key

    def enforce_unique_constraints(self, table_schema: TableSchema,
                                   row_values: List[Value]) -> bool:
        """Enforce UNIQUE constraints"""
        for i, column in enumerate(table_schema.columns):
            if column.has_constraint('UNIQUE'):
                if i >= len(row_values):
                    continue

                value = row_values[i]

                # NULL values are allowed in UNIQUE columns (multiple NULLs allowed in SQL)
                if value.type.name == 'NULL':
                    continue

                # Check uniqueness
                existing = self.index_manager.lookup(table_schema.name, column.name, value)
                if existing is not None:
                    return False  # Unique constraint violation

        return True

    def create_constraint_indexes(self, table_schema: TableSchema):
        """Create indexes for constraints"""
        # Create primary key index
        pk_column = table_schema.get_primary_key_column()
        if pk_column:
            self.index_manager.create_index(
                table_schema.name, pk_column.name,
                is_primary=True, is_unique=True
            )

        # Create unique constraint indexes
        for column in table_schema.columns:
            if column.has_constraint('UNIQUE'):
                self.index_manager.create_index(
                    table_schema.name, column.name,
                    is_primary=False, is_unique=True
                )

    def validate_insert(self, table_schema: TableSchema, row_values: List[Value]) -> bool:
        """Validate constraints for INSERT operation"""
        # Check PRIMARY KEY
        if not self.enforce_primary_key(table_schema, row_values):
            return False

        # Check UNIQUE constraints
        if not self.enforce_unique_constraints(table_schema, row_values):
            return False

        return True

    def validate_update(self, table_schema: TableSchema, old_row_values: List[Value],
                        new_row_values: List[Value]) -> bool:
        """Validate constraints for UPDATE operation"""
        # For each constrained column, check if value changed
        for i, column in enumerate(table_schema.columns):
            if i >= len(old_row_values) or i >= len(new_row_values):
                continue

            old_value = old_row_values[i]
            new_value = new_row_values[i]

            # Skip if value unchanged
            if old_value == new_value:
                continue

            # Check constraints
            if column.has_constraint('PRIMARY_KEY'):
                # Primary key changed - must be unique and not NULL
                if new_value.type.name == 'NULL':
                    return False

                existing = self.index_manager.lookup(table_schema.name, column.name, new_value)
                if existing is not None:
                    return False

            elif column.has_constraint('UNIQUE'):
                # Unique constraint - check if new value is unique
                if new_value.type.name != 'NULL':
                    existing = self.index_manager.lookup(table_schema.name, column.name, new_value)
                    if existing is not None:
                        return False

        return True