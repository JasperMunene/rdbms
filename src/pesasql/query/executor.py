"""
Query Executor - Executes query plans
"""

from typing import List, Dict, Any, Optional, Iterator
from ..storage.file_manager import FileManager
from ..storage.page import Page, PageType
from ..catalog.schema import TableSchema
from ..catalog.catalog import Catalog
from ..types.value import Value
from .exceptions import PesaSQLExecutionError
from ..constants import DB_PAGE_COUNT_OFFSET, PAGE_HEADER_SIZE, PAGE_SIZE
import struct
import time



class Row:
    """A single row of data"""

    def __init__(self, values: List[Value], row_id: int = 0):
        self.values = values
        self.row_id = row_id  # Internal row identifier

    def get_value(self, column_index: int) -> Value:
        """Get value by column index"""
        if 0 <= column_index < len(self.values):
            return self.values[column_index]
        raise IndexError(f"Column index {column_index} out of bounds")

    def set_value(self, column_index: int, value: Value) -> None:
        """Set value at column index"""
        if 0 <= column_index < len(self.values):
            self.values[column_index] = value
        else:
            raise IndexError(f"Column index {column_index} out of bounds")

    def serialize(self) -> bytes:
        """Serialize row to bytes"""
        parts = []
        for value in self.values:
            parts.append(value.serialize())
        return b''.join(parts)

    @classmethod
    def deserialize(cls, data: bytes, schema: TableSchema, row_id: int = 0) -> 'Row':
        """Deserialize row from bytes"""
        values = []
        offset = 0

        for column in schema.columns:
            # Get value type from schema
            from ..types.value import Type
            type_map = {
                'INTEGER': Type.INTEGER,
                'FLOAT': Type.FLOAT,
                'DOUBLE': Type.DOUBLE,
                'STRING': Type.STRING,
                'BOOLEAN': Type.BOOLEAN,
                'TIMESTAMP': Type.TIMESTAMP
            }

            # Parse value
            if offset >= len(data):
                values.append(Value(Type.NULL, None))
                continue

            value = Value.deserialize(data[offset:])
            values.append(value)

            # Move offset
            offset += value.get_serialized_size()

        return cls(values, row_id)

    def __repr__(self):
        values_str = ', '.join(str(v) for v in self.values)
        return f"Row({values_str})"

    def __str__(self):
        return repr(self)


class Executor:
    """Executes query plans"""

    def __init__(self, file_manager: FileManager, catalog: Catalog = None):
        self.file_manager = file_manager
        self.catalog = catalog
        self.table_pages = {}  # Cache of table data pages

    def execute(self, plan: Dict[str, Any]) -> Any:
        """Execute a query plan"""
        plan_type = plan.get('plan_type')


        if plan_type == 'SELECT':
            return self.execute_select(plan)
        elif plan_type == 'INSERT':
            return self.execute_insert(plan)
        elif plan_type == 'CREATE_TABLE':
            return self.execute_create_table(plan)
        elif plan_type == 'DROP_TABLE':
            return self.execute_drop_table(plan)
        else:
            # Handle simple commands
            return {'status': 'executed', 'plan': plan_type}

    # ... (execute_select and execute_insert skipped) ...

    def execute_create_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CREATE TABLE"""
        table_name = plan['table_name']
        columns = plan['columns']  # SchemaColumn objects
        
        # Create table in catalog
        # Create table in catalog
        if self.catalog:
            schema = TableSchema(table_name, columns)
            if not self.catalog.create_table(schema):
                raise PesaSQLExecutionError(f"Failed to create table '{table_name}' (Catalog error)")
            
        return {
            'table_name': table_name,
            'status': 'created',
            'columns': len(columns)
        }

    def execute_drop_table(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DROP TABLE"""
        table_name = plan['table_name']

        # Find and deallocate all data pages for this table
        # (Implementation depends on how we track table data pages)
        
        # Drop from catalog
        if self.catalog:
            self.catalog.drop_table(table_name)

        return {
            'table_name': table_name,
            'status': 'dropped'
        }
    def execute_select(self, plan: Dict[str, Any]) -> List[Row]:
        """Execute SELECT query"""
        table_name = plan['table_name']
        table_schema = plan['table_schema']
        column_indices = plan['column_indices']
        filter_conditions = plan.get('filter_conditions', [])
        limit = plan.get('limit')
        offset = plan.get('offset') or 0

        # Find table data pages
        data_pages = self._find_table_pages(table_name)

        # Scan rows
        rows = []
        rows_scanned = 0

        for page_id in data_pages:
            page = self.file_manager.read_page(page_id)
            page_rows = self._extract_rows_from_page(page, table_schema)

            for row in page_rows:
                # Apply WHERE clause
                if self._row_matches_conditions(row, filter_conditions):
                    rows_scanned += 1

                    # Apply OFFSET
                    if rows_scanned <= offset:
                        continue

                    # Apply LIMIT
                    if limit and len(rows) >= limit:
                        break

                    # Project columns
                    if column_indices:
                        projected_values = [row.get_value(i) for i in column_indices]
                        projected_row = Row(projected_values, row.row_id)
                        rows.append(projected_row)
                    else:
                        rows.append(row)

            if limit and len(rows) >= limit:
                break

        return rows

    def execute_insert(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute INSERT query"""
        table_name = plan['table_name']
        table_schema = plan['table_schema']
        column_indices = plan['column_indices']
        values_ast = plan['values_ast']

        rows_inserted = 0

        # Evaluate value expressions
        from ..parser.parser import Parser
        from ..types.value import Value, Type

        for value_list in values_ast:
            # Create full row with NULLs
            row_values = [Value(Type.NULL, None) for _ in range(len(table_schema.columns))]

            # Fill in provided values
            for i, col_idx in enumerate(column_indices):
                expr = value_list[i]

                # Evaluate expression (simplified - should use expression evaluator)
                if hasattr(expr, 'literal'):
                    value = expr.literal.to_value()
                else:
                    # For now, only support literals
                    raise PesaSQLExecutionError("Only literal values supported in INSERT")

                # Type checking
                expected_type = table_schema.columns[col_idx].data_type
                if value.type.value != expected_type.value:
                    # Try to convert
                    try:
                        value = Value(expected_type, value.value)
                    except ValueError:
                        raise PesaSQLExecutionError(
                            f"Type mismatch for column '{table_schema.columns[col_idx].name}': "
                            f"expected {expected_type.name}, got {value.type.name}"
                        )

                row_values[col_idx] = value

            # Create row
            row = Row(row_values)

            # Insert into table
            self._insert_row(table_name, table_schema, row)
            rows_inserted += 1

        return {
            'rows_inserted': rows_inserted,
            'status': 'success'
        }



    # Helper methods
    def _find_table_pages(self, table_name: str) -> List[int]:
        """Find all data pages for a table"""
        # In a real system, we'd have a table metadata page that tracks data pages
        # For simplicity, we'll scan all pages looking for TABLE pages
        # This is inefficient but works for Week 3

        header = self.file_manager.read_page(0)
        total_pages = header.read_int(DB_PAGE_COUNT_OFFSET)
        
        table_pages = []

        for page_id in range(total_pages):
            try:
                page = self.file_manager.read_page(page_id)
                if page.page_type == PageType.TABLE:
                    # Check if this page belongs to our table
                    # Read table name from page header
                    page_table_name = page.read_string(13)
                    if page_table_name == table_name:
                        table_pages.append(page_id)
            except Exception:
                continue



        return table_pages

    def _extract_rows_from_page(self, page: Page, schema: TableSchema) -> List[Row]:
        """Extract rows from a data page (Length-Prefixed Scan)"""
        rows = []

        free_start = page.read_short(9)  # PAGE_FREE_START_OFFSET
        free_start = page.read_short(9)  # PAGE_FREE_START_OFFSET
        offset = 13 + 64  # PAGE_HEADER_SIZE + Table Name (64 bytes)
        row_id = 0


        while offset < free_start:
            # Read 4-byte length prefix
            if offset + 4 > free_start:
                break
                
            row_size = page.read_int(offset)
            offset += 4

            if offset + row_size > free_start:
                break

            # Read row data
            row_data = page.read_bytes(offset, row_size)
            row = Row.deserialize(row_data, schema, row_id)
            rows.append(row)

            offset += row_size
            row_id += 1

        return rows

    def _row_matches_conditions(self, row: Row, conditions: List[Dict[str, Any]]) -> bool:
        """Check if row matches WHERE conditions"""
        if not conditions:
            return True

        for condition in conditions:
            column_index = condition['column_index']
            operator = condition['operator']
            expected_value = condition['value']

            actual_value = row.get_value(column_index)

            if not actual_value.compare(expected_value, operator):
                return False

        return True

    def _insert_row(self, table_name: str, schema: TableSchema, row: Row) -> None:
        """Insert a row into the table"""
        # Serialize row
        row_data = row.serialize()
        row_size = len(row_data)
        total_size = 4 + row_size  # 4 bytes for length prefix

        # Find a page with free space
        # Pass required size including prefix
        page_id = self._find_free_page_for_table(table_name, total_size)
        page = self.file_manager.read_page(page_id)

        # Allocate space in page
        free_start = page.read_short(9)  # PAGE_FREE_START_OFFSET
        allocated_offset = free_start

        # Write length prefix (4 bytes)
        page.write_int(allocated_offset, row_size)
        
        # Write row data
        page.write_bytes(allocated_offset + 4, row_data)

        # Update free space pointer
        page.write_short(9, free_start + total_size)
        page.is_dirty = True

        # Write page back
        self.file_manager.write_page_with_wal(page)

    def _find_free_page_for_table(self, table_name: str, required_size: int) -> int:
        """Find or allocate a page with free space for the table"""
        # Scan existing pages? (Inefficient for now)
        # For Phase 3, we simply allocate new page if strictly needed, 
        # but let's try to reuse the last page if it has space.
        
        # Simple optimization: Check last page of table
        table_pages = self._find_table_pages(table_name)
        if table_pages:
            last_page_id = table_pages[-1]
            page = self.file_manager.read_page(last_page_id)
            free_start = page.read_short(9)
            if free_start + required_size < PAGE_SIZE:
                return last_page_id

        # Allocate new page
        page = self.file_manager.allocate_page()
        page.page_type = PageType.TABLE

        # Initialize page for this table
        # Store table name in page header (after generic header)
        page.write_string(13, table_name, 64)  # Offset 13 = after PAGE_HEADER_SIZE
        page.write_short(9, 13 + 64)  # Update free_start to after table name

        page.is_dirty = True
        self.file_manager.write_page_with_wal(page)

        return page.page_id