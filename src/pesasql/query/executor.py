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
from ..storage.index.index_manager import IndexManager
from ..types.value import Value, Type
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

    def __init__(self, file_manager: FileManager, catalog: Catalog = None, index_manager: IndexManager = None):
        self.file_manager = file_manager
        self.catalog = catalog
        self.index_manager = index_manager
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
            
            # Auto-create indexes for PRIMARY KEY and UNIQUE columns
            if self.index_manager:
                from ..catalog.schema import ColumnConstraint
                for col in columns:
                    is_primary = ColumnConstraint.PRIMARY_KEY in col.constraints
                    is_unique = ColumnConstraint.UNIQUE in col.constraints
                    
                    if is_primary or is_unique:
                        # Create unique index
                        self.index_manager.create_index(
                            table_name, 
                            col.name, 
                            is_primary=is_primary, 
                            is_unique=True
                        )

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
        access_method = plan.get('access_method', 'SEQ_SCAN')
        
        if access_method == 'INDEX_SCAN' and self.index_manager:
            data_pages = self._find_pages_via_index(plan)
        else:
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

                    rows.append(row)

            if limit and len(rows) >= limit:
                break
                
        # Handle Joins
        planned_joins = plan.get('joins', [])
        if planned_joins:
            rows = self._execute_joins(rows, planned_joins, table_schema, column_indices)

        # Apply column projection (after joins)
        if column_indices:
            projected_rows = []
            for row in rows:
                projected_values = [row.get_value(i) for i in column_indices]
                projected_rows.append(Row(projected_values, row.row_id))
            rows = projected_rows

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
                        # Convert DataType to Type enum
                        target_type = Type(expected_type.value)
                        value = Value(target_type, value.value)
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

        # Pre-check Unique Constraints
        if self.index_manager:
            indexes = self.index_manager.get_table_indexes(table_name)
            for index in indexes:
                 if index.is_unique:  # Only check unique/primary indexes
                     col_name = index.column_name
                     col_idx = schema.get_column_index(col_name)
                     if col_idx != -1:
                         key_value = row.get_value(col_idx)
                         # Check if key exists
                         if self.index_manager.lookup(table_name, col_name, key_value) is not None:
                             raise PesaSQLExecutionError(f"Duplicate entry '{key_value}' for key '{col_name}'")

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

        # Update Indexes
        if self.index_manager:
            indexes = self.index_manager.get_table_indexes(table_name)
            for index in indexes:
                col_name = index.column_name
                # Find value for this column
                # We need column index from schema
                col_idx = schema.get_column_index(col_name)
                if col_idx != -1:
                    key_value = row.get_value(col_idx)
                    # Insert (Key, PageID) into index
                    self.index_manager.insert_into_index(table_name, col_name, key_value, page_id)

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

    def _find_pages_via_index(self, plan: Dict[str, Any]) -> List[int]:
        """Find data pages using an index"""
        index_name = plan['index_name']
        filter_conditions = plan['filter_conditions']
        
        # Find the condition relevant to this index
        target_column = index_name.split('.')[1]
        
        target_condition = None
        for cond in filter_conditions:
             if cond['column_name'] == target_column:
                 target_condition = cond
                 break
                 
        if not target_condition:
            return self._find_table_pages(plan['table_name'])
            
        operator = target_condition['operator']
        value = target_condition['value']
        
        page_ids = []
        
        if operator == '=':
            page_ids = self.index_manager.range_lookup(
                plan['table_name'], target_column, value, value
            )
        
        # Use a set to unique-ify Page IDs
        return list(set(page_ids))

    def _execute_joins(self, outer_rows: List[Row], joins: List[Dict], outer_schema: TableSchema, column_indices: List[int]) -> List[Row]:
        """Execute joins with support for Hash Join and Outer Joins"""
        from ..constants import JOIN_TYPE_INNER, JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT, JOIN_TYPE_FULL

        current_rows = outer_rows
        current_schema = outer_schema
        
        for join in joins:
            inner_table_name = join['table_name']
            inner_schema = join['table_schema']
            condition = join['on_condition']
            join_type_raw = join.get('join_type', JOIN_TYPE_INNER)
            # Handle enum or int
            join_type = join_type_raw.value if hasattr(join_type_raw, 'value') else join_type_raw
            
            # Load inner rows
            inner_pages = self._find_table_pages(inner_table_name)
            inner_rows = []
            for page_id in inner_pages:
                page = self.file_manager.read_page(page_id)
                inner_rows.extend(self._extract_rows_from_page(page, inner_schema))

            # Strategy Selection
            # Use Hash Join for simple Equality Joins (=)
            is_equality = condition and condition['operator'] == '='
            
            if is_equality:
                current_rows = self._execute_hash_join(current_rows, inner_rows, current_schema, inner_schema, condition, join_type)
            else:
                current_rows = self._execute_nested_loop_join(current_rows, inner_rows, current_schema, inner_schema, condition, join_type)
            
            # Update schema for next join iteration
            current_schema = self._merge_schemas(current_schema, inner_schema)

        return current_rows

    def _execute_hash_join(self, outer_rows: List[Row], inner_rows: List[Row], 
                           outer_schema: TableSchema, inner_schema: TableSchema, 
                           condition: Dict, join_type: int) -> List[Row]:
        """Execute Hash Join (O(M+N))"""
        from ..constants import JOIN_TYPE_INNER, JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT, JOIN_TYPE_FULL

        # 1. Identify Keys
        left_op = condition['left']
        right_op = condition['right']
        
        # Helper to resolve operand to (is_inner, index)
        def resolve_op(op):
            col_name = op['name']
            table_name = op.get('table')
            
            # 1. Explicit table match for Inner
            if table_name and table_name == inner_schema.name:
                idx = inner_schema.get_column_index(col_name)
                if idx != -1: return True, idx
                
            # 2. Check Outer Schema
            # Try simple name
            idx = outer_schema.get_column_index(col_name)
            if idx != -1:
                # If table name provided, prefer it not matching if ambiguous? 
                # Keeping simple: if found in outer, assume outer unless strict alias mismatch logic added
                # But for now, if table_name matches inner, we took step 1.
                return False, idx
            # Try qualified name (e.g. t_orders.id)
            if table_name:
                idx = outer_schema.get_column_index(f"{table_name}.{col_name}")
                if idx != -1: return False, idx
                
            # 3. Check Inner Schema (fallback if no table alias or alias mismatch but col exists)
            idx = inner_schema.get_column_index(col_name)
            if idx != -1: return True, idx
            
            return False, -1

        l_is_inner, l_idx = resolve_op(left_op)
        r_is_inner, r_idx = resolve_op(right_op)
        
        outer_key_idx = -1
        inner_key_idx = -1
        
        if l_idx != -1 and r_idx != -1:
            if not l_is_inner and r_is_inner:
                outer_key_idx = l_idx
                inner_key_idx = r_idx
            elif l_is_inner and not r_is_inner:
                outer_key_idx = r_idx
                inner_key_idx = l_idx

        if outer_key_idx == -1 or inner_key_idx == -1:
            # Fallback if key resolution fails (ambiguous or complex)
            return self._execute_nested_loop_join(outer_rows, inner_rows, outer_schema, inner_schema, condition, join_type)

        # 2. Build Phase (Build Hash Table on Inner)
        hash_table = {}
        for row in inner_rows:
            key = row.get_value(inner_key_idx).value # Use raw python value for hashing
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(row)
            
        # 3. Probe Phase
        joined_rows = []
        matched_inner_ids = set() # For Right/Full join

        for outer_row in outer_rows:
            key = outer_row.get_value(outer_key_idx).value
            matches = hash_table.get(key, [])
            
            if matches:
                 for inner_row in matches:
                     merged_values = outer_row.values + inner_row.values
                     joined_rows.append(Row(merged_values))
                     matched_inner_ids.add(inner_row.row_id) # Track match (assumes row_id uniqueness)
            elif join_type == 'LEFT' or join_type == 'FULL':
                 # No match: Emit Outer + NULLs
                 nulls = [Value(col.data_type, None) for col in inner_schema.columns]
                 merged_values = outer_row.values + nulls
                 joined_rows.append(Row(merged_values))

        # 4. Handle Right/Full Join (Unmatched Inner Rows)
        if join_type == 'RIGHT' or join_type == 'FULL':
             for inner_row in inner_rows:
                 if inner_row.row_id not in matched_inner_ids: # Identify by object or ID? ID is safer
                       nulls = [Value(col.data_type, None) for col in outer_schema.columns]
                       merged_values = nulls + inner_row.values
                       joined_rows.append(Row(merged_values))
        
        return joined_rows

    def _execute_nested_loop_join(self, outer_rows: List[Row], inner_rows: List[Row], 
                                  outer_schema: TableSchema, inner_schema: TableSchema, 
                                  condition: Dict, join_type: int) -> List[Row]:
        """Execute Nested Loop Join with Outer Support"""
        from ..constants import JOIN_TYPE_INNER, JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT, JOIN_TYPE_FULL
        
        joined_rows = []
        matched_inner_ids = set()

        for outer_row in outer_rows:
            matched = False
            
            for inner_row in inner_rows:
                if self._check_join_condition(outer_row, inner_row, condition, outer_schema, inner_schema):
                    merged_values = outer_row.values + inner_row.values
                    joined_rows.append(Row(merged_values))
                    matched = True
                    matched_inner_ids.add(inner_row.row_id)
            
            if not matched and (join_type == 'LEFT' or join_type == 'FULL'):
                 nulls = [Value(col.data_type, None) for col in inner_schema.columns]
                 merged_values = outer_row.values + nulls
                 joined_rows.append(Row(merged_values))

        if join_type == 'RIGHT' or join_type == 'FULL':
             for inner_row in inner_rows:
                 if inner_row.row_id not in matched_inner_ids:
                       nulls = [Value(col.data_type, None) for col in outer_schema.columns]
                       merged_values = nulls + inner_row.values
                       joined_rows.append(Row(merged_values))

        return joined_rows

    def _check_join_condition(self, outer: Row, inner: Row, cond: Dict, outer_schema: TableSchema, inner_schema: TableSchema) -> bool:
        """Evaluate join condition"""
        if not cond: return True
        
        left = cond['left']
        right = cond['right']
        operator = cond['operator']
        
        # Get values
        val1 = self._get_cond_value(left, outer, inner, outer_schema, inner_schema)
        val2 = self._get_cond_value(right, outer, inner, outer_schema, inner_schema)
        
        if not val1 or not val2: return False
        
        return val1.compare(val2, operator)

    def _get_cond_value(self, operand: Dict, outer: Row, inner: Row, outer_sch: TableSchema, inner_sch: TableSchema) -> Value:
        if operand['type'] == 'literal':
            return operand['value']
        elif operand['type'] == 'column':
            col_name = operand['name']
            table_alias = operand.get('table')
            
            # Try to find in outer
            idx = -1
            if not table_alias or table_alias == outer_sch.name:
                idx = outer_sch.get_column_index(col_name)
                if idx != -1: return outer.get_value(idx)
                    
            # Try in inner
            if not table_alias or table_alias == inner_sch.name:
                idx = inner_sch.get_column_index(col_name)
                if idx != -1: return inner.get_value(idx)
                
        return None

    def _merge_schemas(self, s1: TableSchema, s2: TableSchema) -> TableSchema:
        """Create a merged schema (temporary for join processing)"""
        # This is strictly for column/index resolution during execution
        from ..catalog.schema import TableSchema as TS, Column
        
        merged_cols = []
        seen_names = set()
        
        # Add s1 columns
        for col in s1.columns:
            new_name = col.name
            if new_name in seen_names:
                new_name = f"{s1.name}.{col.name}" 
            seen_names.add(new_name)
            merged_cols.append(Column(new_name, col.data_type, col.max_length, col.constraints, col.default_value))
            
        # Add s2 columns
        for col in s2.columns:
            new_name = col.name
            if new_name in seen_names:
                new_name = f"{s2.name}.{col.name}"
                if new_name in seen_names:
                     new_name = f"{s2.name}.{col.name}_2"
            seen_names.add(new_name)
            merged_cols.append(Column(new_name, col.data_type, col.max_length, col.constraints, col.default_value))

        return TS(f"{s1.name}_{s2.name}", merged_cols)