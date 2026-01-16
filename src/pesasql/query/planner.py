"""
Query Planner - Creates execution plans from AST
"""

from typing import List, Dict, Any
from ..parser.ast import *
from ..catalog.schema import TableSchema, Column as SchemaColumn
from ..catalog.catalog import Catalog
from ..storage.index.index_manager import IndexManager
from ..types.value import Value, Type
from .exceptions import PesaSQLExecutionError


class QueryPlan:
    """Execution plan for a query"""

    def __init__(self, plan_type: str, details: Dict[str, Any] = None):
        self.plan_type = plan_type  # 'SELECT', 'INSERT', 'CREATE_TABLE', etc.
        self.details = details or {}

    def __repr__(self):
        return f"QueryPlan({self.plan_type}, {self.details})"


class Planner:
    """Creates execution plans from AST"""

    def __init__(self, catalog: Catalog, index_manager: IndexManager = None):
        self.catalog = catalog
        self.index_manager = index_manager

    def plan(self, ast: Node) -> QueryPlan:
        """Create execution plan from AST"""
        if isinstance(ast, SelectStatement):
            return self.plan_select(ast)
        elif isinstance(ast, InsertStatement):
            return self.plan_insert(ast)
        elif isinstance(ast, CreateTableStatement):
            return self.plan_create_table(ast)
        elif isinstance(ast, DropTableStatement):
            return self.plan_drop_table(ast)
        else:
            # Handle simple commands
            command_type = type(ast).__name__
            return QueryPlan(command_type, {'ast': ast})

    def plan_select(self, stmt: SelectStatement) -> QueryPlan:
        """Plan SELECT query"""
        # Get table schema
        table_schema = self.catalog.get_table(stmt.table_name)
        if not table_schema:
            raise PesaSQLExecutionError(f"Table '{stmt.table_name}' not found")

        # Validate columns
        column_indices = []
        column_names = []

        if stmt.columns and stmt.columns[0].name == '*':
            # Select all columns (including joins)
            column_indices = list(range(len(table_schema.columns)))
            column_names = [col.name for col in table_schema.columns]
            
            # Add columns from joined tables
            current_offset = len(table_schema.columns)
            if stmt.joins:
                for join in stmt.joins:
                    join_schema = self.catalog.get_table(join.table_name)
                    if join_schema:
                        indices = list(range(current_offset, current_offset + len(join_schema.columns)))
                        column_indices.extend(indices)
                        column_names.extend([f"{join.table_name}.{col.name}" for col in join_schema.columns])
                        current_offset += len(join_schema.columns)
        else:
            # Select specific columns
            for col in stmt.columns:
                found = False
                for i, schema_col in enumerate(table_schema.columns):
                    if schema_col.name == col.name:
                        column_indices.append(i)
                        column_names.append(col.name)
                        found = True
                        break
                if not found:
                    raise PesaSQLExecutionError(f"Column '{col.name}' not found in table '{stmt.table_name}'")

        # Plan WHERE clause
        filter_conditions = []
        if stmt.where_clause:
            filter_conditions = self._extract_conditions(stmt.where_clause, table_schema)

        # Check for Index Scan opportunity
        access_method = 'SEQ_SCAN'
        index_name = None
        index_conditions = []

        if self.index_manager and filter_conditions:
            for condition in filter_conditions:
                column_name = condition['column_name']
                operator = condition['operator']
                
                # Check directly if index exists for this column
                # Note: condition must be index-compatible (e.g. =, <, >, <=, >=)
                # AND it must not be negated (simple check)
                
                # Supported index operators
                if operator in ('=', '<', '>', '<=', '>='):
                     if self.index_manager.has_index(table_schema.name, column_name):
                         access_method = 'INDEX_SCAN'
                         # We use the index name from manager convention: "table.col"
                         index_name = f"{table_schema.name}.{column_name}"
                         # We'll rely on all conditions being checked, but the index scan 
                         # will efficiently filter by this specific condition first.
                         # In a real planner we'd separate index keys from residual predicates.
                         # For now, we pass all conditions to executor, but mark the index to use.
                         # IMPORTANT: We only pick ONE index.
                         break

        # Process Joins
        planned_joins = []
        if stmt.joins:
            for join in stmt.joins:
                join_table_schema = self.catalog.get_table(join.table_name)
                if not join_table_schema:
                    raise PesaSQLExecutionError(f"Table '{join.table_name}' not found")
                
                # Check for index on join condition for the inner table?
                # Simplified: Assume Inner Loop is SEQ_SCAN for now unless we do sophisticated planning.
                # Ideally we check index on join column in T2.
                # For now, just pass the info.
                
                planned_joins.append({
                    'table_name': join.table_name,
                    'join_type': join.join_type.value,
                    'on_condition': self._extract_condition(join.on_condition), # Transform expression to dict
                    'table_schema': join_table_schema
                })

        return QueryPlan('SELECT', {
            'table_name': stmt.table_name,
            'table_schema': table_schema,
            'column_indices': column_indices,
            'column_names': column_names,
            'filter_conditions': filter_conditions,
            'limit': stmt.limit,
            'offset': stmt.offset,
            'order_by': stmt.order_by,
            'access_method': access_method,
            'index_name': index_name,
            'joins': planned_joins
        })

    def _extract_condition(self, expr: Expression) -> Dict[str, Any]:
        """Extract simple condition from expression (Helper for Joins)"""
        # Simplified: assumes BinaryExpression(Column, Op, Column/Literal)
        if isinstance(expr, BinaryExpression):
            return {
                'left': self._extract_operand(expr.left),
                'operator': expr.operator,
                'right': self._extract_operand(expr.right)
            }
        return {'raw': str(expr)} # Fallback

    def _extract_operand(self, expr: Expression) -> Dict[str, Any]:
        """Extract operand info"""
        if isinstance(expr, ColumnExpression):
            return {'type': 'column', 'name': expr.column.name, 'table': expr.column.table_alias}
        elif isinstance(expr, LiteralExpression):
            return {'type': 'literal', 'value': expr.literal.to_value(), 'data_type': expr.literal.value_type}
        return {'type': 'unknown'}


    def plan_insert(self, stmt: InsertStatement) -> QueryPlan:
        """Plan INSERT query"""
        # Get table schema
        table_schema = self.catalog.get_table(stmt.table_name)
        if not table_schema:
            raise PesaSQLExecutionError(f"Table '{stmt.table_name}' not found")

        # Validate columns
        column_indices = []
        if stmt.columns:
            # Insert with specified columns
            for col_name in stmt.columns:
                found = False
                for i, schema_col in enumerate(table_schema.columns):
                    if schema_col.name == col_name:
                        column_indices.append(i)
                        found = True
                        break
                if not found:
                    raise PesaSQLExecutionError(f"Column '{col_name}' not found in table '{stmt.table_name}'")
        else:
            # Insert into all columns
            column_indices = list(range(len(table_schema.columns)))

        # Validate values
        for value_list in stmt.values:
            if len(value_list) != len(column_indices):
                raise PesaSQLExecutionError(
                    f"Expected {len(column_indices)} values, got {len(value_list)}"
                )

            # Type checking would happen during execution

        return QueryPlan('INSERT', {
            'table_name': stmt.table_name,
            'table_schema': table_schema,
            'column_indices': column_indices,
            'values_ast': stmt.values  # Will be evaluated during execution
        })

    def plan_create_table(self, stmt: CreateTableStatement) -> QueryPlan:
        """Plan CREATE TABLE query"""
        # Check if table exists
        if not stmt.if_not_exists and self.catalog.get_table(stmt.table_name):
            raise PesaSQLExecutionError(f"Table '{stmt.table_name}' already exists")

        # Convert AST column definitions to schema columns
        from ..catalog.schema import Column as SchemaColumn, DataType, ColumnConstraint

        columns = []
        for col_def in stmt.columns:
            # Parse data type
            data_type_str = col_def.data_type.upper()
            if data_type_str.startswith('INT'):
                data_type = DataType.INTEGER
                max_length = 0
            elif data_type_str.startswith('STRING'):
                data_type = DataType.STRING
                # Extract length
                if '(' in data_type_str and ')' in data_type_str:
                    length_str = data_type_str[data_type_str.find('(') + 1:data_type_str.find(')')]
                    max_length = int(length_str)
                else:
                    max_length = 255
            elif data_type_str == 'FLOAT':
                data_type = DataType.FLOAT
                max_length = 0
            elif data_type_str == 'DOUBLE':
                data_type = DataType.DOUBLE
                max_length = 0
            elif data_type_str in ['BOOLEAN', 'BOOL']:
                data_type = DataType.BOOLEAN
                max_length = 0
            elif data_type_str == 'TIMESTAMP':
                data_type = DataType.TIMESTAMP
                max_length = 0
            else:
                raise PesaSQLExecutionError(f"Unknown data type: {col_def.data_type}")

            # Parse constraints
            constraints = []
            for constr in col_def.constraints:
                if constr == 'PRIMARY KEY':
                    constraints.append(ColumnConstraint.PRIMARY_KEY)
                elif constr == 'UNIQUE':
                    constraints.append(ColumnConstraint.UNIQUE)
                elif constr == 'NOT NULL':
                    constraints.append(ColumnConstraint.NOT_NULL)

            columns.append(SchemaColumn(
                name=col_def.name,
                data_type=data_type,
                max_length=max_length,
                constraints=constraints
            ))

        return QueryPlan('CREATE_TABLE', {
            'table_name': stmt.table_name,
            'columns': columns,
            'if_not_exists': stmt.if_not_exists
        })

    def plan_drop_table(self, stmt: DropTableStatement) -> QueryPlan:
        """Plan DROP TABLE query"""
        # Check if table exists
        if not stmt.if_exists and not self.catalog.get_table(stmt.table_name):
            raise PesaSQLExecutionError(f"Table '{stmt.table_name}' not found")

        return QueryPlan('DROP_TABLE', {
            'table_name': stmt.table_name,
            'if_exists': stmt.if_exists
        })

    def _extract_conditions(self, expr: Expression, table_schema: TableSchema) -> List[Dict[str, Any]]:
        """Extract filter conditions from WHERE clause"""
        conditions = []

        if isinstance(expr, BinaryExpression):
            if expr.operator.upper() in ('AND', 'OR'):
                # Recursively process both sides
                conditions.extend(self._extract_conditions(expr.left, table_schema))
                conditions.extend(self._extract_conditions(expr.right, table_schema))
            else:
                # Simple comparison
                if isinstance(expr.left, ColumnExpression):
                    column_name = expr.left.column.name
                    column_index = self._find_column_index(column_name, table_schema)

                    if isinstance(expr.right, LiteralExpression):
                        value = expr.right.literal.to_value()
                        conditions.append({
                            'column_index': column_index,
                            'column_name': column_name,
                            'operator': expr.operator,
                            'value': value,
                            'connector': 'AND'  # Default
                        })

        return conditions

    def _find_column_index(self, column_name: str, table_schema: TableSchema) -> int:
        """Find column index by name"""
        for i, col in enumerate(table_schema.columns):
            if col.name == column_name:
                return i
        raise PesaSQLExecutionError(f"Column '{column_name}' not found")