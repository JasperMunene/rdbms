"""
Query Engine - Main interface for executing SQL queries
"""

from typing import Any, List
from ..parser.parser import Parser
from ..parser.exceptions import PesaSQLParseError, PesaSQLSyntaxError
from ..catalog.catalog import Catalog
from ..storage.file_manager import FileManager
from .planner import Planner
from .executor import Executor, Row
from .exceptions import PesaSQLExecutionError


class QueryEngine:
    """Main query engine that coordinates parsing, planning, and execution"""

    def __init__(self, file_manager: FileManager, catalog: Catalog):
        self.file_manager = file_manager
        self.catalog = catalog
        self.planner = Planner(catalog)
        self.executor = Executor(file_manager, catalog)

    def execute_sql(self, sql: str) -> Any:
        """
        Execute SQL query

        Args:
            sql: SQL query string

        Returns:
            Query result (list of rows for SELECT, status dict for others)

        Raises:
            PesaSQLParseError: For syntax errors
            PesaSQLExecutionError: For execution errors
        """
        # Parse SQL
        try:
            ast = Parser.parse_sql(sql.strip())
        except PesaSQLSyntaxError as e:
            raise PesaSQLParseError(f"Syntax error: {e}")

        # Check for simple commands (handled by CLI)
        if hasattr(ast, 'db_name'):  # CREATE DATABASE
            return {'command': 'CREATE_DATABASE', 'db_name': ast.db_name}
        elif hasattr(ast, 'table_name') and type(ast).__name__ == 'DescribeTable':
            return {'command': 'DESCRIBE', 'table_name': ast.table_name}
        elif type(ast).__name__ == 'ShowTables':
            return {'command': 'SHOW_TABLES'}
        elif type(ast).__name__ == 'ShowDatabases':
            return {'command': 'SHOW_DATABASES'}
        elif hasattr(ast, 'db_name') and type(ast).__name__ == 'UseDatabase':
            return {'command': 'USE', 'db_name': ast.db_name}

        # Plan query
        plan = self.planner.plan(ast)

        # Execute query
        try:
            # Inject plan_type into details
            execution_plan = plan.details.copy()
            execution_plan['plan_type'] = plan.plan_type
            
            result = self.executor.execute(execution_plan)

            if plan.plan_type == 'SELECT':
                return self._format_select_result(result, plan.details)
            else:
                return result

        except PesaSQLExecutionError as e:
            raise
        except Exception as e:
            raise PesaSQLExecutionError(f"Execution error: {e}")

    def _format_select_result(self, rows: List[Row], plan_details: dict) -> dict:
        """Format SELECT results for display"""
        column_names = plan_details.get('column_names', [])

        # Convert rows to list of lists
        data = []
        for row in rows:
            row_data = []
            for value in row.values:
                # Format based on type
                if value.type.name == 'TIMESTAMP' and value.value:
                    from datetime import datetime
                    if isinstance(value.value, datetime):
                        formatted = value.value.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        formatted = str(value.value)
                else:
                    formatted = str(value.value)
                row_data.append(formatted)
            data.append(row_data)

        return {
            'columns': column_names,
            'data': data,
            'row_count': len(rows),
            'plan_type': 'SELECT'
        }