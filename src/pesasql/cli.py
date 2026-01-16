"""
Command Line Interface Module - Updated with Query Engine integration
"""

import cmd
import os


from .storage.file_manager import FileManager
from .catalog.catalog import Catalog
from .query.engine import QueryEngine
from .parser.exceptions import PesaSQLParseError
from .query.exceptions import PesaSQLExecutionError


class PesaSQLREPL(cmd.Cmd):
    """Interactive REPL for PesaSQL database - Updated for Week 3"""

    intro = """
    ╔══════════════════════════════════════╗
    ║      PesaSQL Database System         ║
    ║      Version 0.3 - Week 3            ║
    ║      SQL Parser & Query Engine       ║
    ║      Type 'help' for commands        ║
    ╚══════════════════════════════════════╝
    
    Now with SQL support:
      SELECT * FROM table [WHERE condition]
      INSERT INTO table VALUES (val1, val2, ...)
      CREATE DATABASE name
      USE name
    """
    prompt = "pesasql> "

    def __init__(self):
        super().__init__()
        self.engine = None
        self.db_path = None

    def do_quit(self, arg):
        """Exit the REPL"""
        print("Goodbye!")
        return True

    def do_exit(self, arg):
        """Exit the REPL"""
        return self.do_quit(arg)

    def do_EOF(self, arg):
        """Exit on Ctrl-D"""
        print()
        return self.do_quit(arg)

    def _create_database(self, db_name):
        """Create a new database"""
        if not db_name.endswith('.db'):
            db_name += '.db'
        
        path = os.path.join(os.getcwd(), db_name)
        try:
            fm = FileManager(path)
            fm.create_database()
            print(f"Database created: {db_name}")
            # Auto-use
            self._use_database(db_name)
        except Exception as e:
            print(f"Error creating database: {e}")

    def _use_database(self, db_name):
        """Switch to a database"""
        if not db_name.endswith('.db'):
            db_name += '.db'
            
        path = os.path.join(os.getcwd(), db_name)
        if not os.path.exists(path):
            print(f"Database not found: {db_name}")
            return

        try:
            fm = FileManager(path)
            catalog = Catalog(fm)
            self.engine = QueryEngine(fm, catalog)
            self.db_path = path
            self.prompt = f"pesasql({db_name})> "
            print(f"Using database: {db_name}")
            
            # Load catalog
            print(f"Loaded {len(catalog.tables)} tables from catalog")
        except Exception as e:
            print(f"Error loading database: {e}")

    def do_describe(self, table_name):
        """Describe a table schema"""
        if not self.engine:
            print("No database selected")
            return

        schema = self.engine.catalog.get_table(table_name)
        if not schema:
            print(f"Table '{table_name}' not found")
            return

        print(f"\nTable: {schema.name}")
        print("-" * 40)
        print(f"{'Column':<20} | {'Type':<15}")
        print("-" * 40)
        
        for col in schema.columns:
            type_str = col.data_type.name
            if col.length > 0:
                type_str += f"({col.length})"
            print(f"{col.name:<20} | {type_str:<15}")
        print()

    def _show_tables(self):
        """Show all tables"""
        if not self.engine:
            print("No database selected")
            return

        tables = list(self.engine.catalog.tables.keys())
        if not tables:
            print("No tables found")
            return

        print("\nTables in database:")
        print("-" * 20)
        for table in tables:
            print(table)
        print()

    def default(self, line: str) -> None:
        """
        Handle SQL commands
        """
        # Handle special SQL commands that affect CLI state but are parsed by Engine
        # We need a temporary engine to parse commands even if no DB is selected?
        # Actually, Parser is static. CLI should probably use Parser directly for these?
        # But for now, we rely on engine. 
        # CAUTION: If self.engine is None, we can't call execute_sql.
        
        # We need to handle CREATE DATABASE / USE manually if no engine?
        # Or create a dummy engine?
        # Better: Check for CREATE DATABASE / USE simply here, or use Parser directly.
        
        lower_line = line.strip().lower()
        if lower_line.startswith('create database'):
            parts = line.strip().split()
            if len(parts) >= 3:
                self._create_database(parts[2])
            return
        elif lower_line.startswith('use'):
            parts = line.strip().split()
            if len(parts) >= 2:
                self._use_database(parts[1])
            return

        if not self.engine:
            print("No database selected. Use CREATE DATABASE or USE first")
            return

        try:
            result = self.engine.execute_sql(line)
            
            # Handle commands delegated back from engine (e.g. parsed but not executed)
            if isinstance(result, dict) and 'command' in result:
                cmd = result['command']
                if cmd == 'CREATE_DATABASE':
                    self._create_database(result['db_name'])
                elif cmd == 'USE':
                    self._use_database(result['db_name'])
                elif cmd == 'DESCRIBE':
                    self.do_describe(result['table_name'])
                elif cmd == 'SHOW_TABLES':
                    self._show_tables()
                elif cmd == 'SHOW_DATABASES':
                    # Not implemented yet
                    print("SHOW DATABASES not implemented")
            else:
                self._display_result(result)
                
        except PesaSQLParseError as e:
            print(f"Parse error: {e}")
        except PesaSQLExecutionError as e:
            print(f"Execution error: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def _display_result(self, result: dict) -> None:
        """Display query result"""
        if result.get('plan_type') == 'SELECT':
            self._display_select_result(result)
        elif 'rows_inserted' in result:
            print(f"Inserted {result['rows_inserted']} row(s)")
        elif result.get('status') == 'created':
             print(f"Table '{result.get('table_name')}' created")
        elif result.get('status') == 'dropped':
             print(f"Table '{result.get('table_name')}' dropped")
        else:
            print(f"Result: {result}")

    def _display_select_result(self, result: dict) -> None:
        """Display SELECT query result in table format"""
        columns = result['columns']
        data = result['data']

        if not data:
            print("Empty result set")
            return

        # Calculate column widths
        col_widths = []
        for i, col in enumerate(columns):
            max_len = len(str(col))
            for row in data:
                max_len = max(max_len, len(str(row[i])))
            col_widths.append(min(max_len, 50))  # Cap at 50 chars

        # Print header
        header = " | ".join(f"{col:<{width}}" for col, width in zip(columns, col_widths))
        separator = "-+-".join("-" * width for width in col_widths)

        print(header)
        print(separator)

        # Print rows
        for row in data:
            row_str = " | ".join(f"{str(val):<{width}}" for val, width in zip(row, col_widths))
            print(row_str)

        print(f"\n{len(data)} row(s) returned")


def main():
    repl = PesaSQLREPL()
    try:
        repl.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()