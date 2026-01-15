"""
Command Line Interface Module - Interactive REPL for PesaSQL
Provides SQL-like interface for database operations.
"""

import cmd
import sys
import shlex
from typing import List
from pathlib import Path

from .storage.file_manager import FileManager
from .catalog.catalog import Catalog
from .catalog.schema import TableSchema, Column, DataType, ColumnConstraint
from .storage.buffer_pool import BufferPool


class PesaSQLREPL(cmd.Cmd):
    """Interactive REPL for PesaSQL database"""

    intro = """
    ╔══════════════════════════════════════╗
    ║      PesaSQL Database System         ║
    ║      Version 0.2 -  Day 1            ║
    ║      Type 'help' for commands        ║
    ╚══════════════════════════════════════╝

    Welcome to PesaSQL! A lightweight RDBMS.
    """
    prompt = "pesasql> "

    def __init__(self):
        """Initialize REPL"""
        super().__init__()
        self.file_manager = None
        self.catalog = None
        self.buffer_pool = BufferPool(capacity=50)
        self.current_db = None

    def parseline(self, line):
        """Parse the line into a command name and string of arguments."""
        cmd, arg, line = super().parseline(line)
        if cmd:
            cmd = cmd.lower()
        return cmd, arg, line

    def do_create(self, arg: str) -> None:
        """
        CREATE DATABASE <name> - Create new database
        CREATE TABLE <name> (col1 type, col2 type, ...) - Create new table

        Examples:
          CREATE DATABASE mydb
          CREATE TABLE users (id INT PRIMARY KEY, name STRING(100), balance DOUBLE)
        """
        args = shlex.split(arg)

        if len(args) < 2:
            print("Syntax: CREATE DATABASE <name>")
            print("        CREATE TABLE <name> (col1 type, ...)")
            return

        command = args[0].upper()

        if command == "DATABASE":
            self._create_database(args[1])
        elif command == "TABLE":
            self._create_table(args[1:])
        else:
            print(f"Unknown CREATE command: {command}")

    def _create_database(self, db_name: str) -> None:
        """Create new database"""
        try:
            # Ensure .db extension
            if not db_name.endswith('.db'):
                db_name += '.db'

            # Check if exists
            if Path(db_name).exists():
                print(f"Database '{db_name}' already exists")
                return

            # Create database
            self.file_manager = FileManager(db_name)
            self.file_manager.create_database()

            # Initialize catalog
            self.catalog = Catalog(self.file_manager)
            self.current_db = db_name

            print(f"Database '{db_name}' created successfully")
            print(f"Using buffer pool: {self.buffer_pool}")

        except Exception as e:
            print(f"Error creating database: {e}")

    def _create_table(self, args: List[str]) -> None:
        """Create new table"""
        if not self.catalog:
            print("No database selected. Use CREATE DATABASE first")
            return

        if len(args) < 2:
            print("Syntax: CREATE TABLE <name> (col1 type, ...)")
            return

        table_name = args[0]

        # Parse column definitions (simplified parser)
        try:
            # Join remaining args and extract column definitions
            col_defs = ' '.join(args[1:])

            # Remove parentheses if present
            if col_defs.startswith('(') and col_defs.endswith(')'):
                col_defs = col_defs[1:-1]

            # Split by comma
            col_parts = [c.strip() for c in col_defs.split(',')]

            columns = []
            for col_def in col_parts:
                col_def = col_def.strip()
                if not col_def:
                    continue

                # Parse column definition
                # Format: name type [constraints]
                parts = col_def.split()
                if len(parts) < 2:
                    print(f"Invalid column definition: {col_def}")
                    return

                col_name = parts[0]
                col_type_str = parts[1].upper()

                # Parse type
                if col_type_str.startswith('STRING'):
                    # Extract length
                    if '(' in col_type_str and ')' in col_type_str:
                        length_str = col_type_str[col_type_str.find('(') + 1:col_type_str.find(')')]
                        try:
                            max_length = int(length_str)
                        except:
                            max_length = 255
                        data_type = DataType.STRING
                    else:
                        data_type = DataType.STRING
                        max_length = 255
                elif col_type_str == 'INT' or col_type_str == 'INTEGER':
                    data_type = DataType.INTEGER
                    max_length = 0
                elif col_type_str == 'FLOAT':
                    data_type = DataType.FLOAT
                    max_length = 0
                elif col_type_str == 'DOUBLE':
                    data_type = DataType.DOUBLE
                    max_length = 0
                elif col_type_str == 'BOOLEAN' or col_type_str == 'BOOL':
                    data_type = DataType.BOOLEAN
                    max_length = 0
                elif col_type_str == 'TIMESTAMP':
                    data_type = DataType.TIMESTAMP
                    max_length = 0
                else:
                    print(f"Unknown data type: {col_type_str}")
                    return

                # Parse constraints
                constraints = []
                for part in parts[2:]:
                    part_upper = part.upper()
                    if part_upper == 'PRIMARY' and 'KEY' in [p.upper() for p in parts]:
                        constraints.append(ColumnConstraint.PRIMARY_KEY)
                    elif part_upper == 'UNIQUE':
                        constraints.append(ColumnConstraint.UNIQUE)
                    elif part_upper == 'NOT' and 'NULL' in [p.upper() for p in parts]:
                        constraints.append(ColumnConstraint.NOT_NULL)
                    elif part_upper == 'PRIMARY_KEY':
                        constraints.append(ColumnConstraint.PRIMARY_KEY)

                # Create column
                column = Column(
                    name=col_name,
                    data_type=data_type,
                    max_length=max_length,
                    constraints=constraints
                )
                columns.append(column)

            # Create table schema
            table_schema = TableSchema(name=table_name, columns=columns)

            # Add to catalog
            if self.catalog.create_table(table_schema):
                print(f"Table '{table_name}' created successfully")
            else:
                print(f"Failed to create table '{table_name}'")

        except Exception as e:
            print(f"Error creating table: {e}")

    def do_use(self, arg: str) -> None:
        """
        USE <database> - Open existing database

        Example:
          USE mydb
        """
        if not arg:
            print("Syntax: USE <database>")
            return

        db_name = arg.strip()
        if not db_name.endswith('.db'):
            db_name += '.db'

        try:
            if not Path(db_name).exists():
                print(f"Database '{db_name}' not found")
                return

            # Open database
            self.file_manager = FileManager(db_name)
            self.catalog = Catalog(self.file_manager)
            self.current_db = db_name

            print(f"Using database '{db_name}'")
            print(f"Tables: {len(self.catalog.list_tables())}")

        except Exception as e:
            print(f"Error opening database: {e}")

    def do_show(self, arg: str) -> None:
        """
        SHOW TABLES - List all tables
        SHOW DATABASES - List all databases in current directory
        """
        arg = arg.upper()

        if arg == "TABLES":
            self._show_tables()
        elif arg == "DATABASES":
            self._show_databases()
        else:
            print("Syntax: SHOW TABLES")
            print("        SHOW DATABASES")

    def _show_tables(self) -> None:
        """List all tables in current database"""
        if not self.catalog:
            print("No database selected")
            return

        tables = self.catalog.list_tables()
        if not tables:
            print("No tables in database")
            return

        print("\nTables in database:")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"{i:3}. {table}")
        print("-" * 40)
        print(f"Total: {len(tables)} table(s)")

    def _show_databases(self) -> None:
        """List all .db files in current directory"""
        db_files = list(Path('.').glob('*.db'))

        if not db_files:
            print("No databases found in current directory")
            return

        print("\nDatabases in current directory:")
        print("-" * 40)
        for i, db_file in enumerate(db_files, 1):
            size_kb = db_file.stat().st_size / 1024
            current = " (current)" if str(db_file) == self.current_db else ""
            print(f"{i:3}. {db_file.name} ({size_kb:.1f} KB){current}")
        print("-" * 40)

    def do_describe(self, arg: str) -> None:
        """
        DESCRIBE <table> - Show table structure

        Example:
          DESCRIBE users
        """
        if not arg:
            print("Syntax: DESCRIBE <table>")
            return

        if not self.catalog:
            print("No database selected")
            return

        table_name = arg.strip()
        description = self.catalog.describe_table(table_name)

        if description:
            print(f"\n{description}")
        else:
            print(f"Table '{table_name}' not found")

    def do_drop(self, arg: str) -> None:
        """
        DROP TABLE <name> - Remove table from database

        Example:
          DROP TABLE users
        """
        args = shlex.split(arg)

        if len(args) < 2:
            print("Syntax: DROP TABLE <name>")
            return

        command = args[0].upper()

        if command == "TABLE":
            table_name = args[1]
            self._drop_table(table_name)
        else:
            print(f"Unknown DROP command: {command}")

    def _drop_table(self, table_name: str) -> None:
        """Drop table from database"""
        if not self.catalog:
            print("No database selected")
            return

        # Confirm
        confirm = input(f"Are you sure you want to drop table '{table_name}'? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Drop cancelled")
            return

        if self.catalog.drop_table(table_name):
            print(f"Table '{table_name}' dropped")
        else:
            print(f"Failed to drop table '{table_name}'")

    def do_info(self, arg: str) -> None:
        """
        INFO - Show database information
        INFO CATALOG - Show catalog information
        INFO BUFFER - Show buffer pool statistics
        """
        if not self.file_manager:
            print("No database selected")
            return

        arg = arg.upper() if arg else ""

        if arg == "CATALOG":
            self._show_catalog_info()
        elif arg == "BUFFER" or arg == "BUFFERPOOL":
            self._show_buffer_info()
        else:
            self._show_database_info()

    def _show_database_info(self) -> None:
        """Show database information"""
        info = self.file_manager.get_database_info()

        print("\nDatabase Information:")
        print("-" * 50)
        print(f"File: {self.current_db}")
        print(f"Size: {info['file_size'] / 1024:.1f} KB")
        print(f"Page Size: {info['page_size']} bytes")
        print(f"Total Pages: {info['total_pages']}")
        print(f"Free List Head: {info['free_list_head']}")
        print(f"WAL Size: {info['wal_size']} bytes")
        print(f"Magic: {info['magic_string']}")
        print("-" * 50)

    def _show_catalog_info(self) -> None:
        """Show catalog information"""
        if not self.catalog:
            print("No catalog loaded")
            return

        info = self.catalog.get_catalog_info()

        print("\nCatalog Information:")
        print("-" * 50)
        print(f"Tables: {info['table_count']}")
        print(f"Catalog Pages: {info['catalog_pages']}")
        print(f"Memory Usage: {info['memory_size']} bytes")
        print("\nTable List:")
        for i, table in enumerate(info['tables'], 1):
            print(f"  {i:2}. {table}")
        print("-" * 50)

    def _show_buffer_info(self) -> None:
        """Show buffer pool information"""
        stats = self.buffer_pool.get_stats()

        print("\nBuffer Pool Information:")
        print("-" * 50)
        print(f"Capacity: {stats['capacity']} pages")
        print(f"Current Size: {stats['current_size']} pages")
        print(f"Hits: {stats['hits']}")
        print(f"Misses: {stats['misses']}")
        print(f"Hit Ratio: {stats['hit_ratio']}")
        print(f"Evictions: {stats['evictions']}")
        print(f"Pinned Pages: {stats['pinned_pages']}")
        print("-" * 50)

    def do_insert(self, arg: str) -> None:
        """
        INSERT INTO <table> VALUES (val1, val2, ...) - Insert data

        Example:
          INSERT INTO users VALUES (1, 'John', 100.50)

        Note: Basic placeholder for Week 2
        """
        print("INSERT command placeholder - To be implemented in Week 3")
        print("This will insert data into the specified table")

    def do_select(self, arg: str) -> None:
        """
        SELECT * FROM <table> - Query data

        Example:
          SELECT * FROM users

        Note: Basic placeholder for Week 2
        """
        print("SELECT command placeholder - To be implemented in Week 3")
        print("This will query data from the specified table")

    def do_quit(self, arg: str) -> None:
        """QUIT - Exit PesaSQL"""
        print("\nThank you for using PesaSQL!")
        if self.buffer_pool and self.file_manager:
            self.buffer_pool.clear(self.file_manager)
        return True

    def do_exit(self, arg: str) -> None:
        """EXIT - Exit PesaSQL"""
        return self.do_quit(arg)

    def do_help(self, arg: str) -> None:
        """HELP - Show available commands"""
        commands = {
            "CREATE": "Create database or table",
            "USE": "Open existing database",
            "SHOW": "Show tables or databases",
            "DESCRIBE": "Show table structure",
            "DROP": "Drop table",
            "INFO": "Show database/catalog/buffer information",
            "INSERT": "Insert data into table (Week 3)",
            "SELECT": "Query data from table (Week 3)",
            "QUIT/EXIT": "Exit PesaSQL",
            "HELP": "Show this help"
        }

        print("\nAvailable Commands:")
        print("-" * 60)
        for cmd, desc in commands.items():
            print(f"{cmd:15} - {desc}")
        print("\nExamples:")
        print("  CREATE DATABASE mydb")
        print("  USE mydb")
        print("  CREATE TABLE users (id INT PRIMARY KEY, name STRING(100))")
        print("  SHOW TABLES")
        print("  DESCRIBE users")
        print("-" * 60)

    # Aliases
    do_q = do_quit
    do_EOF = do_quit  # Ctrl-D support


def main():
    """Main entry point for PesaSQL REPL"""
    print(PesaSQLREPL.intro)

    # Check for command line arguments
    if len(sys.argv) > 1:
        # Non-interactive mode (future)
        print(f"Command line mode not yet implemented")
        return

    # Start interactive REPL
    repl = PesaSQLREPL()
    repl.cmdloop()


if __name__ == "__main__":
    main()