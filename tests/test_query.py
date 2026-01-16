
import pytest
from pesasql.storage.file_manager import FileManager
from pesasql.catalog.catalog import Catalog
from pesasql.query.engine import QueryEngine
from pesasql.query.executor import Executor
import inspect
print(f"DEBUG: Executor file: {inspect.getfile(Executor)}")

@pytest.fixture
def db_context(tmp_path):
    db_path = tmp_path / "test_query.db"
    fm = FileManager(str(db_path))
    fm.create_database()
    catalog = Catalog(fm)
    engine = QueryEngine(fm, catalog)
    return engine

def test_variable_length_rows(db_context):
    engine = db_context

    # Create table
    engine.execute_sql("CREATE TABLE users (id INT, name STRING(100))")

    # Insert rows with different string lengths
    engine.execute_sql("INSERT INTO users VALUES (1, 'Short')")
    engine.execute_sql("INSERT INTO users VALUES (2, 'A very long string that definitely takes up more space')")
    engine.execute_sql("INSERT INTO users VALUES (3, 'Medium')")

    # Select all
    result = engine.execute_sql("SELECT * FROM users")
    
    # Verify count
    assert result['row_count'] == 3
    
    # Verify data integrity
    rows = result['data']
    assert rows[0][1] == "Short"
    assert rows[1][1] == "A very long string that definitely takes up more space"
    assert rows[2][1] == "Medium"

def test_insert_select_types(db_context):
    engine = db_context
    engine.execute_sql("CREATE TABLE products (id INT, price DOUBLE, active BOOLEAN)")
    
    engine.execute_sql("INSERT INTO products VALUES (1, 10.50, TRUE)")
    engine.execute_sql("INSERT INTO products VALUES (2, 20.00, FALSE)")
    
    result = engine.execute_sql("SELECT * FROM products")
    assert result['row_count'] == 2
    
    rows = result['data']
    assert rows[0][1] == "10.5"
    assert rows[0][2] == "True"
    
def test_filter_query(db_context):
    engine = db_context
    engine.execute_sql("CREATE TABLE scores (id INT, score INT)")
    
    engine.execute_sql("INSERT INTO scores VALUES (1, 10)")
    engine.execute_sql("INSERT INTO scores VALUES (2, 20)")
    engine.execute_sql("INSERT INTO scores VALUES (3, 30)")
    
    result = engine.execute_sql("SELECT * FROM scores WHERE score > 15")
    assert result['row_count'] == 2
    assert result['data'][0][1] == "20"
    assert result['data'][1][1] == "30"
