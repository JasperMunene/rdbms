
import pytest
import os
from pesasql.storage.file_manager import FileManager
from pesasql.catalog.catalog import Catalog
from pesasql.query.engine import QueryEngine


DB_PATH = "test_indexing_joins.db"

@pytest.fixture
def engine():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    if os.path.exists(DB_PATH + ".wal"):
        os.remove(DB_PATH + ".wal")
        
    fm = FileManager(DB_PATH)
    fm.create_database()
    
    cat = Catalog(fm)
    eng = QueryEngine(fm, cat)
    
    yield eng
    
    # Cleanup
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    if os.path.exists(DB_PATH + ".wal"):
        os.remove(DB_PATH + ".wal")

def test_create_index(engine):
    engine.execute_sql("CREATE TABLE users (id INT PRIMARY KEY, name STRING(50))")
    
    # Create index via CLI logic (calling manager directly as parser support for CREATE INDEX is limited currently)
    # But we want to test end-to-end if possible. 
    # Since we implemented do_create_index in CLI, let's use engine.index_manager directly here for "unit" test of the feature
    
    success = engine.index_manager.create_index("users", "name", False, False)
    assert success
    
    # Verify index exists
    assert engine.index_manager.has_index("users", "name")

def test_index_scan_execution(engine):
    engine.execute_sql("CREATE TABLE items (id INT, code STRING(20))")
    engine.index_manager.create_index("items", "code", False, True)
    
    # Insert Data
    engine.execute_sql("INSERT INTO items VALUES (1, 'A100')")
    engine.execute_sql("INSERT INTO items VALUES (2, 'B200')")
    engine.execute_sql("INSERT INTO items VALUES (3, 'C300')")

    
    result = engine.execute_sql("SELECT * FROM items WHERE code = 'B200'")
    assert result['row_count'] == 1
    assert result['data'][0][1] == 'B200'
    
    # Verify it works for non-existent key
    result = engine.execute_sql("SELECT * FROM items WHERE code = 'Z999'")
    assert result['row_count'] == 0

def test_join_execution(engine):
    # Setup Schema
    engine.execute_sql("CREATE TABLE users (id INT, name STRING(20))")
    engine.execute_sql("CREATE TABLE orders (oid INT, uid INT, amount DOUBLE)")
    
    # Insert Data
    engine.execute_sql("INSERT INTO users VALUES (1, 'Alice')")
    engine.execute_sql("INSERT INTO users VALUES (2, 'Bob')")
    
    engine.execute_sql("INSERT INTO orders VALUES (101, 1, 50.0)")
    engine.execute_sql("INSERT INTO orders VALUES (102, 1, 25.0)")
    engine.execute_sql("INSERT INTO orders VALUES (103, 2, 100.0)")
    engine.execute_sql("INSERT INTO orders VALUES (104, 3, 75.0)") # Orphan order
    
    # Test INNER JOIN
    # SELECT * FROM users JOIN orders ON users.id = orders.uid
    sql = "SELECT * FROM users JOIN orders ON users.id = orders.uid"
    result = engine.execute_sql(sql)
    
    # Expect 3 rows: Alice-101, Alice-102, Bob-103
    assert result['row_count'] == 3
    
    # Verify content (Alice)
    alice_rows = [r for r in result['data'] if r[1] == 'Alice']
    assert len(alice_rows) == 2
    
    # Test LEFT JOIN
    # SELECT * FROM users LEFT JOIN orders ON users.id = orders.uid
    # Expect Alice(2 calls), Bob(1 call), plus if we had a user with no orders (let's add one)
    engine.execute_sql("INSERT INTO users VALUES (4, 'Dave')")
    
    result_left = engine.execute_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.uid")
    # Rows: Alice(2), Bob(1), Dave(1 - NULLs)
    assert result_left['row_count'] == 4
    
    dave_row = [r for r in result_left['data'] if r[1] == 'Dave'][0]
    # Check NULLs for order columns (indices 2, 3, 4)
    # QueryEngine formats None as string "None"
    assert dave_row[2] == 'None'
    assert dave_row[4] == 'None'

    # Test RIGHT JOIN
    # SELECT * FROM users RIGHT JOIN orders ON users.id = orders.uid
    # Expect all matched rows + Orphan order (104, uid 3) -> User part NULL
    result_right = engine.execute_sql("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.uid")
    # Rows: Alice(2), Bob(1), Order 104 (1 - NULLs)
    # Note: User 4 (Dave) not included
    assert result_right['row_count'] == 4 # 3 matched + 1 orphan
    
    orphan_order = [r for r in result_right['data'] if r[2] == '104'][0]
    assert orphan_order[0] == 'None'
    assert orphan_order[1] == 'None'

    # Test FULL OUTER JOIN
    # SELECT * FROM users FULL JOIN orders ON users.id = orders.uid
    # Expect: Alice(2), Bob(1, nulls), Dave(1, nulls), Order 104(1, nulls)
    # Total 5 rows
    result_full = engine.execute_sql("SELECT * FROM users FULL JOIN orders ON users.id = orders.uid")
    assert result_full['row_count'] == 5
    
    # Verify we have Dave (Left-only) AND Order 104 (Right-only)
    dave_in_full = [r for r in result_full['data'] if r[1] == 'Dave']
    assert len(dave_in_full) == 1
    
    orphan_in_full = [r for r in result_full['data'] if r[2] == '104']
    assert len(orphan_in_full) == 1


def test_join_on_string_keys(engine):
    """Test joining on String columns (Hash Join stress test)"""
    engine.execute_sql("CREATE TABLE countries (code STRING(3) PRIMARY KEY, name STRING(50))")
    engine.execute_sql("CREATE TABLE cities (id INT, country_code STRING(3), name STRING(50))")
    
    engine.execute_sql("INSERT INTO countries VALUES ('USA', 'United States')")
    engine.execute_sql("INSERT INTO countries VALUES ('KEN', 'Kenya')")
    
    engine.execute_sql("INSERT INTO cities VALUES (1, 'USA', 'New York')")
    engine.execute_sql("INSERT INTO cities VALUES (2, 'KEN', 'Nairobi')")
    engine.execute_sql("INSERT INTO cities VALUES (3, 'USA', 'Chicago')")
    engine.execute_sql("INSERT INTO cities VALUES (4, 'FRA', 'Paris')") # Orphan city
    
    # Join on code
    result = engine.execute_sql("SELECT * FROM countries JOIN cities ON countries.code = cities.country_code")
    
    # Expect: USA-NY, USA-Chi, KEN-Nai. (FRA excluded, USA-US excluded from right side? No inner join)
    assert result['row_count'] == 3
    
    # Verify USA matches
    usa_cities = [r for r in result['data'] if r[0] == 'USA']
    assert len(usa_cities) == 2


def test_multi_table_join(engine):
    """Test joining 3 tables: Users -> Orders -> Items"""
    engine.execute_sql("CREATE TABLE t_users (id INT, name STRING(20))")
    engine.execute_sql("CREATE TABLE t_orders (id INT, uid INT, item_id INT)")
    engine.execute_sql("CREATE TABLE t_items (id INT, iname STRING(20))")
    
    engine.execute_sql("INSERT INTO t_users VALUES (1, 'Alice')")
    engine.execute_sql("INSERT INTO t_items VALUES (99, 'Laptop')")
    engine.execute_sql("INSERT INTO t_orders VALUES (100, 1, 99)")
    
    # 3-way Join
    # Note: Planner only supports sequence of joins if Parser produces them correctly.
    # SELECT * FROM t_users JOIN t_orders ON t_users.id = t_orders.uid JOIN t_items ON t_orders.item_id = t_items.id
    sql = """
        SELECT * FROM t_users 
        JOIN t_orders ON t_users.id = t_orders.uid 
        JOIN t_items ON t_orders.item_id = t_items.id
    """
    result = engine.execute_sql(sql)
    
    assert result['row_count'] == 1
    row = result['data'][0]
    # Check projection: id, name, id, uid, item_id, id, iname
    # Alice should be there
    assert row[1] == 'Alice'
    # Laptop should be there (last column usually, but depends on schema merge order)
    # t_users: 2 cols. t_orders: 3 cols. t_items: 2 cols. Total 7.
    assert row[6] == 'Laptop' 
    

def test_join_with_where(engine):
    """Test JOIN combined with WHERE clause"""
    engine.execute_sql("CREATE TABLE j_emp (id INT, dept_id INT, salary DOUBLE)")
    engine.execute_sql("CREATE TABLE j_dept (id INT, name STRING(20))")
    
    engine.execute_sql("INSERT INTO j_dept VALUES (10, 'Sales')")
    engine.execute_sql("INSERT INTO j_dept VALUES (20, 'IT')")
    
    engine.execute_sql("INSERT INTO j_emp VALUES (1, 10, 5000)")
    engine.execute_sql("INSERT INTO j_emp VALUES (2, 10, 3000)")
    engine.execute_sql("INSERT INTO j_emp VALUES (3, 20, 8000)")
    
    # Join and Filter
    # Get Sales employees with salary > 4000
    sql = "SELECT * FROM j_emp JOIN j_dept ON j_emp.dept_id = j_dept.id WHERE salary > 4000"
    result = engine.execute_sql(sql)

    
    assert result['row_count'] == 2
    salaries = sorted([float(r[2]) for r in result['data']])
    assert salaries == [5000.0, 8000.0]

def test_primary_key_constraint(engine):
    """Test that PRIMARY KEY prevents duplicates"""
    engine.execute_sql("CREATE TABLE users_pk (id INT PRIMARY KEY, name STRING(20))")
    
    # 1. Insert first row (OK)
    engine.execute_sql("INSERT INTO users_pk VALUES (1, 'Alice')")
    
    # 2. Insert duplicate row (Should Fail)
    from pesasql.query.exceptions import PesaSQLExecutionError
    import pytest
    
    with pytest.raises(PesaSQLExecutionError, match="Duplicate entry"):
        engine.execute_sql("INSERT INTO users_pk VALUES (1, 'Bob')")
        
    # 3. Verify only one row exists
    res = engine.execute_sql("SELECT * FROM users_pk")
    assert res['row_count'] == 1
    assert res['data'][0][1] == 'Alice'
