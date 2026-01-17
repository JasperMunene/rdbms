import os
import shutil
import pytest
from pesasql.storage.file_manager import FileManager
from pesasql.catalog.catalog import Catalog
from pesasql.query.engine import QueryEngine
from pesasql.catalog.schema import DataType, ColumnConstraint

DB_PATH = "test_constraints.db"

@pytest.fixture
def engine():
    if os.path.exists(DB_PATH):
         shutil.rmtree(DB_PATH, ignore_errors=True) # It's a file but just in case
         try: os.remove(DB_PATH) 
         except: pass
     
    fm = FileManager(DB_PATH)
    fm.create_database()
    cat = Catalog(fm)
    eng = QueryEngine(fm, cat)
    yield eng
    
    # fm.close() # No close method
    try: os.remove(DB_PATH)
    except: pass
    if os.path.exists(DB_PATH + '.wal'):
         try: os.remove(DB_PATH + '.wal')
         except: pass

def test_default_values(engine):
    # Create table with defaults
    sql = """
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name STRING(50) NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        score DOUBLE DEFAULT 10.5,
        status STRING(20) DEFAULT 'unverified'
    )
    """
    result = engine.execute_sql(sql)
    assert result['status'] == 'created'
    
    # 1. Insert provided values (override default)
    sql_insert_1 = "INSERT INTO users (id, name, is_active) VALUES (1, 'Alice', FALSE)"
    result = engine.execute_sql(sql_insert_1)
    assert result['rows_inserted'] == 1
    
    # Verify
    res = engine.execute_sql("SELECT * FROM users WHERE id = 1")
    alice = res['data'][0]
    # id=0, name=1, is_active=2, score=3, status=4
    # QueryEngine converts to string
    assert alice[2] == 'False' 
    assert alice[3] == '10.5'
    assert alice[4] == 'unverified'
    
    # 2. Insert using all defaults where possible
    sql_insert_2 = "INSERT INTO users (id, name) VALUES (2, 'Bob')"
    engine.execute_sql(sql_insert_2)
    
    res = engine.execute_sql("SELECT * FROM users WHERE id = 2")
    bob = res['data'][0]
    assert bob[2] == 'True'
    assert bob[3] == '10.5'
    assert bob[4] == 'unverified'

    # 3. Validation: Missing NOT NULL column without default
    # name is NOT NULL and no default
    try:
        engine.execute_sql("INSERT INTO users (id) VALUES (3)")
        assert False, "Should fail due to missing NOT NULL column"
    except Exception as e:
        assert "cannot be NULL" in str(e)


def test_foreign_key_parsing(engine):
    # Create Parent Table
    engine.execute_sql("CREATE TABLE parent (id INT PRIMARY KEY, name STRING(50))")
    
    # Create Child Table with FK
    sql = """
    CREATE TABLE child (
        child_id INT PRIMARY KEY,
        parent_id INT,
        info STRING(50),
        FOREIGN KEY (parent_id) REFERENCES parent(id)
    )
    """
    result = engine.execute_sql(sql)
    assert result['status'] == 'created'
    assert result['foreign_keys'] == 1
    
    # Verify Schema Persisted
    # We can inspect catalog directly
    tbl = engine.catalog.get_table('child')
    assert len(tbl.foreign_keys) == 1
    fk = tbl.foreign_keys[0]
    assert fk.column_name == 'parent_id'
    assert fk.ref_table == 'parent'
    assert fk.ref_column == 'id'
    
    # Verify serialization/deserialization by reopening
    # (Engine fixture destroys DB, so we simulate close/open)
    # Using internal FileManager logic to simulate restart is hard with fixture.
    # But Catalog loads from pages.
    # If we drop reference and reload catalog...
    
    new_cat = Catalog(engine.file_manager) # Re-reads root
    tbl_reloaded = new_cat.get_table('child')
    assert len(tbl_reloaded.foreign_keys) == 1
    assert tbl_reloaded.foreign_keys[0].ref_table == 'parent'

if __name__ == "__main__":
    # verification script
    print("Run with pytest")
