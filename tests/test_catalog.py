

import pytest
from pesasql.storage.file_manager import FileManager
from pesasql.catalog.catalog import Catalog
from pesasql.catalog.schema import TableSchema, Column, DataType, ColumnConstraint

@pytest.fixture
def file_manager(tmp_path):
    db_path = tmp_path / "test_catalog.db"
    fm = FileManager(str(db_path))
    fm.create_database()
    return fm

@pytest.fixture
def catalog(file_manager):
    return Catalog(file_manager)

def test_schema_serialization():
    # Create manual schema
    col1 = Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
    col2 = Column("name", DataType.STRING, max_length=100)
    schema = TableSchema("test_table", [col1, col2])
    
    # Serialize and deserialize
    data = schema.serialize()
    loaded = TableSchema.deserialize(data)
    
    assert loaded.name == schema.name
    assert len(loaded.columns) == 2
    assert loaded.primary_key == "id"
    assert loaded.get_column("name").max_length == 100

def test_catalog_create_table(catalog):
    # Initial state: 0 tables
    assert len(catalog.list_tables()) == 0
    
    # Create table
    col1 = Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
    schema = TableSchema("users", [col1])
    
    success = catalog.create_table(schema)
    assert success is True
    assert "users" in catalog.list_tables()
    
    # Retrieve
    table = catalog.get_table("users")
    assert table is not None
    assert table.name == "users"

def test_catalog_persistence(file_manager):
    # Create table in one catalog instance
    cat1 = Catalog(file_manager)
    col1 = Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
    schema = TableSchema("users", [col1])
    cat1.create_table(schema)
    
    # Verify in new instance (reload from disk)
    cat2 = Catalog(file_manager)
    assert "users" in cat2.list_tables()
    assert cat2.get_table("users").primary_key == "id"

def test_catalog_drop_table(catalog):
    col1 = Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
    schema = TableSchema("temp_table", [col1])
    catalog.create_table(schema)
    
    assert "temp_table" in catalog.list_tables()
    
    catalog.drop_table("temp_table")
    assert "temp_table" not in catalog.list_tables()
