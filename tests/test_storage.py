

import pytest
from pesasql.storage.page import Page, PageType
from pesasql.storage.file_manager import FileManager
from pesasql.storage.buffer_pool import BufferPool
from pesasql.constants import PAGE_SIZE, PESA_MAGIC

@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test.db"

@pytest.fixture
def file_manager(db_path):
    fm = FileManager(str(db_path))
    fm.create_database()
    return fm

def test_page_rw():
    page = Page(0, PageType.TABLE)
    
    # Int writing
    page.write_int(100, 12345)
    assert page.read_int(100) == 12345
    
    # String writing - new API takes max_length
    test_str = "Hello World"
    page.write_string(200, test_str, 20)
    # read_string no longer takes length
    assert page.read_string(200) == test_str
    
    # Double writing
    page.write_double(300, 3.14159)
    # allow for some float precision weirdness
    assert abs(page.read_double(300) - 3.14159) < 0.000001
    
    # Free space management
    initial_free = page.get_free_space()
    assert initial_free > 0
    
    allocated = page.allocate_space(50)
    assert allocated is not None
    assert page.get_free_space() == initial_free - 50
    
def test_file_manager_create(db_path):
    fm = FileManager(str(db_path))
    fm.create_database()
    
    assert db_path.exists()
    assert db_path.stat().st_size >= PAGE_SIZE * 2 # Header + Catalog
    
    header = fm.read_page(0)
    assert header.page_type == PageType.HEADER
    
    # Read database magic string from info
    info = fm.get_database_info()
    assert info["magic_string"] == PESA_MAGIC
    assert info["total_pages"] == 2

def test_file_manager_allocation(file_manager):
    # Initial state: 2 pages (0, 1)
    
    # Allocate new page
    p2 = file_manager.allocate_page()
    assert p2.page_id == 2
    
    # Write some data
    p2.write_int(100, 999)
    # New API: use write_page_with_wal or internal write_page. 
    # Allocate marks dirty, so write_page works.
    file_manager.write_page(p2)
    
    # Read back
    p2_read = file_manager.read_page(2)
    assert p2_read.read_int(100) == 999
    
    # Allocate another
    p3 = file_manager.allocate_page()
    assert p3.page_id == 3
    
    # Deallocate p2 - New API takes page_id int
    file_manager.deallocate_page(p2.page_id)
    
    # Reallocate - should get p2 back (freelist)
    p_new = file_manager.allocate_page()
    assert p_new.page_id == 2
    
def test_buffer_pool(file_manager):
    bp = BufferPool(capacity=2)
    
    # Use pin_page instead of get_page for initial load
    p0 = bp.pin_page(0, file_manager)
    p1 = bp.pin_page(1, file_manager)
    
    assert len(bp.pool) == 2
    
    # Unpin pages to allow eviction
    bp.unpin_page(0)
    bp.unpin_page(1)
    
    # Ensure p2 exists
    file_manager.allocate_page() # p2
    
    # Pin p2, should evict p0 (LRU) as p0 was added first and unused since
    # Wait, 'get_page' updates timestamp? 'pin_page' uses hit/miss logic.
    # p0, p1 loaded. Pool: {0: p0, 1: p1}. LRU order is by insertion time if not accessed.
    # 0 is oldest.
    
    p2 = bp.pin_page(2, file_manager)
    
    assert 2 in bp.pool
    assert 1 in bp.pool
    assert 0 not in bp.pool
    
    # Test dirty write back
    p2.write_int(100, 777) 
    bp.unpin_page(2, is_dirty=True) # Unpin and mark dirty
    
    # Pin p0 again to force p2 eviction (since p2 is unpinned)
    # p1 is also in pool. p2 was just accessed, so p1 is LRU?
    # Pool: {1: p1, 2: p2}. 
    # Access: p0 (miss) -> Evict LRU (p1)
    p0 = bp.pin_page(0, file_manager)
    
    assert 0 in bp.pool
    assert 2 in bp.pool
    assert 1 not in bp.pool
    
    # Force p2 eviction: Access p1
    bp.unpin_page(0) # Unpin p0
    p1 = bp.pin_page(1, file_manager)
    # Pool: {2: p2, 0: p0}. p2 is LRU? 
    # Order: [p2 (accessed earlier), p0 (accessed just now)]. So p2 evicted.
    
    assert 1 in bp.pool
    assert 2 not in bp.pool
    
    # Check disk for p2 changes (WAL/Checkpointed)
    # flush happens on eviction
    p2_disk = file_manager.read_page(2)
    assert p2_disk.read_int(100) == 777

def test_wal_transaction(file_manager):
    file_manager.begin_transaction()
    p_new = file_manager.allocate_page()
    p_new.write_int(100, 42)
    # Writes go to WAL
    file_manager.write_page_with_wal(p_new)
    file_manager.commit_transaction()
    
    # Check stats
    info = file_manager.get_database_info()
    assert info["wal_size"] > 0
    
    file_manager.checkpoint()
    info_after = file_manager.get_database_info()
    assert info_after["wal_size"] == 0
