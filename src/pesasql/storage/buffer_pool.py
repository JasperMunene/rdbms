"""
Buffer Pool Module - In-memory cache for database pages
Implements LRU eviction policy with pinning mechanism.
"""

from collections import OrderedDict
from typing import Optional
from .page import Page


class BufferPool:
    """LRU cache for database pages with pinning support"""

    def __init__(self, capacity: int = 100):
        """
        Initialize buffer pool

        Args:
            capacity: Maximum number of pages to cache
        """
        self.capacity = capacity
        self.pool: OrderedDict[int, Page] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def pin_page(self, page_id: int, file_manager) -> Page:
        """
        Get page and increment pin count (preventing eviction)

        Args:
            page_id: Page identifier
            file_manager: FileManager instance for disk reads

        Returns:
            Page object (from cache or disk)
        """
        # Try to get from cache first
        if page_id in self.pool:
            page = self.pool.pop(page_id)  # Remove to update LRU position
            self.pool[page_id] = page
            self.hits += 1
        else:
            # Read from disk
            page = file_manager.read_page(page_id)
            self.misses += 1

            # Make room in cache if needed
            self._evict_if_needed(file_manager)

            # Add to cache
            self.pool[page_id] = page

        # Increment pin count
        page.pin_count += 1
        return page

    def unpin_page(self, page_id: int, is_dirty: bool = False) -> None:
        """
        Decrement pin count and optionally mark page as dirty

        Args:
            page_id: Page identifier
            is_dirty: Whether page was modified
        """
        if page_id not in self.pool:
            return

        page = self.pool[page_id]

        # Decrement pin count (never negative)
        if page.pin_count > 0:
            page.pin_count -= 1

        # Mark as dirty if modified
        if is_dirty:
            page.is_dirty = True

    def get_page(self, page_id: int, file_manager) -> Optional[Page]:
        """
        Get page without affecting pin count

        Args:
            page_id: Page identifier
            file_manager: FileManager instance for disk reads

        Returns:
            Page object or None if not found
        """
        if page_id in self.pool:
            # Update LRU position
            page = self.pool.pop(page_id)
            self.pool[page_id] = page
            self.hits += 1
            return page

        self.misses += 1
        return None

    def flush_page(self, page_id: int, file_manager) -> None:
        """
        Write page to disk if dirty and remove from cache

        Args:
            page_id: Page identifier
            file_manager: FileManager instance for disk writes
        """
        if page_id not in self.pool:
            return

        page = self.pool[page_id]

        if page.is_dirty and page.pin_count == 0:
            file_manager.write_page_with_wal(page)

        # Remove from cache
        self.pool.pop(page_id, None)

    def flush_all(self, file_manager) -> None:
        """Write all dirty pages to disk"""
        for page_id, page in list(self.pool.items()):
            if page.is_dirty and page.pin_count == 0:
                file_manager.write_page_with_wal(page)
                self.pool.pop(page_id)

    def _evict_if_needed(self, file_manager) -> None:
        """Evict least recently used unpinned page if capacity exceeded"""
        while len(self.pool) >= self.capacity:
            # Find unpinned page to evict
            for pid, page in self.pool.items():
                if page.pin_count == 0:
                    # Flush if dirty
                    if page.is_dirty:
                        file_manager.write_page_with_wal(page)

                    # Remove from cache
                    self.pool.pop(pid)
                    self.evictions += 1
                    return

            # If all pages are pinned, we can't evict
            raise RuntimeError("Buffer pool full - all pages are pinned")

    def invalidate_page(self, page_id: int) -> None:
        """Remove page from cache without writing"""
        if page_id in self.pool:
            page = self.pool[page_id]
            if page.pin_count > 0:
                raise RuntimeError(f"Cannot invalidate pinned page {page_id}")
            self.pool.pop(page_id)

    def get_stats(self) -> dict:
        """Get buffer pool statistics"""
        total_accesses = self.hits + self.misses
        hit_ratio = self.hits / total_accesses if total_accesses > 0 else 0

        return {
            "capacity": self.capacity,
            "current_size": len(self.pool),
            "hits": self.hits,
            "misses": self.misses,
            "hit_ratio": f"{hit_ratio:.2%}",
            "evictions": self.evictions,
            "pinned_pages": sum(1 for p in self.pool.values() if p.pin_count > 0)
        }

    def clear(self, file_manager) -> None:
        """Clear buffer pool, flushing dirty pages"""
        self.flush_all(file_manager)
        self.pool.clear()

    def __repr__(self) -> str:
        """String representation of buffer pool"""
        stats = self.get_stats()
        return (f"BufferPool(capacity={stats['capacity']}, "
                f"size={stats['current_size']}, "
                f"hit_ratio={stats['hit_ratio']})")