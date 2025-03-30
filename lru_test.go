package gitlfsfuse

import (
	"testing"
)

func TestNewDoubleLRU(t *testing.T) {
	lru := NewDoubleLRU()
	if lru.Size() != 0 {
		t.Errorf("New LRU should have size 0, got %d", lru.Size())
	}
	
	oid, pageNum := lru.First()
	if oid != "" || pageNum != "" {
		t.Errorf("First() on empty LRU should return empty strings, got %s, %s", oid, pageNum)
	}
}

func TestAdd(t *testing.T) {
	lru := NewDoubleLRU()
	
	// Add first item
	lru.Add("oid1", "page1")
	if lru.Size() != 1 {
		t.Errorf("Size should be 1, got %d", lru.Size())
	}
	
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Add second page for same oid
	lru.Add("oid1", "page2")
	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got %d", lru.Size())
	}
	
	// Add page for different oid
	lru.Add("oid2", "page1")
	if lru.Size() != 3 {
		t.Errorf("Size should be 3, got %d", lru.Size())
	}
	
	// Add existing item (should not increase size)
	lru.Add("oid1", "page1")
	if lru.Size() != 3 {
		t.Errorf("Size should still be 3, got %d", lru.Size())
	}
}

func TestMoveToEnd(t *testing.T) {
	lru := NewDoubleLRU()
	
	// Add items
	lru.Add("oid1", "page1")
	lru.Add("oid1", "page2")
	lru.Add("oid2", "page1")
	
	// Check initial order
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Move first item to end
	lru.MoveToEnd("oid1", "page1")
	
	// Check new order
	oid, pageNum = lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Move oid to end
	lru.MoveToEnd("oid2", "page1")
	
	// Check new order
	oid, pageNum = lru.First()
	if oid != "oid1" || pageNum != "page2" {
		t.Errorf("First should be (oid1, page2), got (%s, %s)", oid, pageNum)
	}
}

func TestDelete(t *testing.T) {
	lru := NewDoubleLRU()
	
	// Add items
	lru.Add("oid1", "page1")
	lru.Add("oid1", "page2")
	lru.Add("oid2", "page1")
	
	// Delete existing page
	lru.Delete("oid1", "page1")
	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got %d", lru.Size())
	}
	
	// Delete non-existing page (should not affect size)
	lru.Delete("oid1", "page3")
	if lru.Size() != 2 {
		t.Errorf("Size should still be 2, got %d", lru.Size())
	}
	
	// Delete last page of an oid (should also remove the oid)
	lru.Delete("oid1", "page2")
	if lru.Size() != 1 {
		t.Errorf("Size should be 1, got %d", lru.Size())
	}
	
	oid, pageNum := lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Delete last item
	lru.Delete("oid2", "page1")
	if lru.Size() != 0 {
		t.Errorf("Size should be 0, got %d", lru.Size())
	}
	
	oid, pageNum = lru.First()
	if oid != "" || pageNum != "" {
		t.Errorf("First() on empty LRU should return empty strings, got %s, %s", oid, pageNum)
	}
}

func TestComplexScenario(t *testing.T) {
	lru := NewDoubleLRU()
	
	// Add several items
	lru.Add("oid1", "page1")
	lru.Add("oid1", "page2")
	lru.Add("oid2", "page1")
	lru.Add("oid3", "page1")
	lru.Add("oid3", "page2")
	
	// Test initial size
	if lru.Size() != 5 {
		t.Errorf("Size should be 5, got %d", lru.Size())
	}
	
	// First should be oid1/page1
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Move items around
	lru.MoveToEnd("oid1", "page1")
	lru.MoveToEnd("oid2", "page1")
	
	// First should now be oid1/page2
	oid, pageNum = lru.First()
	if oid != "oid3" || pageNum != "page1" {
		t.Errorf("First should be (oid3, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Delete some items
	lru.Delete("oid1", "page2")
	lru.Delete("oid3", "page1")
	
	// Size should be 3
	if lru.Size() != 3 {
		t.Errorf("Size should be 3, got %d", lru.Size())
	}
	
	// First should be oid3/page2
	oid, pageNum = lru.First()
	if oid != "oid3" || pageNum != "page2" {
		t.Errorf("First should be (oid3, page2), got (%s, %s)", oid, pageNum)
	}
	
	// Delete rest of the items
	lru.Delete("oid1", "page1")
	lru.Delete("oid2", "page1")
	lru.Delete("oid3", "page2")
	
	// Check final state
	if lru.Size() != 0 {
		t.Errorf("Size should be 0, got %d", lru.Size())
	}
	
	oid, pageNum = lru.First()
	if oid != "" || pageNum != "" {
		t.Errorf("First() on empty LRU should return empty strings, got %s, %s", oid, pageNum)
	}
}

func TestEvictionScenario(t *testing.T) {
	// This test simulates how this would be used in a cache eviction scenario
	lru := NewDoubleLRU()
	
	// Add several items
	lru.Add("oid1", "page1")
	lru.Add("oid1", "page2")
	lru.Add("oid2", "page1")
	lru.Add("oid3", "page1")
	
	// Simulate using oid2/page1, which should move it to the back
	lru.MoveToEnd("oid2", "page1")
	
	// Check which item should be evicted (should be oid1/page1)
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}
	
	// Evict the LRU item
	lru.Delete(oid, pageNum)
	
	// Next LRU should be oid1/page2
	oid, pageNum = lru.First()
	if oid != "oid1" || pageNum != "page2" {
		t.Errorf("First should be (oid1, page2), got (%s, %s)", oid, pageNum)
	}
	
	// Simulate accessing oid1/page2
	lru.MoveToEnd("oid1", "page2")
	
	// Next LRU should be oid3/page1
	oid, pageNum = lru.First()
	if oid != "oid3" || pageNum != "page1" {
		t.Errorf("First should be (oid3, page1), got (%s, %s)", oid, pageNum)
	}

	lru.Delete(oid, pageNum)
	oid, pageNum = lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}

	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got (%d)", lru.Size())
	}
}