package gitlfsfuse

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestNewDoubleLRU(t *testing.T) {
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()

	if lru.Size() != 0 {
		t.Errorf("New LRU should have size 0, got %d", lru.Size())
	}

	oid, pageNum := lru.First()
	if oid != "" || pageNum != "" {
		t.Errorf("First() on empty LRU should return empty strings, got %s, %s", oid, pageNum)
	}
}

func TestAdd(t *testing.T) {
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()

	// Add first item
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 1 {
		t.Errorf("Size should be 1, got %d", lru.Size())
	}

	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}

	// Add second page for same oid
	err = lru.Add("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got %d", lru.Size())
	}

	// Add page for different oid
	err = lru.Add("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 3 {
		t.Errorf("Size should be 3, got %d", lru.Size())
	}

	// Add existing item (should not increase size)
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 3 {
		t.Errorf("Size should still be 3, got %d", lru.Size())
	}
}

func TestMoveToEnd(t *testing.T) {
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()

	// Add items
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Check initial order
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}

	// Move first item to end
	err = lru.MoveToEnd("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Check new order
	oid, pageNum = lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}

	// Move oid to end
	err = lru.MoveToEnd("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Check new order
	oid, pageNum = lru.First()
	if oid != "oid1" || pageNum != "page2" {
		t.Errorf("First should be (oid1, page2), got (%s, %s)", oid, pageNum)
	}
}

func TestDelete(t *testing.T) {
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()
	// Add items
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Delete existing page
	err = lru.Delete("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got %d", lru.Size())
	}

	// Delete non-existing page (should not affect size)
	err = lru.Delete("oid1", "page3")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 2 {
		t.Errorf("Size should still be 2, got %d", lru.Size())
	}

	// Delete last page of an oid (should also remove the oid)
	err = lru.Delete("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 1 {
		t.Errorf("Size should be 1, got %d", lru.Size())
	}

	oid, pageNum := lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}

	// Delete last item
	err = lru.Delete("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if lru.Size() != 0 {
		t.Errorf("Size should be 0, got %d", lru.Size())
	}

	oid, pageNum = lru.First()
	if oid != "" || pageNum != "" {
		t.Errorf("First() on empty LRU should return empty strings, got %s, %s", oid, pageNum)
	}
}

func TestComplexScenario(t *testing.T) {
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()
	// Add several items
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid3", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid3", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

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
	err = lru.MoveToEnd("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.MoveToEnd("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// First should now be oid1/page2
	oid, pageNum = lru.First()
	if oid != "oid3" || pageNum != "page1" {
		t.Errorf("First should be (oid3, page1), got (%s, %s)", oid, pageNum)
	}

	// Delete some items
	err = lru.Delete("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Delete("oid3", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

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
	err = lru.Delete("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Delete("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Delete("oid3", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

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
	logFile := "test_new_lru.log"
	defer os.Remove(logFile) // clean up after the test
	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Expected no error from NewDoubleLRU, got: %v", err)
	}
	defer lru.Close()
	// Add several items
	err = lru.Add("oid1", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	err = lru.Add("oid3", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Simulate using oid2/page1, which should move it to the back
	err = lru.MoveToEnd("oid2", "page1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Check which item should be evicted (should be oid1/page1)
	oid, pageNum := lru.First()
	if oid != "oid1" || pageNum != "page1" {
		t.Errorf("First should be (oid1, page1), got (%s, %s)", oid, pageNum)
	}

	// Evict the LRU item
	err = lru.Delete(oid, pageNum)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Next LRU should be oid1/page2
	oid, pageNum = lru.First()
	if oid != "oid1" || pageNum != "page2" {
		t.Errorf("First should be (oid1, page2), got (%s, %s)", oid, pageNum)
	}

	// Simulate accessing oid1/page2
	err = lru.MoveToEnd("oid1", "page2")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Next LRU should be oid3/page1
	oid, pageNum = lru.First()
	if oid != "oid3" || pageNum != "page1" {
		t.Errorf("First should be (oid3, page1), got (%s, %s)", oid, pageNum)
	}

	err = lru.Delete(oid, pageNum)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	oid, pageNum = lru.First()
	if oid != "oid2" || pageNum != "page1" {
		t.Errorf("First should be (oid2, page1), got (%s, %s)", oid, pageNum)
	}

	if lru.Size() != 2 {
		t.Errorf("Size should be 2, got (%d)", lru.Size())
	}
}

func TestLogReplay(t *testing.T) {
	logFile := "test_lru.log"
	defer os.Remove(logFile) // cleanup after test

	// Pre-fill a log file manually
	content := strings.Join([]string{
		"A oid1 page1 -",
		"A oid1 page2 -",
		"A oid2 page1 -",
		"M oid1 page1 -",
		"D oid1 page2 -",
	}, "\n") + "\n" // <-- ensure final newline
	err := os.WriteFile(logFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write log file: %v", err)
	}

	lru, err := NewDoubleLRU(logFile)
	if err != nil {
		t.Fatalf("Failed to initialize LRU from log: %v", err)
	}
	defer lru.Close()
	if lru.Size() != 2 {
		t.Errorf("Expected size 2 after replay, got %d", lru.Size())
	}

	oid, page := lru.First()
	if oid != "oid2" || page != "page1" {
		t.Errorf("First after replay should be (oid2, page1), got (%s, %s)", oid, page)
	}
}

func TestCorruptedLogLine(t *testing.T) {
	logFile := "test_corrupt.log"
	defer os.Remove(logFile)

	content := "A oid1 page1 -\nBADLINE\n"
	err := os.WriteFile(logFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write log file: %v", err)
	}

	_, err = NewDoubleLRU(logFile)
	if err == nil {
		t.Errorf("Expected error due to corrupted log line, got nil")
	}
}
func TestCorruptedLogLine2(t *testing.T) {
	logFile := "test_corrupt.log"
	defer os.Remove(logFile)

	content := "A oid1 page1 \n" // no "-" at the end
	err := os.WriteFile(logFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write log file: %v", err)
	}

	_, err = NewDoubleLRU(logFile)
	if err == nil {
		t.Errorf("Expected error due to corrupted log line, got nil")
	}
}

func TestLRUList_Add_MoveToBack(t *testing.T) {
	lru := NewLRUList()

	lru.Add("key1")
	lru.Add("key2")
	lru.Add("key1")

	if lru.tail.prev == nil || lru.tail.prev.key != "key1" {
		t.Errorf("Expected key1 to be at the back of the list, got %v", lru.tail.prev.key)
	}

	if lru.head.next == nil || lru.head.next.key != "key2" {
		t.Errorf("Expected key2 to be at the front of the list, got %v", lru.head.next.key)
	}
}

func TestCorruptedLogLine_InvalidOperation(t *testing.T) {
	logFile := "test_invalid_op.log"
	defer os.Remove(logFile)

	content := "X oid1 page1 -\n" // Invalid operation 'X'
	err := os.WriteFile(logFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write log file: %v", err)
	}

	_, err = NewDoubleLRU(logFile)
	if err == nil || !strings.Contains(err.Error(), "invalid operation") {
		t.Errorf("Expected error for invalid op, got: %v", err)
	}
}

type errorScanner struct{}

func (e *errorScanner) Scan() bool   { return true }
func (e *errorScanner) Text() string { return "A oid1 page1" }
func (e *errorScanner) Err() error   { return errors.New("scan failed") }

func TestScannerError(t *testing.T) {
	lru := &doubleLRU{}

	// Manually override replayLog to simulate scanner error
	file, err := os.Create("test_lru_scan.log")
	if err != nil {
		t.Fatal(err)
	}
	file.WriteString("A oid1 page1 -\n")
	file.Close()

	// Patch bufio.NewScanner to inject an error would require major refactor.
	// Easier fix: simulate a corrupt file (e.g., very long line)
	corrupt := strings.Repeat("a", 10_000_000) // larger than bufio.Scanner buffer
	err = os.WriteFile("test_lru_scan.log", []byte(corrupt), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = lru.replayLog("test_lru_scan.log")
	if err == nil {
		t.Error("Expected error from scanner.Err(), got nil")
	}
}

func TestDoubleLRU_Add_LogFail(t *testing.T) {
	dir := t.TempDir()
	// Intentionally use a directory as the log path to cause a write error
	lru := &doubleLRU{
		oidMap:  make(map[string]*oidEntry),
		oidList: NewLRUList(),
		size:    0,
	}
	if err := lru.replayLog(dir); err == nil {
		t.Fatal("Expected error due to log failure, but got nil")
	}
	f, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lru.logFile = f
	
	err = lru.Add("oid1", "page1")
	if err == nil {
		t.Fatal("Expected error due to log failure, but got nil")
	}
}
