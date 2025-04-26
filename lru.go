package gitlfsfuse

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type DoubleLRU interface {
	Add(oid string, pageNum string) error
	MoveToEnd(oid string, pageNum string) error
	Size() int64
	Delete(oid string, pageNum string) error
	First() (string, string)
	Close()
}

type Node struct {
	prev *Node
	next *Node
	key  string
}

type LRUList struct {
	items map[string]*Node
	head  *Node
	tail  *Node
}

func NewLRUList() *LRUList {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head
	return &LRUList{
		items: make(map[string]*Node),
		head:  head,
		tail:  tail,
	}
}

func (lru *LRUList) Add(key string) {
	if node, exists := lru.items[key]; exists {
		lru.moveToBack(node)
	} else {
		node := &Node{key: key}
		lru.items[key] = node
		lru.insertToBack(node)
	}
}

func (lru *LRUList) Remove(key string) {
	if node, exists := lru.items[key]; exists {
		lru.unlink(node)
		delete(lru.items, key)
	}
}

func (lru *LRUList) MoveToBack(key string) {
	if node, exists := lru.items[key]; exists {
		lru.moveToBack(node)
	}
}

func (lru *LRUList) Front() (string, bool) {
	if lru.head.next == lru.tail {
		return "", false
	}
	return lru.head.next.key, true
}

func (lru *LRUList) Len() int {
	return len(lru.items)
}

func (lru *LRUList) unlink(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

func (lru *LRUList) insertToBack(node *Node) {
	node.prev = lru.tail.prev
	node.next = lru.tail
	lru.tail.prev.next = node
	lru.tail.prev = node
}

func (lru *LRUList) moveToBack(node *Node) {
	lru.unlink(node)
	lru.insertToBack(node)
}

type oidEntry struct {
	pages *LRUList
}
type doubleLRU struct {
	oidList   *LRUList
	oidMap    map[string]*oidEntry
	logFile   *os.File
	lruLogPos int64
	size      int64
}

func NewDoubleLRU(lruLogPath string) (DoubleLRU, error) {
	d := &doubleLRU{
		oidList: NewLRUList(),
		oidMap:  make(map[string]*oidEntry),
	}
	err := d.replayLog(lruLogPath) // Restore from log
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (d *doubleLRU) replayLog(lruLogPath string) error {
	file, err := os.OpenFile(lruLogPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 4 {
			file.Close()
			return fmt.Errorf("invalid line (%d): %q", d.lruLogPos, line)
		}
		op, oid, page, end := parts[0], parts[1], parts[2], parts[3]
		if end != "-" {
			file.Close()
			return fmt.Errorf("invalid line (%d): %q", d.lruLogPos, line)
		}
		switch op {
		case "A":
			_ = d.Add(oid, page)
		case "M":
			_ = d.MoveToEnd(oid, page)
		case "D":
			_ = d.Delete(oid, page)
		default:
			file.Close()
			return fmt.Errorf("invalid operation (%d): %q", d.lruLogPos, line)
		}
		d.lruLogPos += int64(len(line)) + 1
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return err
	}
	d.logFile = file
	return nil
}

func (d *doubleLRU) Add(oid string, pageNum string) (err error) {
	entry, exists := d.oidMap[oid]
	if !exists {
		entry = &oidEntry{pages: NewLRUList()}
		d.oidMap[oid] = entry
		d.oidList.Add(oid)
	}
	if _, exists := entry.pages.items[pageNum]; !exists {
		if d.logFile != nil {
			err = d.logOperation("A", oid, pageNum)
		}
		if err == nil {
			entry.pages.Add(pageNum)
			d.size++
		}
	}
	return err
}

func (d *doubleLRU) MoveToEnd(oid string, pageNum string) (err error) {
	entry, ok := d.oidMap[oid]
	if ok {
		if _, exists := entry.pages.items[pageNum]; exists {
			if d.logFile != nil {
				err = d.logOperation("M", oid, pageNum)
			}
			if err == nil {
				entry.pages.MoveToBack(pageNum)
				d.oidList.MoveToBack(oid)
			}
		}
	}
	return err
}

func (d *doubleLRU) Size() int64 {
	return d.size
}

func (d *doubleLRU) Delete(oid string, pageNum string) (err error) {
	entry, ok := d.oidMap[oid]
	if ok {
		if _, exists := entry.pages.items[pageNum]; exists {
			if d.logFile != nil {
				err = d.logOperation("D", oid, pageNum)
			}
			if err == nil {
				entry.pages.Remove(pageNum)
				d.size--
				if entry.pages.Len() == 0 {
					delete(d.oidMap, oid)
					d.oidList.Remove(oid)
				}
			}
		}
	}
	return err
}

func (d *doubleLRU) First() (oid string, pageNum string) {
	var ok bool
	oid, ok = d.oidList.Front()
	if ok {
		entry := d.oidMap[oid]
		pageNum, ok = entry.pages.Front()
	}
	if !ok {
		return "", ""
	}
	return oid, pageNum
}

func (d *doubleLRU) logOperation(op string, oid, page string) error {
	line := fmt.Sprintf("%s %s %s -\n", op, oid, page)
	_, err := d.logFile.WriteAt([]byte(line), d.lruLogPos)
	if err == nil {
		err = d.logFile.Sync()
	}
	if err == nil {
		d.lruLogPos += int64(len(line))
	}
	return err
}

func (d *doubleLRU) Close() {
	d.logFile.Close()
}
