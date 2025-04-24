package gitlfsfuse

import (
	"bufio"
	"errors"
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
	oidList    *LRUList
	oidMap     map[string]*oidEntry
	lruLogPath string
	size       int64
	doLog      bool
}

func NewDoubleLRU(lruLogPath string) (DoubleLRU, error) {
	d := &doubleLRU{
		oidList:    NewLRUList(),
		oidMap:     make(map[string]*oidEntry),
		lruLogPath: lruLogPath,
		doLog:      false,
	}
	err := d.replayLog() // Restore from log
	if err != nil {
		return nil, err
	}
	d.doLog = true
	return d, nil
}

func (d *doubleLRU) replayLog() error {
	file, err := os.Open(d.lruLogPath)
	if err != nil {
		// It's okay if the file doesn't exist yet (first startup)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	// TODO: Check whether the line is complete
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return errors.New("Corrupted log")
		}
		op, oid, page := parts[0], parts[1], parts[2]
		switch op {
		case "A":
			_ = d.Add(oid, page)
		case "M":
			_ = d.MoveToEnd(oid, page)
		case "D":
			_ = d.Delete(oid, page)
		default:
			return errors.New("Invald operation " + op)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
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
		if d.doLog {
			err = logOperation(d.lruLogPath, "A", oid, pageNum)
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
			if d.doLog {
				err = logOperation(d.lruLogPath, "M", oid, pageNum)
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
			if d.doLog {
				err = logOperation(d.lruLogPath, "D", oid, pageNum)
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

func logOperation(lruLogPath string, op string, oid, page string) error {
	f, err := os.OpenFile(lruLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// TODO: Check before appending
	if err != nil {
		return err // optional: log error
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s %s %s\n", op, oid, page)
	if err == nil {
		err = f.Sync()
	}
	return err
}
