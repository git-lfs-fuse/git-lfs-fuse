package gitlfsfuse

type DoubleLRU interface {
	Add(oid string, pageNum string)
	MoveToEnd(oid string, pageNum string)
	Size() int64
	Delete(oid string, pageNum string)
	First() (string, string)
}

// -------------------- Generic Node --------------------

type Node struct {
	key  string
	prev *Node
	next *Node
}

// -------------------- LRUList (for oid and pageNum) --------------------

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

// -------------------- internal list helpers --------------------

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

// -------------------- Double LRU --------------------

type oidEntry struct {
	pages *LRUList
}

type doubleLRU struct {
	oidList *LRUList
	oidMap  map[string]*oidEntry
	size    int64
}

func NewDoubleLRU() DoubleLRU {
	return &doubleLRU{
		oidList: NewLRUList(),
		oidMap:  make(map[string]*oidEntry),
	}
}

func (d *doubleLRU) Add(oid string, pageNum string) {
	entry, exists := d.oidMap[oid]
	if !exists {
		entry = &oidEntry{pages: NewLRUList()}
		d.oidMap[oid] = entry
		d.oidList.Add(oid)
	}
	if _, exists := entry.pages.items[pageNum]; !exists {
		entry.pages.Add(pageNum)
		d.size++
	}
}

func (d *doubleLRU) MoveToEnd(oid string, pageNum string) {
	entry, exists := d.oidMap[oid]
	if !exists {
		return
	}
	if _, exists := entry.pages.items[pageNum]; exists {
		entry.pages.MoveToBack(pageNum)
		d.oidList.MoveToBack(oid)
	}
}

func (d *doubleLRU) Size() int64 {
	return d.size
}

func (d *doubleLRU) Delete(oid string, pageNum string) {
	entry, exists := d.oidMap[oid]
	if !exists {
		return
	}
	if _, exists := entry.pages.items[pageNum]; exists {
		entry.pages.Remove(pageNum)
		d.size--
		if entry.pages.Len() == 0 {
			delete(d.oidMap, oid)
			d.oidList.Remove(oid)
		}
	}
}

func (d *doubleLRU) First() (string, string) {
	oid, ok := d.oidList.Front()
	if !ok {
		return "", ""
	}
	entry := d.oidMap[oid]
	pageNum, ok := entry.pages.Front()
	if !ok {
		return "", ""
	}
	return oid, pageNum
}
