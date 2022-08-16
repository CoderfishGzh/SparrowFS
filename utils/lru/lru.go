package lru

import (
	"SparrowFS/storage"
	"container/list"
)

type Cache struct {
	maxBytes  int64
	nBytes    int64
	ll        *list.List
	cache     map[string]*list.Element
	OnEvicted func(key []byte, value storage.Entry)
}

func New(maxBytes int64, onEvicted func([]byte, storage.Entry)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key []byte) (value storage.Entry, ok bool) {
	if ele, ok := c.cache[string(key)]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*storage.Entry)
		return *kv, true
	}
	return
}

// Remove the least recently used to make the cache fit.
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()

	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*storage.Entry)
		delete(c.cache, string(kv.Key))
		c.nBytes -= int64(kv.GetSize())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.Key, *kv)
		}
	}
}

func (c *Cache) Add(key []byte, value []byte) {
	if ele, ok := c.cache[string(key)]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*storage.Entry)
		c.nBytes += int64(len(value)) - int64(kv.ValueSize)
		kv.Value = value
		kv.ValueSize = uint32(len(value))
	} else {
		// create an entry
		newEntry := storage.NewEntry(key, value, storage.PUT)
		ele := c.ll.PushFront(newEntry)
		c.cache[string(key)] = ele
		c.nBytes += int64(newEntry.GetSize())
	}

	for c.maxBytes != 0 && c.nBytes > c.maxBytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}