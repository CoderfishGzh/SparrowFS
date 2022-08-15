package utils

import (
	"SparrowFS/storage"
	"container/list"
)

type Cache struct {
	// 允许使用的最大内存
	maxBytes int64
	// 已经使用的内存
	nBytes   int64
	// 标准库的双链表
	// 队头是最近最少使用的元素，队尾是最近最久使用的元素
	ll       *list.List
	// cache 的 map
	cache    map[string]*list.Element
}

// 创建一个Cache对象
func New(maxBytes int64) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
	}
}


// 获取cache中，key对应的值
// 存在返回true，不存在返回false
func (c *Cache) Get(key []byte) (value []byte, ok bool) {
	// 判断key是否在cache里面里面
	if elem, ok := c.cache[string(key)]; ok {
		// 如果存在，将元素移动到队尾，这里约定front是队尾
		c.ll.MoveToFront(elem)
		// 获取该元素的值
		kv := elem.Value.(*storage.Entry)
		// return the value of the key
		return kv.Value, true
	}

	// 如果不存在，返回的是默认的 false
	return
}

// 淘汰最近最少访问的节点，即队头
func (c *Cache) RemoveOldest() {
	// 获取对头节点
	elem := c.ll.Back()

	// 如果list不为空，则删除该节点
	if elem != nil {
		// 从双链表中删除该节点
		c.ll.Remove(elem)
		// 从cache中删除该节点
		kv := elem.Value.(*storage.Entry)
		delete(c.cache, string(kv.Key))
		// 更新已使用的内存
		c.nBytes -= int64(kv.GetSize())
	}
}

// 将entry 添加到cache中
func (c *Cache) Add(entry storage.Entry) {
	if ele, ok := c.cache[string(entry.Key)]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*storage.Entry)
		c.nBytes += int64(entry.GetSize()) - int64(kv.GetSize())
		kv.Key = entry.Key
	} else {
		ele := c.ll.PushFront(&entry)
		c.cache[string(entry.Key)] = ele
		c.nBytes += int64(entry.GetSize())
	}

	// 如果已使用的内存超过了最大的内存，则删除最近最少使用的节点
	for c.maxBytes != 0 && c.maxBytes < c.nBytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}