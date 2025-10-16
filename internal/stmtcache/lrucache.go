package stmtcache

import (
	"container/list"
	"sync"
)

type lruCache[K comparable, V any] struct {
	cap     int
	mutex   sync.Mutex
	m       map[K]*list.Element
	l       *list.List
	onEvict func(K, V)
}

func newLRUCache[K comparable, V any](cap int, onEvict func(K, V)) *lruCache[K, V] {
	return &lruCache[K, V]{
		cap, sync.Mutex{}, make(map[K]*list.Element), list.New(), onEvict,
	}
}

type entry[K comparable, V any] struct {
	key K
	val V
}

func (c *lruCache[K, V]) get(key K) (val V, ok bool) {
	defer c.mutex.Unlock()
	c.mutex.Lock()

	if el, ok := c.m[key]; ok {
		c.l.MoveToFront(el)
		return el.Value.(entry[K, V]).val, true
	}

	return val, false
}

func (c *lruCache[K, V]) put(key K, val V) (evicted bool) {
	defer c.mutex.Unlock()
	c.mutex.Lock()

	if el, ok := c.m[key]; ok {
		el.Value = entry[K, V]{key, val}
		c.l.MoveToFront(el)
		return
	}

	if c.l.Len() >= c.cap {
		evicted = true
		c.evict()
	}

	el := c.l.PushFront(entry[K, V]{key, val})
	c.m[key] = el

	return evicted
}

func (c *lruCache[K, V]) evict() {
	el := c.l.Remove(c.l.Back()).(entry[K, V])
	delete(c.m, el.key)
	if c.onEvict != nil {
		c.onEvict(el.key, el.val)
	}
}
