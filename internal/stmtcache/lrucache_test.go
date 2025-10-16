package stmtcache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLRUCache(t *testing.T) {
	const cap = 2
	c := newLRUCache[string, string](cap, nil)

	t.Run("put and get value", func(t *testing.T) {
		evicted := c.put("foo", "fooval")
		assert.False(t, evicted)
		v, ok := c.get("foo")
		require.True(t, ok)
		assert.Equal(t, "fooval", v)
	})

	t.Run("updating existing key moves it to front", func(t *testing.T) {
		evicted := c.put("foo", "fooval2")
		assert.False(t, evicted)
		v, ok := c.get("foo")
		require.True(t, ok)
		assert.Equal(t, "fooval2", v)
		assert.Equal(t, "fooval2", c.l.Front().Value.(entry[string, string]).val)
	})

	t.Run("evict when full", func(t *testing.T) {
		evicted := c.put("bar", "barval")
		assert.False(t, evicted)

		evicted = c.put("baz", "bazval")
		assert.True(t, evicted)

		_, ok := c.get("foo")
		assert.False(t, ok)

		v, ok := c.get("bar")
		assert.True(t, ok)
		assert.Equal(t, "barval", v)

		v, ok = c.get("baz")
		assert.True(t, ok)
		assert.Equal(t, "bazval", v)

		assert.Equal(t, cap, c.l.Len())
		assert.Equal(t, cap, len(c.m))
	})
}

func TestLRUCache_concurrency(t *testing.T) {
	c := newLRUCache[string, int](50, nil)
	var wg sync.WaitGroup

	// multiple writers
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 100 {
				key := string(rune(j))
				c.put(key, j)
			}
		}()
	}

	// multiple readers
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 100 {
				key := string(rune(j))
				c.get(key)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, c.l.Len(), c.cap)
	assert.Equal(t, len(c.m), c.cap)
}
