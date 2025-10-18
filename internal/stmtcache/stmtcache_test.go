package stmtcache

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStmt struct {
	closeCalled bool
}

func (m *mockStmt) Close() error {
	m.closeCalled = true
	return nil
}

func (m *mockStmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return nil, nil
}

func (m *mockStmt) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	return nil, nil
}

func TestStmtCache(t *testing.T) {
	t.Run("panic if cap <= 0", func(t *testing.T) {
		assert.Panics(t, func() { New(0) })
	})

	const cap = 2
	c := New(cap)

	fooStmt := &mockStmt{}
	barStmt := &mockStmt{}
	bazStmt := &mockStmt{}

	t.Run("put and get value", func(t *testing.T) {
		evicted := c.Put("foo", nil)
		assert.False(t, evicted)
		v, ok := c.Get("foo")
		require.True(t, ok)
		assert.Equal(t, nil, v)
		assert.Equal(t, 1, c.Len())
	})

	t.Run("updating existing key moves it to front", func(t *testing.T) {
		evicted := c.Put("foo", fooStmt)
		assert.False(t, evicted)
		v, ok := c.Get("foo")
		require.True(t, ok)
		assert.Equal(t, fooStmt, v)
		assert.Equal(t, 1, c.Len())
	})

	t.Run("evict when full", func(t *testing.T) {
		evicted := c.Put("bar", barStmt)
		assert.False(t, evicted)

		assert.False(t, fooStmt.closeCalled)
		evicted = c.Put("baz", bazStmt)
		assert.True(t, evicted)
		assert.True(t, fooStmt.closeCalled)

		_, ok := c.Get("foo")
		assert.False(t, ok)

		v, ok := c.Get("bar")
		assert.True(t, ok)
		assert.Equal(t, barStmt, v)

		v, ok = c.Get("baz")
		assert.True(t, ok)
		assert.Equal(t, bazStmt, v)

		assert.Equal(t, cap, c.Len())
	})

	t.Run("clear", func(t *testing.T) {
		assert.False(t, barStmt.closeCalled)
		assert.False(t, bazStmt.closeCalled)
		c.Clear()
		assert.True(t, barStmt.closeCalled)
		assert.True(t, bazStmt.closeCalled)
		assert.Equal(t, 0, c.Len())
	})

	t.Run("blank key should panic", func(t *testing.T) {
		assert.Panics(t, func() {
			c.Put("", nil)
		})
	})
}

func TestHashKey(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "small string",
			input:  "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
			expect: "a58dd8680234c1f8cc2ef2b325a43733605a7f16f288e072",
		},
		{
			name:   "medium string",
			input:  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin sed dapibus sapien. Donec nec ipsum a lorem aliquet blandit. Nullam quis tempus velit. In id massa blandit, sollicitudin dui non, fermentum nulla. Sed sed eros ac elit aliquet malesuada quis nec ligula.",
			expect: "bd78ae92057058526d9f6a8cf2b3d6e6911196f15c030d9f",
		},
		{
			name:   "large string",
			input:  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin sed dapibus sapien. Donec nec ipsum a lorem aliquet blandit. Mauris metus nibh, commodo ut elit sed, eleifend sollicitudin tellus. Nullam quis tempus velit. In id massa blandit, sollicitudin dui non, fermentum nulla. Sed sed eros ac elit aliquet malesuada quis nec ligula. Etiam nunc ex, accumsan a bibendum pellentesque, maximus et lectus. Ut nisl massa, rutrum id bibendum fringilla, suscipit a nunc. Vivamus fringilla mi eget leo condimentum convallis.",
			expect: "aecb66379c0bdac5883dfa5ea01fa7e2bfd5d753b6f31724",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hashKey(tc.input)
			assert.Equal(t, tc.expect, got)
		})
	}
}
