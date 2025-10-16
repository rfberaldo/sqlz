package stmtcache

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
)

// stmt is satisfied by [sql.Stmt].
type stmt interface {
	Close() error
	ExecContext(ctx context.Context, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, args ...any) (*sql.Rows, error)
}

type StmtCache struct {
	*lruCache[string, stmt]
}

// New returns a new [StmtCache] with n maximum capacity, panics if capacity <= 0.
func New(cap int) *StmtCache {
	if cap <= 0 {
		panic("sqlz/stmtcache: capacity must be > 0")
	}

	return &StmtCache{
		newLRUCache(cap, func(key string, stmt stmt) {
			_ = stmt.Close()
		}),
	}
}

func (c *StmtCache) Get(key string) (stmt, bool) {
	return c.get(hashKey(key))
}

// Put adds a new entry to cache, returns whether an item was evicted,
// panics if key is blank.
func (c *StmtCache) Put(key string, stmt stmt) (evicted bool) {
	if key == "" {
		panic("sqlz/stmtcache: key must not be blank")
	}

	return c.put(hashKey(key), stmt)
}

// CloseAll closes all cached statements, if any.
func (c *StmtCache) CloseAll() {
	for el := c.l.Front(); el != nil; el = el.Next() {
		stmt := el.Value.(entry[string, stmt]).val
		_ = stmt.Close()
	}
}

// hashKey hashes s using SHA256, it's deterministic, and it's a consistent
// way to store a query as a key.
func hashKey(s string) string {
	digest := sha256.Sum256([]byte(s))
	return hex.EncodeToString(digest[0:24])
}
