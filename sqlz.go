// Package sqlz is a thin wrapper around the standard [database/sql] package.
// It provides a less verbose API for working with SQL databases.
package sqlz

import (
	"context"
	"database/sql"

	"github.com/rfberaldo/sqlz/core"
	"github.com/rfberaldo/sqlz/parser"
)

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	pool      *sql.DB
	bind      parser.Bind
	structTag string
}

// Pool return the underlying [sql.DB].
func (db *DB) Pool() *sql.DB { return db.pool }

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the transaction will roll back.
// [Tx.Commit] will return an error if the context provided to BeginTx is canceled.
//
// Begin uses default options; to specify custom options, use [DB.BeginTx]
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginTx(ctx, nil)
}

// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the transaction will roll back.
// [Tx.Commit] will return an error if the context provided to BeginTx is canceled.
//
// The provided [TxOptions] is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{tx, db.bind, db.structTag}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// Returned rows will be scanned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (db *DB) Query(ctx context.Context, dst any, query string, args ...any) error {
	return core.Query(ctx, db.pool, db.bind, db.structTag, dst, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// If the query selects no rows, will return an error which IsNotFound(err) is true.
// Otherwise, scans the first row and discards the rest.
// Returned rows will be scanned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (db *DB) QueryRow(ctx context.Context, dst any, query string, args ...any) error {
	return core.QueryRow(ctx, db.pool, db.bind, db.structTag, dst, query, args...)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, db.pool, db.bind, db.structTag, query, args...)
}

// Tx is an in-progress database transaction, representing a single connection.
//
// A transaction must end with a call to [Tx.Commit] or [Tx.Rollback], or else
// the connection will be locked.
//
// After a call to [Tx.Commit] or [Tx.Rollback], all operations on the
// transaction fail with [sql.ErrTxDone].
type Tx struct {
	conn      *sql.Tx
	bind      parser.Bind
	structTag string
}

// Conn return the underlying [sql.Tx].
func (tx *Tx) Conn() *sql.Tx { return tx.conn }

// Commit commits the transaction.
//
// If Commit fails, then all queries on the Tx should be discarded as invalid.
func (tx *Tx) Commit() error { return tx.conn.Commit() }

// Rollback aborts the transaction.
//
// Even if Rollback fails, the transaction will no longer be valid,
// nor will it have been committed to the database.
func (tx *Tx) Rollback() error { return tx.conn.Rollback() }

// Query executes a query that returns rows, typically a SELECT.
// Returned rows will be scanned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (tx *Tx) Query(ctx context.Context, dst any, query string, args ...any) error {
	return core.Query(ctx, tx.conn, tx.bind, tx.structTag, dst, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// If the query selects no rows, will return an error which IsNotFound(err) is true.
// Otherwise, scans the first row and discards the rest.
// Returned rows will be scanned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (tx *Tx) QueryRow(ctx context.Context, dst any, query string, args ...any) error {
	return core.QueryRow(ctx, tx.conn, tx.bind, tx.structTag, dst, query, args...)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// If args is an array, it will expand query and args for a batch INSERT.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
func (tx *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, tx.conn, tx.bind, tx.structTag, query, args...)
}
