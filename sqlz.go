package sqlz

import (
	"context"
	"database/sql"

	"github.com/rafaberaldo/sqlz/binds"
	"github.com/rafaberaldo/sqlz/internal/core"
)

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	conn      *sql.DB
	bind      binds.Bind
	structTag string
	scanner   core.Scanner
}

// Conn return the underlying [*sql.DB].
func (db *DB) Conn() *sql.DB { return db.conn }

// SetStructTag changes the default struct tag. Default is "db".
func (db *DB) SetStructTag(tag string) {
	db.structTag = tag
	db.scanner = newScanner(tag)
}

// Begin starts a transaction. The default isolation level is dependent on
// the driver.
//
// Begin uses [context.Background] internally;
// to specify the context, use [DB.BeginTx].
func (db *DB) Begin() (*Tx, error) {
	return db.BeginTx(context.Background(), nil)
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
	tx, err := db.conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{tx, db.bind, db.structTag, db.scanner}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Query uses [context.Background] internally;
// to specify the context, use [DB.QueryCtx].
func (db *DB) Query(dst any, query string, args ...any) error {
	return core.Query(context.Background(), db.conn, db.bind, db.scanner, db.structTag, dst, query, args...)
}

// QueryCtx is like [DB.Query], with context.
func (db *DB) QueryCtx(ctx context.Context, dst any, query string, args ...any) error {
	return core.Query(ctx, db.conn, db.bind, db.scanner, db.structTag, dst, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// If the query selects no rows, will return an error which IsNotFound(err) is true.
// Otherwise, scans the first row and discards the rest.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// QueryRow uses [context.Background] internally;
// to specify the context, use [DB.QueryRowCtx].
func (db *DB) QueryRow(dst any, query string, args ...any) error {
	return core.QueryRow(context.Background(), db.conn, db.bind, db.scanner, db.structTag, dst, query, args...)
}

// QueryRowCtx is like [DB.QueryRow], with context.
func (db *DB) QueryRowCtx(ctx context.Context, dst any, query string, args ...any) error {
	return core.QueryRow(ctx, db.conn, db.bind, db.scanner, db.structTag, dst, query, args...)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Exec uses [context.Background] internally;
// to specify the context, use [DB.ExecCtx].
func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return core.Exec(context.Background(), db.conn, db.bind, db.structTag, query, args...)
}

// ExecCtx is like [DB.Exec], with context.
func (db *DB) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, db.conn, db.bind, db.structTag, query, args...)
}

// Tx is an in-progress database transaction.
//
// A transaction must end with a call to [Tx.Commit] or [Tx.Rollback].
//
// After a call to [Tx.Commit] or [Tx.Rollback], all operations on the
// transaction fail with [ErrTxDone].
type Tx struct {
	conn      *sql.Tx
	bind      binds.Bind
	structTag string
	scanner   core.Scanner
}

// Conn return the underlying [*sql.Tx].
func (tx *Tx) Conn() *sql.Tx { return tx.conn }

// Commit commits the transaction.
func (tx *Tx) Commit() error { return tx.conn.Commit() }

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error { return tx.conn.Rollback() }

// Query executes a query that returns rows, typically a SELECT.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Query uses [context.Background] internally;
// to specify the context, use [Tx.QueryCtx].
func (tx *Tx) Query(dst any, query string, args ...any) error {
	return core.Query(context.Background(), tx.conn, tx.bind, tx.scanner, tx.structTag, dst, query, args...)
}

// QueryCtx is like [Tx.Query], with context.
func (tx *Tx) QueryCtx(ctx context.Context, dst any, query string, args ...any) error {
	return core.Query(ctx, tx.conn, tx.bind, tx.scanner, tx.structTag, dst, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// If the query selects no rows, will return an error which IsNotFound(err) is true.
// Otherwise, scans the first row and discards the rest.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// QueryRow uses [context.Background] internally;
// to specify the context, use [Tx.QueryRowCtx].
func (tx *Tx) QueryRow(dst any, query string, args ...any) error {
	return core.QueryRow(context.Background(), tx.conn, tx.bind, tx.scanner, tx.structTag, dst, query, args...)
}

// QueryRowCtx is like [Tx.QueryRow], with context.
func (tx *Tx) QueryRowCtx(ctx context.Context, dst any, query string, args ...any) error {
	return core.QueryRow(ctx, tx.conn, tx.bind, tx.scanner, tx.structTag, dst, query, args...)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// If args is an array, it will expand query and args for a batch INSERT.
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Exec uses [context.Background] internally;
// to specify the context, use [Tx.ExecCtx].
func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return core.Exec(context.Background(), tx.conn, tx.bind, tx.structTag, query, args...)
}

// ExecCtx is like [Tx.Exec], with context.
func (tx *Tx) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, tx.conn, tx.bind, tx.structTag, query, args...)
}
