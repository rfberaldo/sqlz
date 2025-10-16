// Package sqlz is an extension for the standard [database/sql] package.
// It adds named queries, scanning, and batch operations, while prividing a simple API.
package sqlz

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"

	"github.com/rfberaldo/sqlz/internal/parser"
)

const (
	BindAt       = parser.BindAt       // Syntax: '@p1'
	BindColon    = parser.BindColon    // Syntax: ':param'
	BindDollar   = parser.BindDollar   // Syntax: '$1'
	BindQuestion = parser.BindQuestion // Syntax: '?'
)

// Options are optional configs for sqlz.
type Options struct {
	// Bind is the placeholder the database driver uses, this should be blank for most users.
	// Default is based on driver.
	Bind parser.Bind

	// StructTag is the reflection tag that will be used to map struct fields.
	// Default is "db".
	StructTag string

	// FieldNameTransformer transforms a struct field name,
	// it is only used when the struct tag is not found.
	// Default is [ToSnakeCase].
	FieldNameTransformer func(string) string

	// IgnoreMissingFields causes the scanner to ignore missing struct fields
	// rather than returning an error.
	// Default is false.
	IgnoreMissingFields bool
}

// New returns a [DB] instance using an existing [sql.DB].
// The opts parameter can be nil for defaults.
//
// Example:
//
//	pool, err := sql.Open("sqlite3", ":memory:")
//	db := sqlz.New("sqlite3", pool, nil)
func New(driverName string, db *sql.DB, opts *Options) *DB {
	if opts == nil {
		opts = &Options{}
	}

	bind := cmp.Or(opts.Bind, bindByDriverName[driverName])
	if bind == parser.BindUnknown {
		panic(fmt.Sprintf("sqlz: unable to find bind for '%s', set with Options.Bind", driverName))
	}

	cfg := &config{
		bind:                 bind,
		structTag:            opts.StructTag,
		fieldNameTransformer: opts.FieldNameTransformer,
		ignoreMissingFields:  opts.IgnoreMissingFields,
	}
	cfg.defaults()

	return &DB{db, &base{cfg}}
}

// Connect opens a database specified by its database driver name and a
// driver-specific data source name, then verify the connection with a ping.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Connect
// function should be called just once.
func Connect(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sqlz: unable to open sql connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlz: unable to ping connection: %w", err)
	}

	return New(driverName, db, nil), nil
}

// MustConnect is like [Connect], but panics on error.
func MustConnect(driverName, dataSourceName string) *DB {
	db, err := Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// DB is a database handle representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type DB struct {
	pool *sql.DB
	base *base
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

	return &Tx{tx, db.base}, nil
}

// Query executes a query that can return multiple rows. Any errors are deferred
// until [Scanner.Err] or [Scanner.Scan] is called.
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (db *DB) Query(ctx context.Context, query string, args ...any) *Scanner {
	return db.base.query(ctx, db.pool, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// Any errors are deferred until [Scanner.Err] or [Scanner.Scan] is called,
// if the query selects no rows, it returns [sql.ErrNoRows].
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (db *DB) QueryRow(ctx context.Context, query string, args ...any) *Scanner {
	return db.base.queryRow(ctx, db.pool, query, args...)
}

// Exec executes a query without returning any rows.
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.base.exec(ctx, db.pool, query, args...)
}

// Tx is an in-progress database transaction, representing a single connection.
//
// A transaction must end with a call to [Tx.Commit] or [Tx.Rollback], or else
// the connection will be locked.
//
// After a call to [Tx.Commit] or [Tx.Rollback], all operations on the
// transaction fail with [sql.ErrTxDone].
type Tx struct {
	conn *sql.Tx
	base *base
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

// Query executes a query that can return multiple rows. Any errors are deferred
// until [Scanner.Err] or [Scanner.Scan] is called.
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (tx *Tx) Query(ctx context.Context, query string, args ...any) *Scanner {
	return tx.base.query(ctx, tx.conn, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// Any errors are deferred until [Scanner.Err] or [Scanner.Scan] is called,
// if the query selects no rows, it returns [sql.ErrNoRows].
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (tx *Tx) QueryRow(ctx context.Context, query string, args ...any) *Scanner {
	return tx.base.queryRow(ctx, tx.conn, query, args...)
}

// Exec executes a query without returning any rows.
//
// The args are for any placeholder parameters in the query,
// the default placeholder depends on the driver.
//
// Named queries works for all drivers, allowing the use of struct field names or
// map keys as placeholders (e.g. :id, :name), rather than having to refer to parameters positionally.
func (tx *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.base.exec(ctx, tx.conn, query, args...)
}
