package sqlu

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/rafaberaldo/sqlz/internal/core"
	"github.com/rafaberaldo/sqlz/internal/parser"
)

var defaultBind atomic.Value

func init() {
	defaultBind.Store(parser.BindQuestion)
}

// SetDefaultBind sets the package-level bindvar placeholder.
func SetDefaultBind(bind parser.Bind) {
	defaultBind.Store(bind)
}

// bind returns the package-level default [parser.Bind].
func bind() parser.Bind { return defaultBind.Load().(parser.Bind) }

const (
	BindAt       = parser.BindAt       // BindAt is the placeholder '@p1'
	BindColon    = parser.BindColon    // BindColon is the placeholder ':name'
	BindDollar   = parser.BindDollar   // BindDollar is the placeholder '$1'
	BindQuestion = parser.BindQuestion // BindQuestion is the placeholder '?'
)

// Query executes a query that returns rows, typically a SELECT.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The db parameter accepts [sql.DB], [sql.Tx] or [sql.Conn].
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Query uses [context.Background] internally;
// to specify the context, use [QueryCtx].
func Query[T any](db core.Querier, query string, args ...any) ([]T, error) {
	return QueryCtx[T](context.Background(), db, query, args...)
}

// QueryCtx is like [Query], with context.
func QueryCtx[T any](ctx context.Context, db core.Querier, query string, args ...any) ([]T, error) {
	var data []T
	err := core.Query(ctx, db, bind(), &data, query, args...)
	return data, err
}

// QueryRow executes a query that is expected to return at most one row.
// If the query selects no rows, will return an error which IsNotFound(err) is true.
// Otherwise, scans the first row and discards the rest.
// Returned rows will be scaned to dst.
// The args are for any placeholder parameters in the query.
//
// The db parameter accepts [sql.DB], [sql.Tx] or [sql.Conn].
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// QueryRow uses [context.Background] internally;
// to specify the context, use [QueryRowCtx].
func QueryRow[T any](db core.Querier, query string, args ...any) (T, error) {
	return QueryRowCtx[T](context.Background(), db, query, args...)
}

// QueryRowCtx is like [QueryRow], with context.
func QueryRowCtx[T any](ctx context.Context, db core.Querier, query string, args ...any) (T, error) {
	var data T
	err := core.QueryRow(ctx, db, bind(), &data, query, args...)
	return data, err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
//
// The db parameter accepts [sql.DB], [sql.Tx] or [sql.Conn].
//
// The default placeholder depends on the driver.
// The placeholder for any driver can be in the format of a colon
// followed by the key of the map or struct, e.g. :id, :name, etc.
// A struct can have a struct-tag `db:"id"`, `db:"name"`, etc.
//
// Exec uses [context.Background] internally;
// to specify the context, use [ExecCtx].
func Exec(db core.Querier, query string, args ...any) (sql.Result, error) {
	return ExecCtx(context.Background(), db, query, args...)
}

// ExecCtx is like [Exec], with context.
func ExecCtx(ctx context.Context, db core.Querier, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, db, bind(), query, args...)
}

// Connect opens a database specified by its database driver name and a
// driver-specific data source name, then verify the connection with a Ping.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// The returned [*sql.DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Connect
// function should be called just once.
func Connect(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sqlz: unable to open sql connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlz: unable to ping: %w", err)
	}

	return db, nil
}

// MustConnect is like [Connect], but panics on error.
func MustConnect(driverName, dataSourceName string) *sql.DB {
	db, err := Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// IsNotFound is a helper to check if err contains [sql.ErrNoRows].
func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
