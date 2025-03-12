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

func SetDefaultBind(bind parser.Bind) {
	defaultBind.Store(bind)
}

// bind returns the default [parser.Bind].
func bind() parser.Bind { return defaultBind.Load().(parser.Bind) }

// querier can be [sql.DB], [sql.Tx] or [sql.Conn]
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func QueryCtx[T any](ctx context.Context, db querier, query string, args ...any) ([]T, error) {
	var data []T
	err := core.Query(ctx, db, bind(), &data, query, args...)
	return data, err
}

func Query[T any](db querier, query string, args ...any) ([]T, error) {
	var data []T
	err := core.Query(context.Background(), db, bind(), &data, query, args...)
	return data, err
}

func QueryRowCtx[T any](ctx context.Context, db querier, query string, args ...any) (T, error) {
	var data T
	err := core.QueryRow(ctx, db, bind(), &data, query, args...)
	return data, err
}

func QueryRow[T any](db querier, query string, args ...any) (T, error) {
	var data T
	err := core.QueryRow(context.Background(), db, bind(), &data, query, args...)
	return data, err
}

func ExecCtx(ctx context.Context, db querier, query string, args ...any) (sql.Result, error) {
	return core.Exec(ctx, db, bind(), query, args...)
}

func Exec(db querier, query string, args ...any) (sql.Result, error) {
	return core.Exec(context.Background(), db, bind(), query, args...)
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
