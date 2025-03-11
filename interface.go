package sqlz

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/rafaberaldo/sqlz/internal/named"
	"github.com/rafaberaldo/sqlz/internal/parser"
)

// querier can be [sql.DB], [sql.Tx] or [sql.Conn]
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func QueryCtx[T any](ctx context.Context, db querier, query string, args ...any) ([]T, error) {
	return _query[T](ctx, db, query, args...)
}

func Query[T any](db querier, query string, args ...any) ([]T, error) {
	return _query[T](context.Background(), db, query, args...)
}

func QueryRowCtx[T any](ctx context.Context, db querier, query string, args ...any) (T, error) {
	return queryRow[T](ctx, db, query, args...)
}

func QueryRow[T any](db querier, query string, args ...any) (T, error) {
	return queryRow[T](context.Background(), db, query, args...)
}

func ExecCtx(ctx context.Context, db querier, query string, args ...any) (sql.Result, error) {
	return exec(ctx, db, query, args...)
}

func Exec(db querier, query string, args ...any) (sql.Result, error) {
	return exec(context.Background(), db, query, args...)
}

func _query[T any](ctx context.Context, db querier, query string, args ...any) (data []T, err error) {
	rows, err := queryDecider(ctx, db, query, args...)

	if err != nil {
		return data, fmt.Errorf("sqlz: querying multiple rows: %w", err)
	}

	if err := sqlscan.ScanAll(&data, rows); err != nil {
		return data, fmt.Errorf("sqlz: scanning multiple rows: %w", err)
	}

	return data, nil
}

func queryRow[T any](ctx context.Context, db querier, query string, args ...any) (data T, err error) {
	rows, err := queryDecider(ctx, db, query, args...)

	if err != nil {
		return data, fmt.Errorf("sqlz: querying single row: %w", err)
	}

	if err := sqlscan.ScanOne(&data, rows); err != nil {
		return data, fmt.Errorf("sqlz: scanning single row: %w", err)
	}

	return data, nil
}

func queryDecider(ctx context.Context, db querier, query string, args ...any) (*sql.Rows, error) {
	// no args, just query directly
	if len(args) == 0 {
		return db.QueryContext(ctx, query)
	}

	// args >1, it's a regular query with `IN` clause parsing
	if len(args) > 1 {
		q, args, err := parser.ParseIn(defs().bind, query, args...)
		if err != nil {
			return nil, err
		}
		return db.QueryContext(ctx, q, args...)
	}

	// only one arg, if first element is struct/map then its named
	arg := args[0]
	kind := reflect.TypeOf(arg).Kind()
	switch kind {
	case reflect.Map, reflect.Struct:
		q, args, err := named.Compile(defs().bind, query, arg)
		if err != nil {
			return nil, err
		}
		return db.QueryContext(ctx, q, args...)
	}

	// otherwise it's a regular query with `IN` clause parsing
	q, args, err := parser.ParseIn(defs().bind, query, args...)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, q, args...)
}

func exec(ctx context.Context, db querier, query string, args ...any) (sql.Result, error) {
	// if no args, or args >1 then it's a regular exec
	if len(args) == 0 || len(args) > 1 {
		return db.ExecContext(ctx, query, args...)
	}

	// only one arg, if first element is struct/map/slice then its named
	arg := args[0]
	kind := reflect.TypeOf(arg).Kind()
	switch kind {
	case reflect.Map, reflect.Struct, reflect.Array, reflect.Slice:
		q, args, err := named.Compile(defs().bind, query, arg)
		if err != nil {
			return nil, err
		}
		return db.ExecContext(ctx, q, args...)
	}

	// otherwise it's a regular exec
	return db.ExecContext(ctx, query, args...)
}
