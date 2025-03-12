package core

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/rafaberaldo/sqlz/internal/named"
	"github.com/rafaberaldo/sqlz/internal/parser"
)

// Querier can be [sql.DB], [sql.Tx] or [sql.Conn]
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func Query(ctx context.Context, db Querier, bind parser.Bind, dst any, query string, args ...any) error {
	rows, err := QueryDecider(ctx, db, bind, query, args...)

	if err != nil {
		return fmt.Errorf("sqlz: querying multiple rows: %w", err)
	}

	if err := sqlscan.ScanAll(dst, rows); err != nil {
		return fmt.Errorf("sqlz: scanning multiple rows: %w", err)
	}

	return nil
}

func QueryRow(ctx context.Context, db Querier, bind parser.Bind, dst any, query string, args ...any) error {
	rows, err := QueryDecider(ctx, db, bind, query, args...)

	if err != nil {
		return fmt.Errorf("sqlz: querying single row: %w", err)
	}

	if err := sqlscan.ScanOne(dst, rows); err != nil {
		return fmt.Errorf("sqlz: scanning single row: %w", err)
	}

	return nil
}

func QueryDecider(ctx context.Context, db Querier, bind parser.Bind, query string, args ...any) (*sql.Rows, error) {
	// no args, just query directly
	if len(args) == 0 {
		return db.QueryContext(ctx, query)
	}

	// args >1, it's a regular query with `IN` clause parsing
	if len(args) > 1 {
		q, args, err := parser.ParseIn(bind, query, args...)
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
		q, args, err := named.Compile(bind, query, arg)
		if err != nil {
			return nil, err
		}
		return db.QueryContext(ctx, q, args...)
	}

	// otherwise it's a regular query with `IN` clause parsing
	q, args, err := parser.ParseIn(bind, query, args...)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, q, args...)
}

func Exec(ctx context.Context, db Querier, bind parser.Bind, query string, args ...any) (sql.Result, error) {
	// if no args, or args >1 then it's a regular exec
	if len(args) == 0 || len(args) > 1 {
		return db.ExecContext(ctx, query, args...)
	}

	// only one arg, if first element is struct/map/slice then its named
	arg := args[0]
	kind := reflect.TypeOf(arg).Kind()
	switch kind {
	case reflect.Map, reflect.Struct, reflect.Array, reflect.Slice:
		q, args, err := named.Compile(bind, query, arg)
		if err != nil {
			return nil, err
		}
		return db.ExecContext(ctx, q, args...)
	}

	// otherwise it's a regular exec
	return db.ExecContext(ctx, query, args...)
}
