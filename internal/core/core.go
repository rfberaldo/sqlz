package core

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/rfberaldo/sqlz/binds"
	"github.com/rfberaldo/sqlz/internal/named"
	"github.com/rfberaldo/sqlz/internal/parser"
)

// Querier can be [sql.DB], [sql.Tx] or [sql.Conn]
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Scanner interface {
	ScanOne(dst any, rows dbscan.Rows) error
	ScanAll(dst any, rows dbscan.Rows) error
	StructTagKey() string
}

// Query will adapt depending on args:
//
//	No args: perform a regular query.
//	1 arg struct/map: perform a named query.
//	Anything else: parse query for `IN` clause and then query.
//
// Query return values will be scanned to dst.
func Query(
	ctx context.Context,
	db Querier,
	bind binds.Bind,
	scanner Scanner,
	dst any,
	query string,
	args ...any,
) error {
	rows, err := queryDecider(ctx, db, bind, scanner.StructTagKey(), query, args...)
	if err != nil {
		return err
	}

	if err := scanner.ScanAll(dst, rows); err != nil {
		return err
	}

	return nil
}

// QueryRow is like [Query], but will only scan one row,
// will return error if query result is more than one row.
func QueryRow(
	ctx context.Context,
	db Querier,
	bind binds.Bind,
	scanner Scanner,
	dst any,
	query string,
	args ...any,
) error {
	rows, err := queryDecider(ctx, db, bind, scanner.StructTagKey(), query, args...)
	if err != nil {
		return err
	}

	if err := scanner.ScanOne(dst, rows); err != nil {
		return errors.Join(sql.ErrNoRows, err)
	}

	return nil
}

func queryDecider(
	ctx context.Context,
	db Querier,
	bind binds.Bind,
	structTag string,
	query string,
	args ...any,
) (*sql.Rows, error) {
	// no args, just query directly
	if len(args) == 0 {
		return db.QueryContext(ctx, query)
	}

	if len(args) == 1 {
		arg := args[0]
		kind := reflect.TypeOf(arg).Kind()
		switch kind {
		// 1 arg map/struct is a named query
		case reflect.Map, reflect.Struct:
			q, args, err := named.Compile(bind, structTag, query, arg)
			if err != nil {
				return nil, err
			}
			return db.QueryContext(ctx, q, args...)
		}
	}

	// otherwise it's a regular query with `IN` clause parsing
	q, args, err := parser.ParseIn(bind, query, args...)
	if err != nil {
		return nil, err
	}
	return db.QueryContext(ctx, q, args...)
}

// Exec will adapt depending on args:
//
//	1 arg struct/map: perform a named exec.
//	1 arg slice/array: perform a named batch insert.
//	Anything else: parse query for `IN` clause and then exec.
func Exec(
	ctx context.Context,
	db Querier,
	bind binds.Bind,
	structTag string,
	query string,
	args ...any,
) (sql.Result, error) {
	if len(args) == 1 {
		arg := args[0]
		kind := reflect.TypeOf(arg).Kind()
		switch kind {
		// 1 arg map/struct/array/slice is a named exec
		case reflect.Map, reflect.Struct, reflect.Array, reflect.Slice:
			q, args, err := named.Compile(bind, structTag, query, arg)
			if err != nil {
				return nil, err
			}
			return db.ExecContext(ctx, q, args...)
		}
	}

	// otherwise it's a regular exec with `IN` clause parsing
	q, args, err := parser.ParseIn(bind, query, args...)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, q, args...)
}
