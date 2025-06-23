package core

import (
	"context"
	"database/sql"
	"reflect"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/rfberaldo/sqlz/internal/binds"
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
//	1 arg map/struct: perform a named query.
//	Anything else: parse query for `IN` clause and then query.
//
// Query rows will be scanned to dst.
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
		if err == dbscan.ErrNotFound {
			return sql.ErrNoRows
		}
		return err
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
		kind := reflect.TypeOf(args[0]).Kind()
		// 1 arg map/struct is a named query
		if kind == reflect.Map || kind == reflect.Struct {
			q, args, err := named.Compile(bind, structTag, query, args[0])
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
//	No args: perform a regular exec.
//	1 arg map/struct: perform a named exec.
//	1 arg array/slice with map/struct items: perform a named batch insert.
//	Anything else: parse query for `IN` clause and then exec.
func Exec(
	ctx context.Context,
	db Querier,
	bind binds.Bind,
	structTag string,
	query string,
	args ...any,
) (sql.Result, error) {
	// no args, just exec directly
	if len(args) == 0 {
		return db.ExecContext(ctx, query)
	}

	if len(args) == 1 && isNamedExec(args[0]) {
		q, args, err := named.Compile(bind, structTag, query, args[0])
		if err != nil {
			return nil, err
		}
		return db.ExecContext(ctx, q, args...)
	}

	// otherwise it's a regular exec with `IN` clause parsing
	q, args, err := parser.ParseIn(bind, query, args...)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, q, args...)
}

func isNamedExec(arg any) bool {
	kind := reflect.TypeOf(arg).Kind()

	// 1 arg map/struct is a named exec
	if kind == reflect.Map || kind == reflect.Struct {
		return true
	}

	// 1 arg array/slice is a batch insert if items are map/struct
	if kind == reflect.Array || kind == reflect.Slice {
		elValue := reflect.ValueOf(arg).Index(0)
		elKind := elValue.Kind()

		if elKind == reflect.Ptr {
			elKind = elValue.Elem().Kind()
		}

		return elKind == reflect.Map || elKind == reflect.Struct
	}

	return false
}
