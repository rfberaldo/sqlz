package core

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rfberaldo/sqlz/parser"
	"github.com/rfberaldo/sqlz/reflectutil"
)

// Querier can be [sql.DB], [sql.Tx] or [sql.Conn]
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func Query(
	ctx context.Context,
	db Querier,
	bind parser.Bind,
	structTag string,
	dst any,
	query string,
	args ...any,
) error {
	query, args, err := resolveQuery(bind, structTag, query, args...)
	if err != nil {
		return fmt.Errorf("sqlz: parsing query: %w", err)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	scanner, err := NewScanner(rows, &ScannerOptions{
		StructTag: structTag,
	})
	if err != nil {
		return fmt.Errorf("sqlz: creating scanner: %w", err)
	}

	return scanner.Scan(dst)
}

func QueryRow(
	ctx context.Context,
	db Querier,
	bind parser.Bind,
	structTag string,
	dst any,
	query string,
	args ...any,
) error {
	query, args, err := resolveQuery(bind, structTag, query, args...)
	if err != nil {
		return fmt.Errorf("sqlz: parsing query: %w", err)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	scanner, err := NewScanner(rows, &ScannerOptions{
		QueryRow:  true,
		StructTag: structTag,
	})
	if err != nil {
		return fmt.Errorf("sqlz: creating scanner: %w", err)
	}

	return scanner.Scan(dst)
}

func Exec(
	ctx context.Context,
	db Querier,
	bind parser.Bind,
	structTag string,
	query string,
	args ...any,
) (sql.Result, error) {
	query, args, err := resolveQuery(bind, structTag, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlz: parsing query: %w", err)
	}

	return db.ExecContext(ctx, query, args...)
}

func resolveQuery(bind parser.Bind, structTag string, query string, args ...any) (string, []any, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", nil, fmt.Errorf("sqlz: query cannot be blank")
	}

	if len(args) == 0 {
		return query, args, nil
	}

	switch reflectutil.TypeOfAny(args[0]) {
	case reflectutil.Struct, reflectutil.Map,
		reflectutil.SliceStruct, reflectutil.SliceMap:
		if len(args) > 1 {
			return "", nil, fmt.Errorf("sqlz: too many arguments in %T", args)
		}
		return ProcessNamed(query, args[0], &NamedOptions{
			Bind:      bind,
			StructTag: structTag,
		})

	case reflectutil.Invalid:
		panic(fmt.Errorf("sqlz: unsupported argument type: %T", args))

	default:
		// must be a native query, just parse for possible "IN" clauses
		return parser.ParseInClauseNative(bind, query, args...)
	}
}
