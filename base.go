package sqlz

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rfberaldo/sqlz/internal/parser"
	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// querier is satisfied by [sql.DB], [sql.Tx] or [sql.Conn].
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// base contains main methods that are shared between [DB] and [Tx].
type base struct {
	*config
}

func (c *base) resolveQuery(query string, args []any) (string, []any, error) {
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
			return "", nil, fmt.Errorf("sqlz: too many arguments: want 1 got %d", len(args))
		}
		return processNamed(query, args[0], c.config)

	case reflectutil.Invalid:
		panic(fmt.Sprintf("sqlz: unsupported argument type: %T", args[0]))

	default:
		// must be a native query, just parse for possible "IN" clauses
		return parser.ParseInClause(c.bind, query, args)
	}
}

func (c *base) query(ctx context.Context, db querier, query string, args ...any) *Scanner {
	query, args, err := c.resolveQuery(query, args)
	if err != nil {
		return &Scanner{err: err}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &Scanner{err: err}
	}

	return newScanner(rows, c.config)
}

func (c *base) queryRow(ctx context.Context, db querier, query string, args ...any) *Scanner {
	query, args, err := c.resolveQuery(query, args)
	if err != nil {
		return &Scanner{err: err}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return &Scanner{err: err}
	}

	return newRowScanner(rows, c.config)
}

func (c *base) exec(ctx context.Context, db querier, query string, args ...any) (sql.Result, error) {
	query, args, err := c.resolveQuery(query, args)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
