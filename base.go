package sqlz

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rfberaldo/sqlz/internal/parser"
	"github.com/rfberaldo/sqlz/internal/reflectutil"
	"github.com/rfberaldo/sqlz/internal/stmtcache"
)

// querier is satisfied by [sql.DB], [sql.Tx] or [sql.Conn].
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// base contains main methods that are shared between [DB] and [Tx].
type base struct {
	*config
	stmtCache *stmtcache.StmtCache
}

func newBase(cfg *config) *base {
	cfg = applyDefaults(cfg)
	base := &base{config: cfg}

	if cfg.stmtCacheCapacity > 0 {
		base.stmtCache = stmtcache.New(cfg.stmtCacheCapacity)
	}

	return base
}

func (c *base) resolveQuery(query string, args []any) (string, []any, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", nil, fmt.Errorf("sqlz: query cannot be blank")
	}

	if len(args) == 0 {
		return query, nil, nil
	}

	argType := reflectutil.TypeOfAny(args[0])

	if argType == reflectutil.Invalid {
		panic(fmt.Sprintf("sqlz: unsupported argument type: %T", args[0]))
	}

	if argType.IsNamed() {
		if len(args) > 1 {
			return "", nil, fmt.Errorf("sqlz: too many arguments for named query, want 1 got %d", len(args))
		}
		return processNamed(query, args[0], c.config)
	}

	// must be a native query, just parse for possible "IN" clauses
	return parser.ParseInClause(c.bind, query, args)
}

func (c *base) query(ctx context.Context, db querier, query string, args ...any) *Scanner {
	query, args, err := c.resolveQuery(query, args)
	if err != nil {
		return &Scanner{err: err}
	}

	if c.stmtCache == nil || len(args) == 0 {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return &Scanner{err: err}
		}
		return newScanner(rows, c.config)
	}

	stmt, err := c.loadOrPrepare(ctx, db, query)
	if err != nil {
		return &Scanner{err: err}
	}
	rows, err := stmt.QueryContext(ctx, args...)
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

	if c.stmtCache == nil || len(args) == 0 {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return &Scanner{err: err}
		}
		return newRowScanner(rows, c.config)
	}

	stmt, err := c.loadOrPrepare(ctx, db, query)
	if err != nil {
		return &Scanner{err: err}
	}
	rows, err := stmt.QueryContext(ctx, args...)
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

	if c.stmtCache == nil || len(args) == 0 {
		return db.ExecContext(ctx, query, args...)
	}

	stmt, err := c.loadOrPrepare(ctx, db, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (c *base) loadOrPrepare(ctx context.Context, db querier, query string) (*sql.Stmt, error) {
	if c.stmtCache == nil {
		panic("sqlz: stmt cache is not enabled")
	}

	stmt, ok := c.stmtCache.Get(query)
	if !ok {
		var err error
		stmt, err = db.PrepareContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("sqlz: preparing stmt: %w", err)
		}
		c.stmtCache.Put(query, stmt)
	}

	return stmt.(*sql.Stmt), nil
}

// closeStmts closes all cached statements, if any.
func (c *base) closeStmts() {
	if c.stmtCache == nil {
		return
	}
	c.stmtCache.Clear()
}
