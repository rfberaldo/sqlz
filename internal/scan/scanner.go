package scan

import (
	"database/sql"
	"fmt"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

type ColBinding struct {
	columns []string
	values  []any
	ptrs    []any
}

func NewColBinding(columns []string) *ColBinding {
	cb := &ColBinding{
		columns: columns,
		values:  make([]any, len(columns)),
		ptrs:    make([]any, len(columns)),
	}

	for i := range cb.values {
		cb.ptrs[i] = &cb.values[i]
	}

	return cb
}

// Rows is [sql.Rows]
type Rows interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	NextResultSet() bool
	Scan(dest ...any) error
}

type Scanner struct {
	queryRow bool
	rows     Rows
}

func (r *Scanner) Scan(arg any) (err error) {
	argType := reflectutil.TypeOf(arg)
	switch argType {
	case reflectutil.Invalid:
		return fmt.Errorf("sqlz/scan: arg type is not valid")

	// maps and slices are references by default
	case reflectutil.Primitive, reflectutil.Struct:
		return fmt.Errorf("sqlz/scan: arg must be a pointer")
	}

	defer func() {
		if errClose := r.rows.Close(); errClose != nil {
			err = fmt.Errorf("sqlz/scan: closing rows: %w", errClose)
		}
	}()

	columns, err := r.rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlz/scan: getting column names: %w", err)
	}

	count := 0
	for r.rows.Next() {
		cb := NewColBinding(columns)
		if err := r.rows.Scan(cb.ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		switch argType {
		case reflectutil.Map, reflectutil.PointerMap:
			if err := scanMap(arg, cb); err != nil {
				return err
			}
		}

		count++
		if r.queryRow && count > 1 {
			return fmt.Errorf("sqlz/scan: expected one row, but got multiple")
		}
	}

	if err := r.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if r.queryRow && count == 0 {
		return sql.ErrNoRows
	}

	return err
}

func scanMap(arg any, cb *ColBinding) error {
	m, ok := asMap(arg)
	if !ok {
		return fmt.Errorf("sqlz/scan: maps must be of type map[string]any")
	}

	for i, col := range cb.columns {
		v := cb.values[i]
		if v, ok := v.([]byte); ok {
			m[col] = string(v)
			continue
		}
		m[col] = v
	}

	return nil
}

func asMap(arg any) (map[string]any, bool) {
	switch v := arg.(type) {
	case map[string]any:
		if v == nil {
			v = make(map[string]any)
		}
		return v, true

	case *map[string]any:
		if *v == nil {
			*v = make(map[string]any)
		}
		return *v, true

	default:
		return nil, false
	}
}
