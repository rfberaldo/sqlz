package scan

import (
	"database/sql"
	"fmt"
	"reflect"

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

func (cb *ColBinding) Value(i int) any {
	v := cb.values[i]
	if v, ok := v.([]byte); ok {
		return string(v)
	}
	return v
}

// Rows is [sql.Rows]
type Rows interface {
	Close() error
	// ColumnTypes() ([]*sql.ColumnType, error)
	Columns() ([]string, error)
	Err() error
	Next() bool
	// NextResultSet() bool
	Scan(dest ...any) error
}

type Scanner struct {
	queryRow bool
	rowCount int
	rows     Rows
}

func (r *Scanner) Scan(arg any) (err error) {
	if reflect.TypeOf(arg).Kind() != reflect.Pointer {
		return fmt.Errorf("sqlz/scan: arg must be a pointer")
	}

	argType := reflectutil.TypeOf(arg)

	if argType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: arg type is not valid")
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

	switch argType {
	case reflectutil.Primitive:
		if len(columns) != 1 {
			return fmt.Errorf("sqlz/scan: expected 1 column, got %d", len(columns))
		}

		if err := r.scanPrimitive(arg); err != nil {
			return err
		}

	case reflectutil.Map:
		if err := r.scanMap(arg, columns); err != nil {
			return err
		}

	case reflectutil.SliceMap:
		if err := r.scanSliceMap(arg, columns); err != nil {
			return err
		}
	}

	if err := r.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if r.queryRow && r.rowCount > 1 {
		return fmt.Errorf("sqlz/scan: expected one row, but got %d", r.rowCount)
	}

	if r.queryRow && r.rowCount == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (r *Scanner) scanPrimitive(arg any) (err error) {
	for r.rows.Next() {
		if err := r.rows.Scan(arg); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanMap(arg any, columns []string) error {
	v := reflectutil.DerefValue(reflect.ValueOf(arg))
	if !v.IsValid() {
		return fmt.Errorf("sqlz/scan: unexpected arg")
	}

	m, ok := v.Interface().(map[string]any)
	if !ok {
		return fmt.Errorf("sqlz/scan: map must be of type map[string]any")
	}

	for r.rows.Next() {
		cb := NewColBinding(columns)
		if err := r.rows.Scan(cb.ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		for i, col := range cb.columns {
			m[col] = cb.Value(i)
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceMap(arg any, columns []string) error {
	v := reflectutil.DerefValue(reflect.ValueOf(arg))
	if !v.IsValid() {
		return fmt.Errorf("sqlz/scan: unexpected arg")
	}

	for r.rows.Next() {
		cb := NewColBinding(columns)
		if err := r.rows.Scan(cb.ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		m := make(map[string]any, len(columns))
		for i, col := range cb.columns {
			m[col] = cb.Value(i)
		}
		reflectutil.Append(v, m)

		r.rowCount++
	}

	return nil
}
