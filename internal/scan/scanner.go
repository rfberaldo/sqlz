package scan

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// Rows is [sql.Rows]
type Rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
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

	if len(columns) == 0 {
		return fmt.Errorf("sqlz/scan: columns length must be > 0")
	}

	expectOneCol := argType == reflectutil.Primitive ||
		argType == reflectutil.SlicePrimitive

	if expectOneCol && len(columns) != 1 {
		return fmt.Errorf("sqlz/scan: expected 1 column, got %d", len(columns))
	}

	switch argType {
	case reflectutil.Primitive:
		err = r.scanPrimitive(arg)

	case reflectutil.SlicePrimitive:
		err = r.scanSlicePrimitive(arg)

	case reflectutil.Map:
		err = r.scanMap(arg, columns)

	case reflectutil.SliceMap:
		err = r.scanSliceMap(arg, columns)

	case reflectutil.Struct:
		err = r.scanStruct(arg, columns)

	case reflectutil.SliceStruct:
		err = r.scanSliceStruct(arg, columns)

	}

	if err != nil {
		return err
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

func (r *Scanner) scanSlicePrimitive(arg any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(arg))
	elType := sliceValue.Type().Elem()

	for r.rows.Next() {
		v := reflect.New(elType)
		if err := r.rows.Scan(v.Interface()); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		sliceValue.Set(reflect.Append(sliceValue, v.Elem()))

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanMap(arg any, columns []string) error {
	mapValue := reflectutil.DerefValue(reflect.ValueOf(arg))

	m, ok := mapValue.Interface().(map[string]any)
	if !ok {
		return fmt.Errorf("sqlz/scan: map must be of type map[string]any")
	}

	for r.rows.Next() {
		cb := newColBinding(columns)
		if err := r.rows.Scan(cb.ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		for i, col := range cb.columns {
			m[col] = cb.value(i)
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceMap(arg any, columns []string) error {
	argValue := reflectutil.DerefValue(reflect.ValueOf(arg))

	for r.rows.Next() {
		cb := newColBinding(columns)
		if err := r.rows.Scan(cb.ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		m := make(map[string]any, len(columns))
		for i, col := range cb.columns {
			m[col] = cb.value(i)
		}

		argValue.Set(reflect.Append(argValue, reflect.ValueOf(m)))

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanStruct(arg any, columns []string) error {
	stv := reflectutil.NewStruct("db", SnakeCaseMapper)

	for r.rows.Next() {
		ptrs, err := structPtrs(stv, reflect.ValueOf(arg), columns)
		if err != nil {
			return err
		}

		if err := r.rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceStruct(arg any, columns []string) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(arg))
	stv := reflectutil.NewStruct("db", SnakeCaseMapper)

	for r.rows.Next() {
		structValue := reflect.New(sliceValue.Type().Elem())
		ptrs, err := structPtrs(stv, structValue, columns)
		if err != nil {
			return err
		}

		if err := r.rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sqlz/scan: scanning row: %w", err)
		}

		sliceValue.Set(reflect.Append(sliceValue, structValue.Elem()))

		r.rowCount++
	}

	return nil
}
