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
	hasSetup   bool
	columns    []string
	queryRow   bool
	queryError error
	rowCount   int
	rows       *sql.Rows
}

// setup must be called before scanning rows.
func (r *Scanner) setup() (err error) {
	if r.hasSetup {
		return nil
	}

	if r.queryError != nil {
		return r.queryError
	}

	r.columns, err = r.rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlz/scan: getting column names: %w", err)
	}

	if len(r.columns) == 0 {
		return fmt.Errorf("sqlz/scan: columns length must be > 0")
	}

	r.hasSetup = true
	return nil
}

// cleanup must be called after scanning rows.
func (r *Scanner) cleanup() error {
	if err := r.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if r.queryRow && r.rowCount > 1 {
		return fmt.Errorf("sqlz/scan: expected one row, but got %d", r.rowCount)
	}

	if r.queryRow && r.rowCount == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// Scan automatically iterates over rows and scans results into arg.
// arg must be a pointer of any primitive type, map, struct or slices.
func (r *Scanner) Scan(arg any) (err error) {
	if err := r.setup(); err != nil {
		return err
	}

	if reflect.TypeOf(arg).Kind() != reflect.Pointer {
		return fmt.Errorf("sqlz/scan: arg must be a pointer")
	}

	argType := reflectutil.TypeOf(arg)

	if argType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: arg type is not valid")
	}

	expectOneCol := argType == reflectutil.Primitive ||
		argType == reflectutil.SlicePrimitive

	if expectOneCol && len(r.columns) != 1 {
		return fmt.Errorf("sqlz/scan: expected 1 column, got %d", len(r.columns))
	}

	defer func() {
		if errClose := r.rows.Close(); errClose != nil {
			err = fmt.Errorf("sqlz/scan: closing rows: %w", errClose)
		}
	}()

	switch argType {
	case reflectutil.Primitive:
		err = r.scanPrimitive(arg)

	case reflectutil.SlicePrimitive:
		err = r.scanSlicePrimitive(arg)

	case reflectutil.Map:
		err = r.scanMap(arg)

	case reflectutil.SliceMap:
		err = r.scanSliceMap(arg)

	case reflectutil.Struct:
		err = r.scanStruct(arg)

	case reflectutil.SliceStruct:
		err = r.scanSliceStruct(arg)
	}

	if err != nil {
		return err
	}

	if err := r.cleanup(); err != nil {
		return err
	}

	return err
}

// ScanRow is like [sql.Rows.Scan].
func (r *Scanner) ScanRow(args ...any) error {
	if err := r.setup(); err != nil {
		return err
	}

	if err := r.rows.Scan(args...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

func (r *Scanner) scanPrimitive(arg any) error {
	for r.rows.Next() {
		if err := r.ScanRow(arg); err != nil {
			return err
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
		if err := r.ScanRow(v.Interface()); err != nil {
			return err
		}

		sliceValue.Set(reflect.Append(sliceValue, v.Elem()))

		r.rowCount++
	}

	return nil
}

func (r *Scanner) ScanMap(arg any) error {
	if err := r.setup(); err != nil {
		return err
	}

	mapValue := reflectutil.DerefValue(reflect.ValueOf(arg))

	m, ok := mapValue.Interface().(map[string]any)
	if !ok {
		return fmt.Errorf("sqlz/scan: map must be of type map[string]any")
	}

	cb := newColBinding(r.columns)
	if err := r.ScanRow(cb.ptrs...); err != nil {
		return err
	}

	for i, col := range r.columns {
		m[col] = cb.value(i)
	}

	return nil
}

func (r *Scanner) scanMap(arg any) error {
	for r.rows.Next() {
		if err := r.ScanMap(arg); err != nil {
			return err
		}
		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceMap(arg any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(arg))

	for r.rows.Next() {
		cb := newColBinding(r.columns)
		if err := r.ScanRow(cb.ptrs...); err != nil {
			return err
		}

		m := make(map[string]any, len(r.columns))
		for i, col := range r.columns {
			m[col] = cb.value(i)
		}

		sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(m)))

		r.rowCount++
	}

	return nil
}

func (r *Scanner) ScanStruct(arg any) error {
	if err := r.setup(); err != nil {
		return err
	}

	stv := reflectutil.NewStruct("db", SnakeCaseMapper)

	ptrs, err := structPtrs(stv, reflect.ValueOf(arg), r.columns)
	if err != nil {
		return err
	}

	if err := r.ScanRow(ptrs...); err != nil {
		return err
	}

	return nil
}

func (r *Scanner) scanStruct(arg any) error {
	stv := reflectutil.NewStruct("db", SnakeCaseMapper)

	for r.rows.Next() {
		ptrs, err := structPtrs(stv, reflect.ValueOf(arg), r.columns)
		if err != nil {
			return err
		}

		if err := r.ScanRow(ptrs...); err != nil {
			return err
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceStruct(arg any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(arg))
	stv := reflectutil.NewStruct("db", SnakeCaseMapper)

	for r.rows.Next() {
		structValue := reflect.New(sliceValue.Type().Elem())
		ptrs, err := structPtrs(stv, structValue, r.columns)
		if err != nil {
			return err
		}

		if err := r.ScanRow(ptrs...); err != nil {
			return err
		}

		sliceValue.Set(reflect.Append(sliceValue, structValue.Elem()))

		r.rowCount++
	}

	return nil
}

// Close closes [Scanner], preventing further enumeration, and returning the connection to the pool.
// Close is idempotent and does not affect the result of [Scanner.Err].
func (r *Scanner) Close() error { return r.rows.Close() }

// NextRow prepares the next result row for reading with [Scanner.ScanMap] or [Scanner.ScanStruct] methods.
// It returns true on success, or false if there is no next result row or an error
// happened while preparing it. [Scanner.Err] should be consulted to distinguish between
// the two cases.
//
// Every call to [Scanner.ScanMap] or [Scanner.ScanStruct], even the first one,
// must be preceded by a call to [Scanner.NextRow].
func (r *Scanner) NextRow() bool { return r.rows.Next() }

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit [Scanner.Close].
func (r *Scanner) Err() error { return r.rows.Err() }
