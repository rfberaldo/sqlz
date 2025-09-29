package sqlz

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
	columns               []string
	queryRow              bool
	structTag             string
	structFieldNameMapper func(string) string
	rowCount              int
	rows                  Rows
}

type ScannerOptions struct {
	// QueryRow enforces result to be a single row.
	QueryRow bool

	// StructTag is the reflection tag that will used to map fields.
	StructTag string

	// StructFieldNameMapper is used to process a struct field name in case
	// the tag was not found.
	StructFieldNameMapper func(string) string

	// StructIgnoreOmitFields makes omitted struct fields to not return error.
	StructIgnoreOmitFields bool
}

func NewScanner(rows Rows, opts *ScannerOptions) (*Scanner, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("sqlz/scan: getting column names: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("sqlz/scan: columns length must be > 0")
	}

	if opts == nil {
		opts = &ScannerOptions{}
	}

	if opts.StructTag == "" {
		opts.StructTag = defaultStructTag
	}

	if opts.StructFieldNameMapper == nil {
		opts.StructFieldNameMapper = SnakeCaseMapper
	}

	scanner := &Scanner{
		columns:               columns,
		rows:                  rows,
		queryRow:              opts.QueryRow,
		structTag:             opts.StructTag,
		structFieldNameMapper: opts.StructFieldNameMapper,
	}

	return scanner, nil
}

func (r *Scanner) postScan() error {
	if err := r.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if r.queryRow && r.rowCount > 1 {
		return fmt.Errorf("sqlz/scan: expected one row, got %d", r.rowCount)
	}

	if r.queryRow && r.rowCount == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Scanner) checkDest(dest any) error {
	t := reflect.TypeOf(dest)
	if t.Kind() != reflect.Pointer {
		return fmt.Errorf("sqlz/scan: destination must be a pointer, got %T", dest)
	}
	return nil
}

// Scan automatically iterates over rows and scans results into dest.
// dest must be a pointer of any primitive type, map, struct or slices.
func (r *Scanner) Scan(dest any) (err error) {
	if err := r.checkDest(dest); err != nil {
		return err
	}

	destType := reflectutil.TypeOf(dest)

	if destType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: destination type is not valid, got %T", dest)
	}

	expectOneCol := destType == reflectutil.Primitive ||
		destType == reflectutil.SlicePrimitive

	if expectOneCol && len(r.columns) != 1 {
		return fmt.Errorf("sqlz/scan: expected 1 column, got %d", len(r.columns))
	}

	defer func() {
		if errClose := r.rows.Close(); errClose != nil {
			err = fmt.Errorf("sqlz/scan: closing rows: %w", errClose)
		}
	}()

	switch destType {
	case reflectutil.Primitive:
		err = r.scanPrimitive(dest)

	case reflectutil.SlicePrimitive:
		err = r.scanSlicePrimitive(dest)

	case reflectutil.Map:
		err = r.scanMap(dest)

	case reflectutil.SliceMap:
		err = r.scanSliceMap(dest)

	case reflectutil.Struct:
		err = r.scanStruct(dest)

	case reflectutil.SliceStruct:
		err = r.scanSliceStruct(dest)
	}

	if err != nil {
		return err
	}

	if err := r.postScan(); err != nil {
		return err
	}

	return err
}

// ScanRow is like [sql.Rows.Scan].
func (r *Scanner) ScanRow(dest ...any) error {
	if err := r.rows.Scan(dest...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

func (r *Scanner) scanPrimitive(dest any) error {
	for r.rows.Next() {
		if err := r.ScanRow(dest); err != nil {
			return err
		}
		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSlicePrimitive(dest any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(dest))
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

// ScanMap scans a single row into m. It must be called after [Scanner.NextRow].
func (r *Scanner) ScanMap(m map[string]any) error {
	cb := newColBinding(r.columns)
	if err := r.ScanRow(cb.ptrs...); err != nil {
		return err
	}

	for i, col := range r.columns {
		m[col] = cb.value(i)
	}

	return nil
}

func (r *Scanner) scanMap(dest any) error {
	mapValue := reflectutil.DerefValue(reflect.ValueOf(dest))

	m, ok := mapValue.Interface().(map[string]any)
	if !ok {
		return fmt.Errorf("sqlz/scan: map must be of type map[string]any, got %T", m)
	}

	for r.rows.Next() {
		if err := r.ScanMap(m); err != nil {
			return err
		}
		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceMap(dest any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(dest))

	for r.rows.Next() {
		m := make(map[string]any, len(r.columns))
		if err := r.ScanMap(m); err != nil {
			return err
		}
		sliceValue.Set(reflect.Append(sliceValue, reflect.ValueOf(m)))
		r.rowCount++
	}

	return nil
}

// ScanStruct scans a single row into dest, if dest is not a struct it panics.
// It must be called after [Scanner.NextRow].
func (r *Scanner) ScanStruct(dest any) error {
	stv := reflectutil.NewStructMapper(r.structTag, r.structFieldNameMapper)

	ptrs, err := structPtrs(stv, reflect.ValueOf(dest), r.columns)
	if err != nil {
		return err
	}

	if err := r.ScanRow(ptrs...); err != nil {
		return err
	}

	return nil
}

func (r *Scanner) scanStruct(dest any) error {
	for r.rows.Next() {
		if err := r.ScanStruct(dest); err != nil {
			return err
		}

		r.rowCount++
	}

	return nil
}

func (r *Scanner) scanSliceStruct(dest any) error {
	sliceValue := reflectutil.DerefValue(reflect.ValueOf(dest))

	elType := sliceValue.Type().Elem()
	if isScannable(elType) {
		return r.scanSlicePrimitive(dest)
	}

	stv := reflectutil.NewStructMapper(r.structTag, r.structFieldNameMapper)

	for r.rows.Next() {
		structValue := reflect.New(elType)
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
