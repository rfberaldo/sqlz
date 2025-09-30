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
	columns      []string
	queryRow     bool
	rowCount     int
	structMapper *reflectutil.StructMapper
	rows         Rows
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
		columns:  columns,
		rows:     rows,
		queryRow: opts.QueryRow,
		structMapper: reflectutil.NewStructMapper(
			opts.StructTag,
			opts.StructFieldNameMapper,
		),
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
func (r *Scanner) Scan(dest any) (err error) {
	if err := r.checkDest(dest); err != nil {
		return err
	}

	destType := reflectutil.TypeOf(dest)

	if destType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: unsupported destination type: %T", dest)
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

	destValue := reflectutil.DerefValue(reflect.ValueOf(dest))
	var elType reflect.Type
	if reflectutil.IsSlice(destValue.Kind()) {
		elType = destValue.Type().Elem()
	}

	for r.rows.Next() {
		var elValue reflect.Value

		switch destType {
		case reflectutil.Primitive:
			err = r.ScanRow(dest)

		case reflectutil.SlicePrimitive:
			elValue = reflect.New(elType)
			err = r.ScanRow(elValue.Interface())

		case reflectutil.Struct:
			if isScannable(destValue.Type()) {
				err = r.ScanRow(dest)
			} else {
				err = r.ScanStruct(dest)
			}

		case reflectutil.SliceStruct:
			elValue = reflect.New(elType)
			if isScannable(elType) {
				err = r.ScanRow(elValue.Interface())
			} else {
				err = r.ScanStruct(elValue.Interface())
			}

		case reflectutil.Map:
			m, errMap := assertMap(dest)
			if errMap != nil {
				return errMap
			}
			err = r.ScanMap(m)

		case reflectutil.SliceMap:
			elValue = reflect.MakeMap(elType)
			m, errMap := assertMap(elValue.Interface())
			if errMap != nil {
				return errMap
			}
			err = r.ScanMap(m)
		}

		if err != nil {
			return err
		}

		switch destType {
		case reflectutil.SlicePrimitive, reflectutil.SliceStruct:
			destValue.Set(reflect.Append(destValue, elValue.Elem()))

		case reflectutil.SliceMap:
			destValue.Set(reflect.Append(destValue, elValue))
		}

		r.rowCount++
	}

	if err != nil {
		return err
	}

	if err := r.postScan(); err != nil {
		return err
	}

	return err
}

// ScanRow copies the columns in the current row into the values pointed at by dest.
// The number of values in dest must be the same as the number of columns.
// Must be called after [Scanner.NextRow]. Refer to [sql.Rows.Scan].
func (r *Scanner) ScanRow(dest ...any) error {
	if err := r.rows.Scan(dest...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

// ScanMap scans a single row into m. Must be called after [Scanner.NextRow].
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

// ScanStruct scans a single row into dest, if dest is not a struct it panics.
// Must be called after [Scanner.NextRow].
func (r *Scanner) ScanStruct(dest any) error {
	if err := r.checkDest(dest); err != nil {
		return err
	}

	ptrs, err := structPtrs(r.structMapper, reflect.ValueOf(dest), r.columns)
	if err != nil {
		return err
	}

	if err := r.ScanRow(ptrs...); err != nil {
		return err
	}

	return nil
}

// Close closes [Scanner], preventing further enumeration, and returning the connection to the pool.
// Close is idempotent and does not affect the result of [Scanner.Err].
func (r *Scanner) Close() error { return r.rows.Close() }

// NextRow prepares the next result row for reading with [Scanner.ScanRow],
// [Scanner.ScanMap] or [Scanner.ScanStruct] methods.
// It returns true on success, or false if there is no next result row or an error
// happened while preparing it. [Scanner.Err] should be consulted to distinguish between
// the two cases.
//
// Every call to [Scanner.ScanRow], [Scanner.ScanMap] or [Scanner.ScanStruct],
// even the first one, must be preceded by a call to [Scanner.NextRow].
func (r *Scanner) NextRow() bool { return r.rows.Next() }

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit [Scanner.Close].
func (r *Scanner) Err() error { return r.rows.Err() }
