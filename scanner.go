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
	columns             []string
	queryRow            bool
	rowCount            int
	ignoreMissingFields bool
	rows                Rows
	mapBinding          *mapBinding
	structMapper        *reflectutil.StructMapper
}

type ScannerOptions struct {
	// QueryRow enforces result to be a single row.
	QueryRow bool

	// StructTag is the reflection tag that will be used to map fields.
	StructTag string

	// FieldNameMapper is a func that maps a struct field name to the database column.
	// It is only used when the struct tag is not found.
	FieldNameMapper func(string) string

	// IgnoreMissingFields makes struct scan to ignore missing fields instead of returning error.
	IgnoreMissingFields bool
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

	if opts.FieldNameMapper == nil {
		opts.FieldNameMapper = SnakeCaseMapper
	}

	scanner := &Scanner{
		columns:             columns,
		rows:                rows,
		queryRow:            opts.QueryRow,
		ignoreMissingFields: opts.IgnoreMissingFields,
		mapBinding:          newMapBinding(len(columns)),
		structMapper: reflectutil.NewStructMapper(
			opts.StructTag,
			opts.FieldNameMapper,
		),
	}

	return scanner, nil
}

func (s *Scanner) postScan() error {
	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if s.queryRow && s.rowCount > 1 {
		return fmt.Errorf("sqlz/scan: expected one row, got %d", s.rowCount)
	}

	if s.queryRow && s.rowCount == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (s *Scanner) checkDest(dest any) (reflect.Value, error) {
	v := reflectutil.DerefValue(reflect.ValueOf(dest))
	if !v.CanSet() {
		return reflect.Value{}, fmt.Errorf("sqlz/scan: destination must be addressable: %T", dest)
	}
	return v, nil
}

// Scan automatically iterates over rows and scans results into dest.
// Scan can only run once, after it is done [sql.Rows] are closed.
func (s *Scanner) Scan(dest any) (err error) {
	if s.rowCount > 0 {
		panic("sqlz/scan: Scan already done or in progress")
	}

	destValue, err := s.checkDest(dest)
	if err != nil {
		return err
	}

	destType := reflectutil.TypeOf(dest)

	if destType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: unsupported destination type: %T", dest)
	}

	expectOneCol := destType == reflectutil.Primitive ||
		destType == reflectutil.SlicePrimitive

	if expectOneCol && len(s.columns) != 1 {
		return fmt.Errorf("sqlz/scan: expected 1 column, got %d", len(s.columns))
	}

	defer func() {
		if errClose := s.rows.Close(); errClose != nil {
			err = fmt.Errorf("sqlz/scan: closing rows: %w", errClose)
		}
	}()

	isSlice := reflectutil.IsSlice(destValue.Kind())
	var elType reflect.Type
	if isSlice {
		elType = destValue.Type().Elem()
	}

	for s.rows.Next() {
		var elValue reflect.Value

		switch destType {
		case reflectutil.Primitive:
			err = s.ScanRow(dest)

		case reflectutil.SlicePrimitive:
			elValue = reflect.New(elType)
			err = s.ScanRow(elValue.Interface())

		case reflectutil.Struct:
			if isScannable(destValue.Type()) {
				err = s.ScanRow(dest)
			} else {
				err = s.ScanStruct(dest)
			}

		case reflectutil.SliceStruct:
			elValue = reflect.New(elType)
			if isScannable(elType) {
				err = s.ScanRow(elValue.Interface())
			} else {
				err = s.ScanStruct(elValue.Interface())
			}

		case reflectutil.Map:
			if reflectutil.IsNilMap(destValue) {
				destValue.Set(reflect.MakeMap(mapType))
			}
			m, errMap := assertMap(destValue.Interface())
			if errMap != nil {
				return errMap
			}
			err = s.ScanMap(m)

		case reflectutil.SliceMap:
			mapValue := reflect.MakeMap(mapType)
			m, errMap := assertMap(mapValue.Interface())
			if errMap != nil {
				return errMap
			}
			elValue = reflect.New(mapType) // pointer to map
			elValue.Elem().Set(mapValue)   // point to mapValue
			err = s.ScanMap(m)
		}

		if err != nil {
			return err
		}

		if isSlice {
			destValue.Set(reflect.Append(destValue, elValue.Elem()))
		}

		s.rowCount++
	}

	if err != nil {
		return err
	}

	if err := s.postScan(); err != nil {
		return err
	}

	return err
}

// ScanRow copies the columns in the current row into the values pointed at by dest.
// The number of values in dest must be the same as the number of columns.
// Must be called after [Scanner.NextRow]. Refer to [sql.Rows.Scan].
func (s *Scanner) ScanRow(dest ...any) error {
	if err := s.rows.Scan(dest...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

// ScanMap scans a single row into m. Must be called after [Scanner.NextRow].
func (s *Scanner) ScanMap(m map[string]any) error {
	// cb := newMapBinding(s.columns)
	if err := s.ScanRow(s.mapBinding.ptrs...); err != nil {
		return err
	}

	for i, col := range s.columns {
		m[col] = s.mapBinding.value(i)
	}

	return nil
}

// ScanStruct scans a single row into dest, if dest is not a struct it panics.
// Must be called after [Scanner.NextRow].
func (s *Scanner) ScanStruct(dest any) error {
	destValue, err := s.checkDest(dest)
	if err != nil {
		return err
	}

	if reflectutil.IsNilStruct(destValue) {
		if !destValue.CanSet() {
			return fmt.Errorf("sqlz/scan: destination is a non addressable nil pointer: %T", dest)
		}
		destValue.Set(reflect.New(destValue.Type().Elem()))
	}

	ptrs, err := s.structPtrs(destValue)
	if err != nil {
		return err
	}

	if err := s.ScanRow(ptrs...); err != nil {
		return err
	}

	return nil
}

func (s *Scanner) structPtrs(v reflect.Value) ([]any, error) {
	ptrs := make([]any, len(s.columns))

	for i, col := range s.columns {
		fv := s.structMapper.FieldByTagName(col, v)
		if !fv.IsValid() {
			if !s.ignoreMissingFields {
				return nil, fmt.Errorf("sqlz/scan: field not found: %s", col)
			}
			var tmp any
			fv = reflect.ValueOf(&tmp).Elem()
		}
		ptrs[i] = fv.Addr().Interface()
	}

	return ptrs, nil
}

// Close closes [Scanner], preventing further enumeration, and returning the connection to the pool.
// Close is idempotent and does not affect the result of [Scanner.Err].
func (s *Scanner) Close() error { return s.rows.Close() }

// NextRow prepares the next result row for reading with [Scanner.ScanRow],
// [Scanner.ScanMap] or [Scanner.ScanStruct] methods.
// It returns true on success, or false if there is no next result row or an error
// happened while preparing it. [Scanner.Err] should be consulted to distinguish between
// the two cases.
//
// Every call to [Scanner.ScanRow], [Scanner.ScanMap] or [Scanner.ScanStruct],
// even the first one, must be preceded by a call to [Scanner.NextRow].
func (s *Scanner) NextRow() bool { return s.rows.Next() }

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit [Scanner.Close].
func (s *Scanner) Err() error { return s.rows.Err() }
