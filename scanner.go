package sqlz

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// RowScanner defines the minimal interface for iterating over
// and scanning database query results. It is satisfied by [sql.Rows].
type RowScanner interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...any) error
}

// Scanner wrapps rows and exposes the Scan method that automatically scan rows
// into destination regardless of type. Each instance of Scan must be for a single
// query result.
//
// Scanner also exposes primitive methods for a single row scan: ScanRow,
// ScanMap and ScanStruct, these methods do not loop over or close rows,
// nor can they be mixed.
type Scanner struct {
	columns             []string
	queryRow            bool
	ignoreMissingFields bool
	rows                RowScanner
	structMapper        *reflectutil.StructMapper
	ptrs                []any // slice of pointers for scan, used in all methods
	values              []any // slice of values from rows, used in map scanning
	noop                any   // ignored fields sink
}

type ScannerOptions struct {
	// QueryRow enforces result to be a single row, only used for Scan method.
	QueryRow bool

	// StructTag is the reflection tag that will be used to map struct fields.
	StructTag string

	// FieldNameMapper is a func that maps a struct field name to the database column.
	// It is only used when the struct tag is not found.
	FieldNameMapper func(string) string

	// IgnoreMissingFields makes scan to ignore missing struct fields instead of returning error.
	IgnoreMissingFields bool
}

func NewScanner(rows RowScanner, opts *ScannerOptions) (*Scanner, error) {
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
		structMapper: reflectutil.NewStructMapper(
			opts.StructTag,
			opts.FieldNameMapper,
		),
	}

	if err := scanner.checkDuplicateColumns(); err != nil {
		return nil, err
	}

	return scanner, nil
}

func (s *Scanner) checkDuplicateColumns() error {
	seen := make(map[string]bool, len(s.columns))
	for _, col := range s.columns {
		if _, ok := seen[col]; ok {
			return fmt.Errorf("sqlz/scan: duplicate column name: '%s'", col)
		}
		seen[col] = true
	}
	return nil
}

func (s *Scanner) checkDest(dest any) (reflect.Value, error) {
	v := reflectutil.Deref(reflect.ValueOf(dest))
	if !v.CanSet() {
		return reflect.Value{}, fmt.Errorf("sqlz/scan: destination must be addressable: %T", dest)
	}
	return v, nil
}

// Scan automatically iterates over rows and scans results into dest.
// Scan can only run once, after it is done [sql.Rows] are closed.
func (s *Scanner) Scan(dest any) (err error) {
	destValue, err := s.checkDest(dest)
	if err != nil {
		return err
	}

	destType := reflectutil.TypeOfAny(dest)

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

	isSlice := destValue.Kind() == reflect.Slice

	rowCount := 0
	for s.rows.Next() {
		if isSlice {
			if destValue.Len() == destValue.Cap() {
				destValue.Grow(1)
			}
			destValue.SetLen(destValue.Len() + 1)
		}

		switch destType {
		case reflectutil.Primitive:
			err = s.ScanRow(dest)

		case reflectutil.SlicePrimitive:
			elValue := destValue.Index(destValue.Len() - 1)
			err = s.ScanRow(elValue.Addr().Interface())

		case reflectutil.Struct:
			err = s.ScanStruct(dest)

		case reflectutil.SliceStruct:
			elValue := destValue.Index(destValue.Len() - 1)
			err = s.ScanStruct(elValue.Addr().Interface())

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
			elValue := destValue.Index(destValue.Len() - 1)
			elValue.Set(reflect.MakeMap(mapType))
			m, errMap := assertMap(elValue.Interface())
			if errMap != nil {
				return errMap
			}
			err = s.ScanMap(m)
		}

		if err != nil {
			return err
		}

		rowCount++
	}

	if err != nil {
		return err
	}

	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing rows: %w", err)
	}

	if s.queryRow && rowCount > 1 {
		return fmt.Errorf("sqlz/scan: expected one row, got %d", rowCount)
	}

	if s.queryRow && rowCount == 0 {
		return sql.ErrNoRows
	}

	return err
}

// ScanRow copies the columns in the current row into the values pointed at by dest.
// The number of values in dest must be the same as the number of columns.
// Must be called after [Scanner.NextRow]. Refer to [sql.Rows.Scan].
func (s *Scanner) ScanRow(dest ...any) error {
	s.ptrs = s.ptrs[:0] // empty slice keeping the underlying array
	s.ptrs = append(s.ptrs, dest...)

	if err := s.rows.Scan(s.ptrs...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

// ScanMap scans a single row into m. Must be called after [Scanner.NextRow].
func (s *Scanner) ScanMap(m map[string]any) error {
	s.setMapPtrs()

	if err := s.rows.Scan(s.ptrs...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row into map: %w", err)
	}

	for i, col := range s.columns {
		v := s.values[i]
		if v, ok := v.([]byte); ok {
			m[col] = string(v)
			continue
		}
		m[col] = v
	}

	return nil
}

func (s *Scanner) setMapPtrs() {
	if s.ptrs != nil {
		return
	}

	s.values = make([]any, len(s.columns))
	s.ptrs = make([]any, len(s.columns))

	for i := range s.values {
		s.ptrs[i] = &s.values[i]
	}
}

// ScanStruct scans a single row into dest, if dest is not a struct it panics.
// Must be called after [Scanner.NextRow].
func (s *Scanner) ScanStruct(dest any) error {
	destValue, err := s.checkDest(dest)
	if err != nil {
		return err
	}

	// if dest implements [sql.Scanner], just pass directly to [sql.Rows.Scan].
	if isScannable(destValue.Type()) {
		return s.ScanRow(dest)
	}

	if reflectutil.IsNilStruct(destValue) {
		if !destValue.CanSet() {
			return fmt.Errorf("sqlz/scan: destination is a non addressable nil pointer: %T", dest)
		}
		destValue.Set(reflect.New(destValue.Type().Elem()))
	}

	if err := s.setStructPtrs(destValue); err != nil {
		return err
	}

	if err := s.rows.Scan(s.ptrs...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row into struct: %w", err)
	}

	return nil
}

func (s *Scanner) setStructPtrs(v reflect.Value) error {
	if s.ptrs == nil {
		s.ptrs = make([]any, len(s.columns))
	}

	for i, col := range s.columns {
		fv := s.structMapper.FieldByKey(col, v)
		if !fv.IsValid() {
			if !s.ignoreMissingFields {
				return fmt.Errorf("sqlz/scan: field not found: '%s' (maybe unexported?)", col)
			}
			s.ptrs[i] = &s.noop
			continue
		}
		s.ptrs[i] = fv.Addr().Interface()
	}

	return nil
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
