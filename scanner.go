package sqlz

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// rows defines the minimal interface for iterating over
// and scanning database query results. It is satisfied by [sql.Rows].
type rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(dest ...any) error
}

// Scanner is the result of calling [DB.Query] or [DB.QueryRow].
type Scanner struct {
	*config

	// one of these two will be non-nil:
	err  error // deferred error
	rows rows

	manualIterating bool
	columns         []string
	queryRow        bool
	destType        reflectutil.Type
	fieldIndexByKey map[string][]int
	ptrs            []any // slice of pointers for scan, used in all methods
	values          []any // slice of values from rows, used in map scanning
	noop            any   // ignored fields sink
}

func newScanner(rows rows, cfg *config) *Scanner {
	return &Scanner{
		config: applyDefaults(cfg),
		rows:   rows,
	}
}

func newRowScanner(rows rows, cfg *config) *Scanner {
	return &Scanner{
		config:   applyDefaults(cfg),
		rows:     rows,
		queryRow: true,
	}
}

func (s *Scanner) resolveColumns() (err error) {
	if s.columns != nil {
		return nil
	}

	s.columns, err = s.rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlz/scan: getting column names: %w", err)
	}

	if len(s.columns) == 0 {
		return fmt.Errorf("sqlz/scan: no columns in result set")
	}

	seen := make(map[string]bool, len(s.columns))
	for _, col := range s.columns {
		if _, ok := seen[col]; ok {
			return fmt.Errorf("sqlz/scan: duplicate column name: '%s'", col)
		}
		seen[col] = true
	}
	return nil
}

func (s *Scanner) resolveDestType(dest any) error {
	if s.destType != reflectutil.Invalid {
		return nil
	}

	s.destType = reflectutil.TypeOfAny(dest)

	if s.destType == reflectutil.Invalid {
		return fmt.Errorf("sqlz/scan: unsupported destination type: %T", dest)
	}

	if !s.manualIterating && !s.queryRow && !s.destType.IsSlice() {
		return fmt.Errorf("sqlz/scan: destination must be a slice to scan multiple rows, got %T", dest)
	}

	if s.destType.IsPrimitive() && len(s.columns) != 1 {
		return fmt.Errorf(
			"sqlz/scan: query must return 1 column to scan into a primitive type, got %d",
			len(s.columns),
		)
	}

	return nil
}

// Scan automatically iterates over rows and scans into dest regardless of type.
// Scan should not be called more than once per [Scanner] instance.
func (s *Scanner) Scan(dest any) (err error) {
	if s.err != nil {
		return s.err
	}

	if s.manualIterating {
		panic("sqlz/scan: Scan cannot be used with manual iteration, use ScanRow instead")
	}

	if err := s.resolveColumns(); err != nil {
		return err
	}

	if err := s.resolveDestType(dest); err != nil {
		return err
	}

	return s.scanAll(dest)
}

// ScanRow scans the current row into dest regardless of type,
// it must be called inside a [NextRow] loop.
func (s *Scanner) ScanRow(dest any) (err error) {
	if s.err != nil {
		return s.err
	}

	if !s.manualIterating {
		panic("sqlz/scan: ScanRow can only be used with manual iteration, use Scan for automatic iteration")
	}

	if err := s.resolveColumns(); err != nil {
		return err
	}

	if err := s.resolveDestType(dest); err != nil {
		return err
	}

	return s.scanOne(dest)
}

func (s *Scanner) scanAll(dest any) (err error) {
	defer func() {
		if errClose := s.rows.Close(); errClose != nil {
			err = fmt.Errorf("sqlz/scan: closing rows: %w", errClose)
		}
	}()

	rowCount := 0
	for s.rows.Next() {
		if err := s.scanOne(dest); err != nil {
			return err
		}
		rowCount++

		if s.queryRow && rowCount > 1 {
			return fmt.Errorf("sqlz/scan: expected one row, got more")
		}
	}

	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing next row: %w", err)
	}

	if s.queryRow && rowCount == 0 {
		return sql.ErrNoRows
	}

	return err
}

func (s *Scanner) scanOne(dest any) (err error) {
	destValue := reflectutil.Init(reflect.ValueOf(dest))
	if !destValue.CanSet() {
		return fmt.Errorf("sqlz/scan: destination must be addressable: %T", dest)
	}

	if s.destType.IsSlice() {
		if destValue.Len() == destValue.Cap() {
			destValue.Grow(1)
		}
		destValue.SetLen(destValue.Len() + 1)
	}

	switch s.destType {
	case reflectutil.Primitive:
		return s.scan(dest)

	case reflectutil.SlicePrimitive:
		elValue := destValue.Index(destValue.Len() - 1)
		return s.scan(elValue.Addr().Interface())

	case reflectutil.Struct:
		return s.scanStruct(dest)

	case reflectutil.SliceStruct:
		elValue := destValue.Index(destValue.Len() - 1)
		return s.scanStruct(elValue.Addr().Interface())

	case reflectutil.Map:
		return s.scanMap(destValue.Interface())

	case reflectutil.SliceMap:
		elValue := destValue.Index(destValue.Len() - 1)
		elValue = reflectutil.Init(elValue)
		return s.scanMap(elValue.Interface())
	}

	panic("sqlz/scan: type not handled, got " + destValue.Type().String())
}

func (s *Scanner) scan(dest ...any) error {
	s.ptrs = s.ptrs[:0] // empty slice keeping the underlying array
	s.ptrs = append(s.ptrs, dest...)

	if err := s.rows.Scan(s.ptrs...); err != nil {
		return fmt.Errorf("sqlz/scan: scanning row: %w", err)
	}

	return nil
}

func (s *Scanner) scanMap(dest any) error {
	m, errMap := assertMap(dest)
	if errMap != nil {
		return errMap
	}

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

func isScannable(t reflect.Type) bool {
	return reflect.PointerTo(t).Implements(scannerType) || t.Implements(scannerType)
}

func (s *Scanner) scanStruct(dest any) error {
	destValue := reflectutil.Init(reflect.ValueOf(dest))

	// if implements [sql.Scanner], just scan it natively
	if isScannable(destValue.Type()) {
		return s.scan(dest)
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

	if s.fieldIndexByKey == nil {
		s.fieldIndexByKey = reflectutil.StructFieldMap(
			v.Type(), s.structTag, "_", s.fieldNameTransformer,
		)
	}

	for i, col := range s.columns {
		index, ok := s.fieldIndexByKey[col]
		if !ok {
			if !s.ignoreMissingFields {
				return fmt.Errorf("sqlz/scan: struct field not found: '%s' (maybe unexported?)", col)
			}
			s.ptrs[i] = &s.noop
			continue
		}

		fv := reflectutil.FieldByIndex(v, index)
		if !fv.IsValid() {
			return fmt.Errorf("sqlz/scan: invalid struct field: '%s'", col)
		}
		s.ptrs[i] = fv.Addr().Interface()
	}

	return nil
}

// Close closes [Scanner], preventing further enumeration, and returning the connection to the pool.
// Close is idempotent and does not affect the result of [Scanner.Err].
func (s *Scanner) Close() error {
	if s.rows == nil {
		return nil
	}
	if err := s.rows.Close(); err != nil {
		return fmt.Errorf("sqlz/scan: closing rows: %w", err)
	}
	return nil
}

// NextRow prepares the next result row for reading with [Scanner.ScanRow].
// It returns true on success, or false if there is no next result row or an error
// happened while preparing it. [Scanner.Err] should be consulted to distinguish between
// the two cases.
//
// Every call to [Scanner.ScanRow], even the first one, must be preceded by a NextRow.
func (s *Scanner) NextRow() bool {
	if s.rows == nil {
		return false
	}
	s.manualIterating = true
	return s.rows.Next()
}

// Err returns the error, if any, that was encountered while running the query
// or during iteration.
// Err may be called after an explicit or implicit [Scanner.Close].
func (s *Scanner) Err() error {
	if s.err != nil {
		return s.err
	}
	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing next row: %w", err)
	}
	return nil
}
