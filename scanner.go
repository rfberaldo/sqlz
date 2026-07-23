package sqlz

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// cursor is satisfied by [sql.Rows].
type cursor interface {
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
	rows cursor

	columns         []string
	queryRow        bool
	destType        reflectutil.Type
	fieldIndexByKey map[string][]int
	ptrs            []any // slice of pointers for scan, used in all methods
	values          []any // slice of values from rows, used in map scanning
	noop            any   // ignored fields sink
}

func newScanner(rows cursor, cfg *config) *Scanner {
	return &Scanner{
		config: applyDefaults(cfg),
		rows:   rows,
	}
}

func newRowScanner(rows cursor, cfg *config) *Scanner {
	return &Scanner{
		config:   applyDefaults(cfg),
		rows:     rows,
		queryRow: true,
	}
}

// Err returns the deferred query error, if any. Useful for wrappers.
func (s *Scanner) Err() error {
	return s.err
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

	if err := s.resolveColumns(); err != nil {
		return err
	}

	if err := s.resolveDestType(dest); err != nil {
		return err
	}

	if !s.queryRow && !s.destType.IsSlice() {
		return fmt.Errorf("sqlz/scan: destination must be a slice to scan multiple rows, got %T", dest)
	}

	return s.scanAll(dest)
}

type ScanFunc func(arg any) error

func (s *Scanner) ForEach(fn func(scan ScanFunc) error) error {
	if s.err != nil {
		return s.err
	}

	if err := s.resolveColumns(); err != nil {
		return err
	}

	defer s.rows.Close()
	for s.rows.Next() {
		if err := fn(s.scanOne); err != nil {
			return err
		}
	}

	if err := s.rows.Err(); err != nil {
		return fmt.Errorf("sqlz/scan: preparing next row: %w", err)
	}

	return nil
}

func (s *Scanner) scanAll(dest any) error {
	defer s.rows.Close()

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

	return nil
}

func (s *Scanner) scanOne(dest any) (err error) {
	if err := s.resolveDestType(dest); err != nil {
		return err
	}

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

func (s *Scanner) scanStruct(dest any) error {
	destValue := reflectutil.Init(reflect.ValueOf(dest))

	// if it implements [sql.Scanner], just scan it natively
	if reflectutil.ImplementsScanner(destValue.Type()) {
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

func (s *Scanner) setStructPtrs(destValue reflect.Value) error {
	if s.ptrs == nil {
		s.ptrs = make([]any, len(s.columns))
	}

	if s.fieldIndexByKey == nil {
		s.fieldIndexByKey = reflectutil.StructFieldMap(
			destValue.Type(), s.structTag, "_", s.fieldNameTransformer,
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

		fv := reflectutil.FieldByIndex(destValue, index)
		if !fv.IsValid() {
			return fmt.Errorf("sqlz/scan: invalid struct field: '%s'", col)
		}
		s.ptrs[i] = fv.Addr().Interface()
	}

	return nil
}
