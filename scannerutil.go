package sqlz

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

var (
	// mapType is the [reflect.Type] of a map[string]any
	mapType = reflect.TypeOf(map[string]any{})
)

type colBinding struct {
	values []any
	ptrs   []any
}

// newColBinding returns a [colBinding], which is a helper for map scanner.
func newColBinding(columns []string) *colBinding {
	cb := &colBinding{
		values: make([]any, len(columns)),
		ptrs:   make([]any, len(columns)),
	}

	for i := range cb.values {
		cb.ptrs[i] = &cb.values[i]
	}

	return cb
}

func (cb *colBinding) value(i int) any {
	v := cb.values[i]
	if v, ok := v.([]byte); ok {
		return string(v)
	}
	return v
}

func SnakeCaseMapper(str string) string {
	var sb strings.Builder
	sb.Grow(len(str) + 2)

	var lastCh rune
	for i, ch := range str {
		isValidLastCh := unicode.IsLower(lastCh) || unicode.IsNumber(lastCh)
		if i > 0 && isValidLastCh && unicode.IsUpper(ch) {
			sb.WriteByte('_')
		}

		sb.WriteRune(unicode.ToLower(ch))
		lastCh = ch
	}

	return sb.String()
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

func isScannable(v reflect.Type) bool {
	return reflect.PointerTo(v).Implements(scannerType) || v.Implements(scannerType)
}

func assertMap(arg any) (map[string]any, error) {
	m, ok := arg.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("sqlz/scan: map must be of type map[string]any, got %T", arg)
	}
	return m, nil
}
