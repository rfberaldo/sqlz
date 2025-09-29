package scan

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
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

func structPtrs(stv *reflectutil.StructValue, v reflect.Value, columns []string) ([]any, error) {
	ptrs := make([]any, len(columns))

	for i, col := range columns {
		fv := stv.FieldByTagName(col, v)
		if !fv.IsValid() {
			return nil, fmt.Errorf("sqlz/scan: %q not found", col)
		}
		ptrs[i] = fv.Addr().Interface()
	}

	return ptrs, nil
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
