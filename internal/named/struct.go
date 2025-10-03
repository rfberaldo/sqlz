package named

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

// structValues return all the values from arg, following the idents order.
// Returned values can be used in a query if they do not have `IN` clause,
// in other words, values can not be slices.
func (n *Named) structValues(idents []string, structArg any) ([]any, error) {
	v := reflectutil.Deref(reflect.ValueOf(structArg))
	outArgs := make([]any, 0, len(idents))
	sm := reflectutil.NewStructMapper(n.structTag, SnakeCaseMapper)

	var arg any
	for _, ident := range idents {
		arg = nil
		v := sm.FieldByKey(ident, v)
		if !v.IsValid() {
			return nil, fmt.Errorf("sqlz: field not found: '%s' (maybe unexported?)", ident)
		}
		v = reflect.Indirect(v)
		if v.IsValid() && v.CanInterface() {
			arg = v.Interface()
		}
		outArgs = append(outArgs, arg)
	}

	return outArgs, nil
}

// TODO: reuse from main pkg, currently with circular dependency
func SnakeCaseMapper(str string) string {
	var sb strings.Builder
	sb.Grow(len(str))

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
