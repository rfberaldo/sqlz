package scan

import (
	"reflect"
	"strings"
	"unicode"

	"github.com/rfberaldo/sqlz/internal/reflectutil"
)

type StructBinding struct {
	structTag      string
	fieldKeyMapper func(string) string
}

// FieldByTag recursively finds a field in a struct by tag or name, value should be a struct.
func (sb *StructBinding) FieldByTag(key string, value reflect.Value) (reflect.Value, bool) {
	value = reflectutil.DerefValue(value)
	if !value.IsValid() {
		return reflect.Value{}, false
	}

	for i := range value.NumField() {
		field := value.Type().Field(i)
		fieldValue := value.Field(i)

		if !field.IsExported() {
			continue
		}

		if field.Type.Kind() == reflect.Struct {
			return sb.FieldByTag(key, fieldValue)
		}

		fkey := reflectutil.FieldName(field, sb.structTag)
		if strings.EqualFold(key, fkey) {
			return fieldValue, true
		}
	}

	return reflect.Value{}, false
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
