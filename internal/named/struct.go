package named

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// structValues return all the values from arg, following the idents order.
// Returned values can be used in a query if they do not have `IN` clause,
// in other words, values can not be slices.
func (n *Named) structValues(idents []string, arg any) ([]any, error) {
	outArgs := make([]any, 0, len(idents))
	for _, ident := range idents {
		n.cacheLastFullKey = ident // save this to cache if value is found
		value, ok := n.structValue(ident, arg)
		if !ok {
			return nil, fmt.Errorf("sqlz: could not find name `%s` in %+v", ident, arg)
		}
		outArgs = append(outArgs, value)
	}
	return outArgs, nil
}

// structValue recursively finds the value of a dot notation key string in a struct.
func (n *Named) structValue(key string, arg any) (any, bool) {
	argValue := reflect.ValueOf(arg)

	if argValue.Kind() == reflect.Ptr {
		argValue = argValue.Elem()
	}

	if argValue.Kind() != reflect.Struct {
		return nil, false
	}

	if !strings.Contains(key, ".") {
		fieldValue, ok := n.findStructValue(key, argValue.Interface())
		if !ok {
			return nil, false
		}
		if fieldValue.Kind() == reflect.Ptr {
			if fieldValue.IsNil() {
				return nil, true
			}
			fieldValue = fieldValue.Elem()
		}
		if !fieldValue.IsValid() {
			return nil, false
		}
		return fieldValue.Interface(), true
	}

	splits := strings.SplitN(key, ".", 2)
	fieldValue, ok := n.findStructValue(splits[0], argValue.Interface())
	if !ok {
		return nil, false
	}

	return n.structValue(splits[1], fieldValue.Interface())
}

// findStructValue finds a field in a struct by key, prioritizing the `db` tag
func (n *Named) findStructValue(key string, arg any) (reflect.Value, bool) {
	argValue := reflect.ValueOf(arg)

	if i, ok := n.getCachedIndexByKey(key); ok {
		return argValue.Field(i), true
	}

	t := argValue.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		fieldValue := argValue.Field(i)

		if !field.IsExported() {
			continue
		}

		if strings.EqualFold(key, fieldKey(field, n.structTag)) {
			n.cacheIndexByKey(key, i)
			return fieldValue, true
		}
	}

	return reflect.Value{}, false
}

// fieldKey extracts the key name for a struct field, prioritizing tag arg.
func fieldKey(field reflect.StructField, tag string) string {
	dbTag := field.Tag.Get(tag)

	if dbTag != "-" && dbTag != "" {
		// check for possible comma as in "...,omitempty"
		commaIdx := strings.Index(dbTag, ",")

		if commaIdx == -1 {
			return dbTag
		}

		return dbTag[:commaIdx]
	}

	return SnakeCaseMapper(field.Name)
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
