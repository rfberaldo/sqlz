package sqlz

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

const defaultStructTag = "db"

var (
	// scannerType is [reflect.Type] of [sql.Scanner]
	scannerType = reflect.TypeFor[sql.Scanner]()

	// valuerType is [reflect.Type] of [driver.Valuer]
	valuerType = reflect.TypeFor[driver.Valuer]()
)

// Assert validates if arg is a map[string]any.
func assertMap(arg any) (map[string]any, error) {
	m, ok := arg.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("sqlz: map must be of type map[string]any, got %T", arg)
	}
	return m, nil
}

// getMapValue recursively find the map value of a dot notation key string.
func getMapValue(key string, m map[string]any) (any, bool) {
	if !strings.Contains(key, ".") {
		value, ok := m[key]
		return value, ok
	}

	splits := strings.SplitN(key, ".", 2)
	maybeMap, ok := m[splits[0]]
	if !ok {
		return nil, false
	}

	nestedMap, ok := maybeMap.(map[string]any)
	if !ok {
		return nil, false
	}

	return getMapValue(splits[1], nestedMap)
}

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
