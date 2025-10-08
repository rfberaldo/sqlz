package sqlz

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"
)

const defaultStructTag = "db"

var (
	// scannerType is [reflect.Type] of [sql.Scanner]
	scannerType = reflect.TypeFor[sql.Scanner]()

	// valuerType is [reflect.Type] of [driver.Valuer]
	valuerType = reflect.TypeFor[driver.Valuer]()
)

// assertMap validates if arg is a map[string]any.
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

// IsNotFound is a helper to check if err contains [sql.ErrNoRows].
func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// ToSnakeCase transforms a string to snake case.
func ToSnakeCase(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 4)

	position := 0

	read := func() (rune, bool) {
		if position >= len(s) {
			return 0, false
		}

		r, size := utf8.DecodeRuneInString(s[position:])
		position += size
		return r, true
	}

	peek := func() rune {
		r, _ := utf8.DecodeRuneInString(s[position:])
		return r
	}

	var prev rune
	for {
		r, ok := read()
		if !ok {
			break
		}

		if prev != 0 && prev != '_' && unicode.IsUpper(r) {
			if unicode.IsLower(prev) || unicode.IsNumber(prev) || unicode.IsLower(peek()) {
				sb.WriteRune('_')
			}
		}

		prev = r
		sb.WriteRune(unicode.ToLower(r))
	}

	return sb.String()
}
