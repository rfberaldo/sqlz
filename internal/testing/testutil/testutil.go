package testutil

import (
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/rafaberaldo/sqlz/internal/parser"
)

// Tests look for `MYSQL_DSN` and `POSTGRES_DSN` environment variables,
// otherwise fallback to these consts.
const (
	MYSQL_DSN    = "root:root@tcp(localhost:3306)/sqlz_test?parseTime=True"
	POSTGRES_DSN = "postgres://postgres:root@localhost:5432/sqlz_test?sslmode=disable"
)

var reDollar = regexp.MustCompile(`\$\d+`)

// DollarToQuestion replaces all `$N` with `?`.
func DollarToQuestion(query string) string {
	return reDollar.ReplaceAllString(query, "?")
}

// DollarToQuestion replaces all `?` with `$N`.
// This replaces all occurrencies of `?`.
func QuestionToDollar(query string) string {
	count := 0
	var sb strings.Builder
	for i := range query {
		ch := query[i]
		if ch == '?' {
			count++
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(count))
			continue
		}
		sb.WriteByte(ch)
	}
	return sb.String()
}

// PtrTo return a pointer to the value v.
// Why is this not in the std lib?
func PtrTo[T any](v T) *T { return &v }

// TableName is a helper to dynamically generate a new table name
// based on the test name.
//
// Example:
//
//	TableName(t.Name())
func TableName(fullName string) string {
	isValid := func(ch byte) bool {
		return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
	}

	var sb strings.Builder
	for i := range fullName {
		ch := fullName[i]

		switch {
		case ch == '/':
			sb.Reset()

		case ch == '.':
			sb.WriteByte('_')

		case isValid(ch):
			sb.WriteByte(ch)
		}
	}

	return strings.ToLower(sb.String())
}

// FuncName is a helper to dynamically get the fn name as snake case.
func FuncName(fn any) string {
	name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	isValid := func(ch byte) bool {
		return 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' || ch == '_'
	}

	isUpperCase := func(ch byte) bool {
		return 'A' <= ch && ch <= 'Z'
	}

	var sb strings.Builder
	for i := range name {
		ch := name[i]

		switch {
		case ch == '/':
			sb.Reset()

		case ch == '.':
			sb.Reset()

		case isUpperCase(ch):
			sb.WriteByte('_')
			sb.WriteString(strings.ToLower(string(ch)))

		case isValid(ch):
			sb.WriteByte(ch)
		}
	}

	return sb.String()
}

// Schema receives a question-bind query and return a rebound query if needed,
// based on bindTo argument.
//
// TODO: add others if needed, currently only Question to Dollar.
func Schema(bindTo parser.Bind, query string) string {
	if bindTo == parser.BindDollar {
		return QuestionToDollar(query)
	}
	return query
}
