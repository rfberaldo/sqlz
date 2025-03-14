package testutil

import (
	"math/rand"
	"strconv"
	"strings"

	"github.com/rafaberaldo/sqlz/binder"
)

// Tests look for `MYSQL_DSN` and `POSTGRES_DSN` environment variables,
// otherwise fallback to these consts.
const (
	MYSQL_DSN    = "root:root@tcp(localhost:3306)/sqlz_test?parseTime=True"
	POSTGRES_DSN = "postgres://postgres:root@localhost:5432/sqlz_test?sslmode=disable"
)

// PtrTo returns a pointer to the value v.
// Why is this not in the std lib?
func PtrTo[T any](v T) *T { return &v }

// TableName dynamically generate a new table name based on the test name.
// Stops at first slash, then appends a random 3-char string at the end.
//
// Example:
//
//	table := TableName(t.Name())
func TableName(name string) string {
	isValid := func(ch byte) bool {
		return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
	}

	var sb strings.Builder
	sb.Grow(len(name) + 2)

nameLoop:
	for i := range name {
		ch := name[i]

		switch {
		case ch == '/':
			break nameLoop

		case isValid(ch):
			sb.WriteByte(ch)
		}
	}

	sb.WriteByte('_')
	sb.Write(randStr(3))

	return sb.String()
}

func randStr(length int) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range length {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

// Rebind receives a question-bind query and return a rebound query if needed,
// based on bindTo argument.
func Rebind(bindTo binder.Bind, query string) string {
	switch bindTo {
	case binder.Question:
		return query

	case binder.Dollar:
		return QuestionToDollar(query)
	}

	panic("Rebind do not support the received bindTo")
}

// DollarToQuestion replaces all `?` with `$N`.
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

// DollarToAt replaces all `$` with `@`.
func DollarToAt(query string) string {
	return strings.ReplaceAll(query, "$", "@")
}
