package testutil

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/rfberaldo/sqlz/internal/binds"
)

// PtrTo returns a pointer to the value v.
// Why is this not in the std lib?
func PtrTo[T any](v T) *T { return &v }

// slugify is used to generate table name based on test name, stops on first '/'.
func slugify(name string) string {
	var sb strings.Builder
	sb.Grow(len(name))
	prevUnderscore := false

	for i, r := range name {
		if i == 0 {
			r = unicode.ToLower(r)
		}

		if r == '/' {
			break
		}

		if unicode.IsLower(r) || unicode.IsNumber(r) {
			sb.WriteRune(r)
			prevUnderscore = false
			continue
		}

		if unicode.IsUpper(r) {
			sb.WriteRune('_')
			sb.WriteRune(unicode.ToLower(r))
			prevUnderscore = false
			continue
		}

		if !prevUnderscore {
			sb.WriteRune('_')
			prevUnderscore = true
		}
	}

	return strings.Trim(sb.String(), "_")
}

func rebind(bindTo binds.Bind, query string) string {
	switch bindTo {
	case binds.Question:
		return query

	case binds.Dollar:
		return QuestionToDollar(query)
	}

	panic("Rebind do not support the received bindTo")
}

// DollarToQuestion replaces all `?` with `$N`.
func QuestionToDollar(query string) string {
	count := 0
	var sb strings.Builder
	for _, ch := range query {
		if ch == '?' {
			count++
			sb.WriteByte('$')
			sb.WriteString(strconv.Itoa(count))
			continue
		}
		sb.WriteRune(ch)
	}
	return sb.String()
}

// DollarToAt replaces all `$` with `@`.
func DollarToAt(query string) string {
	return strings.ReplaceAll(query, "$", "@")
}

// PrettyPrint marshal and print arg, only works with exported fields.
func PrettyPrint(arg any) {
	data, err := json.MarshalIndent(arg, "", "  ")
	if err != nil {
		log.Fatalf("could not pretty print: %s\n", err)
	}
	log.Print(string(data))
}

type TableHelper struct {
	tb        testing.TB
	db        *sql.DB
	bind      binds.Bind
	tableName string
}

// NewTableHelper returns a [TableHelper] which is a helper for dealing with
// dynamic generated tables, it runs a cleanup func that drops the table.
// db is only used to run cleanup.
func NewTableHelper(t testing.TB, db *sql.DB, bind binds.Bind) *TableHelper {
	tableName := slugify(t.Name())

	t.Cleanup(func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	return &TableHelper{t, db, bind, tableName}
}

// Fmt replaces '%s' with table name, and transform MySQL query to the targeted driver.
func (t *TableHelper) Fmt(query string) string {
	return rebind(t.bind, fmt.Sprintf(query, t.tableName))
}
