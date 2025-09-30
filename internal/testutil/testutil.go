package testutil

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rfberaldo/sqlz/internal/binds"
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
func Rebind(bindTo binds.Bind, query string) string {
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
	tableName string
}

func NewTableHelper(t testing.TB) *TableHelper {
	return &TableHelper{t, TableName(t.Name())}
}

// Cleanup drops table with [testing.TB.Cleanup].
func (t *TableHelper) Cleanup(db *sql.DB) {
	t.tb.Cleanup(func() {
		db.Exec(t.Fmt("DROP TABLE IF EXISTS %s"))
	})
}

// Fmt replaces '%s' with table name.
func (t *TableHelper) Fmt(query string) string {
	return fmt.Sprintf(query, t.tableName)
}

// FmtRebind replaces '%s' with table name then run through [Rebind].
func (t *TableHelper) FmtRebind(bindTo binds.Bind, query string) string {
	return Rebind(bindTo, t.Fmt(query))
}

func NewDB(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("error connecting to %v: %w", driverName, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging to %v: %w", driverName, err)
	}

	return db, nil
}

type MultiDB struct {
	dbByName map[string]*sql.DB
}

func NewMultiDB(t testing.TB) *MultiDB {
	mdb := &MultiDB{dbByName: make(map[string]*sql.DB)}

	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), MYSQL_DSN)
	if db, err := NewDB("mysql", dsn); err == nil {
		mdb.dbByName["MySQL"] = db
	}

	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), POSTGRES_DSN)
	if db, err := NewDB("pgx", dsn); err == nil {
		mdb.dbByName["PostgreSQL"] = db
	}

	if len(mdb.dbByName) == 0 {
		t.Fatal("no databases connected")
	}

	return mdb
}

func (m *MultiDB) Run(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	t.Parallel()
	for name, db := range m.dbByName {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if db != nil {
				fn(t, db)
			}

			err := "unable to connect to DB:" + t.Name()
			if os.Getenv("CI") == "true" {
				t.Fatal(err)
			}
			t.Skip(err)
		})
	}
}
