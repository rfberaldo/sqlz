package testutil

import (
	"cmp"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode"

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

type MultiConn []*Conn

type Conn struct {
	Name       string
	DB         *sql.DB
	Bind       binds.Bind
	DriverName string
}

func NewMultiConn(t testing.TB) MultiConn {
	var conns []*Conn

	const mysqlDriverName = "mysql"
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), MYSQL_DSN)
	if db, err := NewDB(mysqlDriverName, dsn); err == nil {
		conns = append(conns, &Conn{
			Name:       "MySQL",
			DB:         db,
			Bind:       binds.Question,
			DriverName: mysqlDriverName,
		})
	}

	const postgresDriverName = "pgx"
	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), POSTGRES_DSN)
	if db, err := NewDB(postgresDriverName, dsn); err == nil {
		conns = append(conns, &Conn{
			Name:       "PostgreSQL",
			DB:         db,
			Bind:       binds.Dollar,
			DriverName: postgresDriverName,
		})
	}

	if len(conns) == 0 {
		t.Fatal("no databases connected")
	}

	return conns
}

func (conns MultiConn) Run(t *testing.T, fn func(t *testing.T, conn *Conn)) {
	t.Parallel()
	for _, conn := range conns {
		t.Run(conn.Name, func(t *testing.T) {
			t.Parallel()
			if conn.DB != nil {
				fn(t, conn)
			}

			err := "unable to connect to DB:" + t.Name()
			if os.Getenv("CI") == "true" {
				t.Fatal(err)
			}
			t.Skip(err)
		})
	}
}
