package sqlz

// this file contains shared testing utils.

import (
	"cmp"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rfberaldo/sqlz/internal/parser"
)

// Tests look for `MYSQL_DSN` and `POSTGRES_DSN` environment variables,
// otherwise fallback to these consts.
const (
	MYSQL_DSN    = "root:root@tcp(localhost:3306)/sqlz_test?parseTime=True"
	POSTGRES_DSN = "postgres://postgres:root@localhost:5432/sqlz_test?sslmode=disable"
)

var (
	mysqlConn    *Conn
	postgresConn *Conn
)

type Conn struct {
	name       string
	db         *sql.DB
	bind       parser.Bind
	driverName string
	err        error
}

func init() {
	db, err := sql.Open("mysql", cmp.Or(os.Getenv("MYSQL_DSN"), MYSQL_DSN))
	errPing := db.Ping()
	mysqlConn = &Conn{
		name:       "MySQL",
		driverName: "mysql",
		bind:       parser.BindQuestion,
		db:         db,
		err:        cmp.Or(err, errPing),
	}

	db, err = sql.Open("pgx", cmp.Or(os.Getenv("POSTGRES_DSN"), POSTGRES_DSN))
	errPing = db.Ping()
	postgresConn = &Conn{
		name:       "PostgreSQL",
		driverName: "pgx",
		bind:       parser.BindDollar,
		db:         db,
		err:        cmp.Or(err, errPing),
	}
}

// runConn runs the same code in both MySQL and PostgreSQL.
func runConn(t *testing.T, fn func(t *testing.T, conn *Conn)) {
	if mysqlConn.err != nil && postgresConn.err != nil {
		t.Fatal("no databases connected")
	}

	for _, conn := range []*Conn{mysqlConn, postgresConn} {
		t.Run(conn.name, func(t *testing.T) {
			t.Parallel()
			if conn.err != nil {
				if _, ok := os.LookupEnv("CI"); ok {
					t.Fatalf("%s not available: %s", conn.name, conn.err)
				}
				t.Skipf("%s not available: %s", conn.name, conn.err)
			}
			fn(t, conn)
		})
	}
}

func ptrTo[T any](v T) *T { return &v }

type TableHelper struct {
	tb        testing.TB
	db        *sql.DB
	bind      parser.Bind
	tableName string
}

// newTableHelper returns a [TableHelper] which is a helper for dealing with
// dynamic generated tables, it runs a cleanup func that drops the table.
// db is only used to run cleanup.
func newTableHelper(t testing.TB, db *sql.DB, bind parser.Bind) *TableHelper {
	tableName := strings.SplitN(ToSnakeCase(t.Name()), "/", 2)[0]
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	t.Cleanup(func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	})

	return &TableHelper{t, db, bind, tableName}
}

// fmt replaces '%s' with table name, and transform MySQL query to the targeted driver.
func (t *TableHelper) fmt(query string) string {
	return rebind(t.bind, fmt.Sprintf(query, t.tableName))
}

func rebind(bindTo parser.Bind, query string) string {
	switch bindTo {
	case parser.BindQuestion:
		return query

	case parser.BindDollar:
		return questionToDollar(query)
	}

	panic("Rebind do not support the received bindTo")
}

// questionToDollar replaces all `?` with `$N`.
func questionToDollar(query string) string {
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
