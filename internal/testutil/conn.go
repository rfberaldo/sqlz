package testutil

import (
	"cmp"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
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
	mysqlConn    atomic.Pointer[Conn]
	postgresConn atomic.Pointer[Conn]
)

type Conn struct {
	Name       string
	DB         *sql.DB
	Bind       parser.Bind
	DriverName string
	Err        error
}

func newDB(driverName, dataSourceName string) (*sql.DB, error) {
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

func GetMySQL(t testing.TB) *Conn {
	if conn := mysqlConn.Load(); conn != nil {
		return conn
	}

	const driverName = "mysql"
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), MYSQL_DSN)
	db, err := newDB(driverName, dsn)

	mysqlConn.CompareAndSwap(nil, &Conn{
		Name:       "MySQL",
		DriverName: driverName,
		Bind:       parser.BindQuestion,
		DB:         db,
		Err:        err,
	})

	conn := mysqlConn.Load()
	if conn.Err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			t.Fatal(conn.Err)
		}
	}

	return conn
}

func GetPostgreSQL(t testing.TB) *Conn {
	if conn := postgresConn.Load(); conn != nil {
		return conn
	}

	const driverName = "pgx"
	dsn := cmp.Or(os.Getenv("POSTGRES_DSN"), POSTGRES_DSN)
	db, err := newDB(driverName, dsn)

	postgresConn.CompareAndSwap(nil, &Conn{
		Name:       "PostgreSQL",
		DriverName: driverName,
		Bind:       parser.BindDollar,
		DB:         db,
		Err:        err,
	})

	conn := postgresConn.Load()
	if conn.Err != nil {
		if _, ok := os.LookupEnv("CI"); ok {
			t.Fatal(conn.Err)
		}
	}

	return conn
}

// RunConn runs the same code in both MySQL and PostgreSQL.
func RunConn(t *testing.T, fn func(t *testing.T, conn *Conn)) {
	if GetMySQL(t).Err != nil && GetPostgreSQL(t).Err != nil {
		t.Fatal("no databases connected")
	}

	for _, conn := range []*Conn{GetMySQL(t), GetPostgreSQL(t)} {
		t.Run(conn.Name, func(t *testing.T) {
			t.Parallel()
			if conn.Err != nil {
				t.Skipf("%s not available: %s", conn.Name, conn.Err)
			}
			fn(t, conn)
		})
	}
}
