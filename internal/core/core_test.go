package core

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/rafaberaldo/sqlz/internal/testing/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestCore(t *testing.T) {
	// tests must be self-contained and able to run in parallel
	type Test func(t *testing.T, db *sql.DB, bind parser.Bind)
	var tests = []Test{
		basicQueryMethods,
		shouldReturnErrorForWrongQuery,
		shouldReturnNotFound,
	}

	t.Run("MySQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Printf("Skipping MySQL tests: %v", err)
			t.Skip()
		}

		for _, fn := range tests {
			t.Run(testutil.FuncName(fn), func(t *testing.T) {
				t.Parallel()
				fn(t, db, parser.BindQuestion)
			})
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Printf("Skipping PostgreSQL tests: %v", err)
			t.Skip()
		}

		for _, fn := range tests {
			t.Run(testutil.FuncName(fn), func(t *testing.T) {
				t.Parallel()
				fn(t, db, parser.BindDollar)
			})
		}
	})
}

func basicQueryMethods(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	var err error
	var s string
	var ss []string

	query := "SELECT 'Hello World'"
	expected := "Hello World"
	expectedSlice := []string{"Hello World"}

	err = Query(ctx, db, bind, &ss, query)
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	err = QueryRow(ctx, db, bind, &s, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)
}

func shouldReturnErrorForWrongQuery(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	var err error
	var dst any
	const query = "WRONG QUERY"
	const str = "WRONG"

	_, err = Exec(ctx, db, bind, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	err = Query(ctx, db, bind, &dst, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	err = QueryRow(ctx, db, bind, &dst, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)
}

func shouldReturnNotFound(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	query := fmt.Sprintf("SELECT * FROM %s", table)

	var dst any
	err = QueryRow(ctx, db, bind, &dst, query)
	assert.Error(t, err)
	assert.Equal(t, true, errors.Is(err, sql.ErrNoRows))
}
