package sqlu

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/rafaberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	dbMySQL *sql.DB
	dbPGS   *sql.DB
)

func init() {
	setupMySQL()
	setupPostgreSQL()
}

func setupMySQL() {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := Connect("mysql", dsn)
	if err != nil {
		log.Printf("Skipping MySQL tests: %v", err)
		return
	}
	dbMySQL = db
}

func setupPostgreSQL() {
	dsn := cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
	db, err := Connect("pgx", dsn)
	if err != nil {
		log.Printf("Skipping PostgreSQL tests: %v", err)
		return
	}
	dbPGS = db
}

// run is a helper to run the test on multiple DB
func run(t *testing.T, fn func(t *testing.T, db *sql.DB, bind parser.Bind)) {
	t.Parallel()
	t.Run("MySQL", func(t *testing.T) {
		t.Parallel()
		if dbMySQL == nil {
			t.SkipNow()
		}
		fn(t, dbMySQL, parser.BindQuestion)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		t.Parallel()
		if dbPGS == nil {
			t.SkipNow()
		}
		fn(t, dbPGS, parser.BindDollar)
	})
}

func TestNotFound(t *testing.T) {
	err := errors.New("some custom error")
	assert.Equal(t, false, IsNotFound(err))

	err = fmt.Errorf("some custom error")
	assert.Equal(t, false, IsNotFound(err))

	err = errors.Join(fmt.Errorf("some custom error"), sql.ErrNoRows)
	assert.Equal(t, true, IsNotFound(err))

	err = fmt.Errorf("a wrapper around sql.ErrNoRows: %w", sql.ErrNoRows)
	assert.Equal(t, true, IsNotFound(err))
}

func TestSetDefaultBind(t *testing.T) {
	assert.Equal(t, parser.BindQuestion, bind())
	SetDefaultBind(BindDollar)
	assert.Equal(t, parser.BindDollar, bind())
}

// more elaborate tests are done in the internal/core package,
// just testing if methods are correctly wired.
func TestBasicMethods(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind parser.Bind) {
		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		ss, err := Query[string](db, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		s, err := QueryRow[string](db, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)

		_, err = Exec(db, query)
		assert.NoError(t, err)
	})
}
