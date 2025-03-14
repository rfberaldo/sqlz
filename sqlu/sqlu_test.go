package sqlu

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/rafaberaldo/sqlz/binder"
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
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	if db, err := Connect("mysql", dsn); err == nil {
		dbMySQL = db
	}

	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
	if db, err := Connect("pgx", dsn); err == nil {
		dbPGS = db
	}
}

// run is a helper to run the test on multiple DB
func run(t *testing.T, fn func(t *testing.T, db *sql.DB, bind binder.Bind)) {
	t.Parallel()
	t.Run("MySQL", func(t *testing.T) {
		t.Parallel()
		if dbMySQL == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbMySQL, binder.Question)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		t.Parallel()
		if dbPGS == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbPGS, binder.Dollar)
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
	assert.Equal(t, binder.Question, bind())
	SetDefaultBind(binder.Dollar)
	assert.Equal(t, binder.Dollar, bind())
}

// more elaborate tests are done in the internal/core package,
// just testing if methods are correctly wired.
func TestBasicMethods(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binder.Bind) {
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
