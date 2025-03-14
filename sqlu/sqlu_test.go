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

// run is a helper to run the test on multiple DB.
// Can't run in parallel because bind is package-level.
func run(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	SetDefaultBind(binder.Question)
	t.Run("MySQL", func(t *testing.T) {
		if dbMySQL == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbMySQL)
	})

	SetDefaultBind(binder.Dollar)
	t.Run("PostgreSQL", func(t *testing.T) {
		if dbPGS == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbPGS)
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
	SetDefaultBind(binder.Dollar)
	assert.Equal(t, binder.Dollar, bind())

	SetDefaultBind(binder.Question)
	assert.Equal(t, binder.Question, bind())
}

func TestBasicMethods(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB) {
		query := testutil.Rebind(bind(), "SELECT 'Hello World' WHERE 1 = ?")
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		ss, err := Query[string](db, query, 1)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		s, err := QueryRow[string](db, query, 1)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)

		_, err = Exec(db, query, 1)
		assert.NoError(t, err)
	})
}
