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

func TestSQLUtil(t *testing.T) {
	t.Run("MySQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
		db, err := Connect("mysql", dsn)
		if err != nil {
			log.Printf("Skipping MySQL tests: %v", err)
			t.Skip()
		}

		checkMethods(t, db)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
		db, err := Connect("pgx", dsn)
		if err != nil {
			log.Printf("Skipping PostgreSQL tests: %v", err)
			t.Skip()
		}

		checkMethods(t, db)
	})
}

// more elaborate tests are done in the internal/core package,
// just have to test if methods are correctly wired.
func checkMethods(t *testing.T, db *sql.DB) {
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
}
