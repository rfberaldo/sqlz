package sqlz

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rafaberaldo/sqlz/binds"
	"github.com/rafaberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

var (
	dbMySQL *sql.DB
	dbPGSQL *sql.DB
)

func init() {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	if db, err := sql.Open("mysql", dsn); err == nil {
		if db.Ping() != nil {
			dbMySQL = db
		}
	}

	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
	if db, err := sql.Open("pgx", dsn); err == nil {
		if db.Ping() != nil {
			dbPGSQL = db
		}
	}
}

// run is a helper to run the test on multiple DB.
func run(t *testing.T, fn func(t *testing.T, db *DB)) {
	t.Parallel()
	t.Run("MySQL", func(t *testing.T) {
		t.Parallel()
		if dbMySQL == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
			if os.Getenv("CI") == "true" {
				t.FailNow()
			}
		}
		db, err := New("mysql", dbMySQL)
		if err != nil {
			t.FailNow()
		}
		fn(t, db)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		t.Parallel()
		if dbPGSQL == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
			if os.Getenv("CI") == "true" {
				t.FailNow()
			}
		}
		db, err := New("pgx", dbPGSQL)
		if err != nil {
			t.FailNow()
		}
		fn(t, db)
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

func TestBasicQueryMethods(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		var err error
		var s string
		var ss []string

		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		err = db.Query(&ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = db.QueryRow(&s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)

		tx, err := db.Begin()
		assert.NoError(t, err)
		defer tx.Rollback()

		err = tx.Query(&ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = tx.QueryRow(&s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)
	})
}

func TestContextCancellation(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		query := "SELECT SLEEP(1)"
		if db.bind == binds.Dollar {
			query = "SELECT PG_SLEEP(1)"
		}

		t.Run("exec context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := db.ExecCtx(ctx, query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("exec context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := db.ExecCtx(ctx, query)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := db.QueryCtx(ctx, new([]int), query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := db.QueryCtx(ctx, new([]int), query)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query row context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := db.QueryRowCtx(ctx, new(int), query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query row context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := db.QueryRowCtx(ctx, new(int), query)
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestTxContextCancellation(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		query := "SELECT SLEEP(1)"
		if db.bind == binds.Dollar {
			query = "SELECT PG_SLEEP(1)"
		}

		t.Run("exec context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err = tx.ExecCtx(ctx, query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("exec context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err = tx.ExecCtx(ctx, query)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = tx.QueryCtx(ctx, new([]int), query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err = tx.QueryCtx(ctx, new([]int), query)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query row context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = tx.QueryRowCtx(ctx, new(int), query)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query row context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin()
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err = tx.QueryRowCtx(ctx, new(int), query)
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestTransaction(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		table := testutil.TableName(t.Name())
		t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

		createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			name VARCHAR(255),
			age INT
		)`
		_, err := db.Exec(fmt.Sprintf(createTmpl, table))
		assert.NoError(t, err)

		t.Run("tx should commit", func(t *testing.T) {
			insertTmpl := testutil.Rebind(db.bind, `
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				tx, err := db.Begin()
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.Exec(fmt.Sprintf(insertTmpl, table),
					1, "Alice", 18,
					2, "Rob", 38,
					3, "John", 4,
				)
				assert.NoError(t, err)

				rows, err := re.RowsAffected()
				assert.NoError(t, err)
				assert.Equal(t, 3, int(rows))

				assert.NoError(t, tx.Commit())
			}()

			var count int
			assert.NoError(t, db.QueryRow(&count, "SELECT count(1) FROM "+table))
			assert.Equal(t, 3, count)

			// clean up
			_, err := db.Exec("DELETE FROM " + table)
			assert.NoError(t, err)
		})

		t.Run("tx should rollback using defer", func(t *testing.T) {
			insertTmpl := testutil.Rebind(db.bind, `
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				tx, err := db.Begin()
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.Exec(fmt.Sprintf(insertTmpl, table),
					1, "Alice", 18,
					2, "Rob", 38,
					3, "John", 4,
				)
				assert.NoError(t, err)

				rows, err := re.RowsAffected()
				assert.NoError(t, err)
				assert.Equal(t, 3, int(rows))

				// simulating an error
				err = errors.New("something happened")
				if err != nil {
					return
				}
				assert.NoError(t, tx.Commit())
			}()

			var count int
			assert.NoError(t, db.QueryRow(&count, "SELECT count(1) FROM "+table))
			assert.Equal(t, 0, count)
		})

		t.Run("tx should rollback using context cancel", func(t *testing.T) {
			insertTmpl := testutil.Rebind(db.bind, `
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tx, err := db.BeginTx(ctx, nil)
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.ExecCtx(ctx, fmt.Sprintf(insertTmpl, table),
					1, "Alice", 18,
					2, "Rob", 38,
					3, "John", 4,
				)
				assert.NoError(t, err)

				rows, err := re.RowsAffected()
				assert.NoError(t, err)
				assert.Equal(t, 3, int(rows))

				// simulating an error
				err = errors.New("something happened")
				if err != nil {
					cancel()
				}
				assert.Error(t, tx.Commit(), "commit should error if it was canceled by context")
			}()

			var count int
			assert.NoError(t, db.QueryRow(&count, "SELECT count(1) FROM "+table))
			assert.Equal(t, 0, count)
		})
	})
}

func TestCustomStructTag(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		table := testutil.TableName(t.Name())
		t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

		createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			username VARCHAR(255),
			email VARCHAR(255),
			password VARCHAR(255),
			age INT,
			active BOOL
		)`
		_, err := db.Exec(fmt.Sprintf(createTmpl, table))
		assert.NoError(t, err)

		insertTmpl := testutil.Rebind(db.bind, `
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?)`)
		_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
			1, "Alice", "alice@wonderland.com", "123456", 18, true,
			2, "Rob", "rob@google.com", "123456", 38, true,
			3, "John", "john@id.com", "123456", 24, false,
		)
		assert.NoError(t, err)

		type User struct {
			Identifier int    `json:"id"`
			User       string `json:"username"`
			Email      string
			Pw         string `json:"password"`
			Age        int
			Active     bool
		}

		db.SetStructTag("json")

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		arg := User{Identifier: 1}
		var user User
		err = db.QueryRow(&user, fmt.Sprintf("SELECT * FROM %s WHERE id = :id", table), arg)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}

func TestConn(t *testing.T) {
	run(t, func(t *testing.T, db *DB) {
		assert.IsType(t, &sql.DB{}, db.Conn())
		tx, err := db.Begin()
		assert.NoError(t, err)
		assert.IsType(t, &sql.Tx{}, tx.Conn())
	})
}
