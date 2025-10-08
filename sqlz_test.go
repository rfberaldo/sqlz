package sqlz_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/rfberaldo/sqlz"
	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/rfberaldo/sqlz/parser"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestNew(t *testing.T) {
	db := sqlz.New("sqlite3", &sql.DB{}, nil)
	assert.NotNil(t, db)
	assert.IsType(t, &sqlz.DB{}, db)
}

func TestNew_panic(t *testing.T) {
	defer func() {
		err, ok := recover().(error)
		assert.True(t, ok)
		assert.ErrorContains(t, err, "unable to find bind")
	}()

	sqlz.New("wrongdriver", &sql.DB{}, nil)
}

func TestConnect_wrong_driver(t *testing.T) {
	_, err := sqlz.Connect("wrongdriver", ":memory:")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unknown driver")
}

func TestDB_basic(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, nil)
		var err error
		var s string
		var ss []string

		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		err = db.Select(ctx, &ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = db.Get(ctx, &s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)

		tx, err := db.Begin(ctx)
		assert.NoError(t, err)
		defer tx.Rollback()

		ss = ss[:0] // clear slice
		err = tx.Select(ctx, &ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = tx.Get(ctx, &s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)
	})
}

func TestContextCancellation(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, nil)
		q := "SELECT SLEEP(1)"
		if conn.Bind == parser.BindDollar {
			q = "SELECT PG_SLEEP(1)"
		}

		t.Run("exec context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := db.Exec(ctx, q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("exec context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := db.Exec(ctx, q)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := db.Select(ctx, new([]int), q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := db.Select(ctx, new([]int), q)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query row context should timeout", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err := db.Get(ctx, new(int), q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query row context should cancel", func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := db.Get(ctx, new(int), q)
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestTxContextCancellation(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, nil)
		q := "SELECT SLEEP(1)"
		if conn.Bind == parser.BindDollar {
			q = "SELECT PG_SLEEP(1)"
		}

		t.Run("exec context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err = tx.Exec(ctx, q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("exec context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err = tx.Exec(ctx, q)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = tx.Select(ctx, new([]int), q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err = tx.Select(ctx, new([]int), q)
			assert.ErrorIs(t, err, context.Canceled)
		})

		t.Run("query row context should timeout", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			err = tx.Get(ctx, new(int), q)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})

		t.Run("query row context should cancel", func(t *testing.T) {
			t.Parallel()
			tx, err := db.Begin(ctx)
			assert.NoError(t, err)
			defer tx.Rollback()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err = tx.Get(ctx, new(int), q)
			assert.ErrorIs(t, err, context.Canceled)
		})
	})
}

func TestTransaction(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, nil)
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := db.Exec(ctx, th.Fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				name VARCHAR(255),
				age INT
			)`,
		))
		assert.NoError(t, err)

		t.Run("tx should commit", func(t *testing.T) {
			q := th.Fmt(`INSERT INTO %s (id, name, age) VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				tx, err := db.Begin(ctx)
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.Exec(ctx, q,
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
			assert.NoError(t, db.Get(ctx, &count, th.Fmt("SELECT count(1) FROM %s")))
			assert.Equal(t, 3, count)

			// clean up
			_, err := db.Exec(ctx, th.Fmt("DELETE FROM %s"))
			assert.NoError(t, err)
		})

		t.Run("tx should rollback using defer", func(t *testing.T) {
			q := th.Fmt(`INSERT INTO %s (id, name, age) VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				tx, err := db.Begin(ctx)
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.Exec(ctx, q,
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
			assert.NoError(t, db.Get(ctx, &count, th.Fmt("SELECT count(1) FROM %s")))
			assert.Equal(t, 0, count)
		})

		t.Run("tx should rollback using context cancel", func(t *testing.T) {
			q := th.Fmt(`INSERT INTO %s (id, name, age) VALUES (?,?,?),(?,?,?),(?,?,?)`)

			func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tx, err := db.BeginTx(ctx, nil)
				assert.NoError(t, err)
				defer tx.Rollback()

				re, err := tx.Exec(ctx, q,
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
			assert.NoError(t, db.Get(ctx, &count, th.Fmt("SELECT count(1) FROM %s")))
			assert.Equal(t, 0, count)
		})
	})
}

func TestCustomStructTag(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, &sqlz.Options{StructTag: "json"})

		type User struct {
			Identifier int    `json:"id"`
			User       string `json:"username"`
			Email      string
			Pw         string `json:"password"`
			Age        int
			Active     bool
		}

		q := `SELECT
						1 AS id,
						'Alice' AS username,
						'alice@wonderland.com' AS email,
						'123456' as password,
						18 as age,
						TRUE as active`

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err := db.Get(ctx, &user, q)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}

func TestPool(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		db := sqlz.New(conn.DriverName, conn.DB, nil)
		assert.IsType(t, &sql.DB{}, db.Pool())
		db.Pool().SetMaxOpenConns(42)
		assert.Equal(t, 42, db.Pool().Stats().MaxOpenConnections)

		tx, err := db.Begin(ctx)
		assert.NoError(t, err)
		assert.IsType(t, &sql.Tx{}, tx.Conn())
	})
}
