package core

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/rafaberaldo/sqlz/binds"
	"github.com/rafaberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const structTag = "db"

var (
	dbMySQL *sql.DB
	dbPGSQL *sql.DB
	ctx     = context.Background()
	scanner = newScanner(structTag)
)

func init() {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	if db, err := connect("mysql", dsn); err == nil {
		dbMySQL = db
	}

	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
	if db, err := connect("pgx", dsn); err == nil {
		dbPGSQL = db
	}
}

func connect(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Printf("error connecting to %v: %v", driverName, err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Printf("error pinging to %v: %v", driverName, err)
		db.Close()
		return nil, err
	}

	return db, nil
}

// run is a helper to run the test on multiple DB.
func run(t *testing.T, fn func(t *testing.T, db *sql.DB, bind binds.Bind)) {
	t.Parallel()
	t.Run("MySQL", func(t *testing.T) {
		t.Parallel()
		if dbMySQL == nil {
			if os.Getenv("CI") == "true" {
				t.Fatal("Fail, unable to connect to DB:", t.Name())
			} else {
				t.Skip("Skipping, unable to connect to DB:", t.Name())
			}
		}
		fn(t, dbMySQL, binds.Question)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		t.Parallel()
		if dbPGSQL == nil {
			if os.Getenv("CI") == "true" {
				t.Fatal("Fail, unable to connect to DB:", t.Name())
			} else {
				t.Skip("Skipping, unable to connect to DB:", t.Name())
			}
		}
		fn(t, dbPGSQL, binds.Dollar)
	})
}

func newScanner(tag string) *dbscan.API {
	scanner, err := dbscan.NewAPI(
		dbscan.WithStructTagKey(tag),
		dbscan.WithScannableTypes((*sql.Scanner)(nil)),
	)
	if err != nil {
		panic("sqlz: creating scanner: " + err.Error())
	}
	return scanner
}

func TestBasicQueryMethods(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		var err error
		var s string
		var ss []string

		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		err = Query(ctx, db, bind, scanner, structTag, &ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = QueryRow(ctx, db, bind, scanner, structTag, &s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)
	})
}

func TestShouldReturnErrorForWrongQuery(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		var err error
		var dst any
		const query = "WRONG QUERY"
		const shouldContain = "WRONG"

		_, err = Exec(ctx, db, bind, structTag, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)

		err = Query(ctx, db, bind, scanner, structTag, &dst, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)

		err = QueryRow(ctx, db, bind, scanner, structTag, &dst, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)
	})
}

func TestShouldReturnNotFoundOnQueryRow(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		table := testutil.TableName(t.Name())
		t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

		createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
		_, err := db.Exec(fmt.Sprintf(createTmpl, table))
		assert.NoError(t, err)

		query := fmt.Sprintf("SELECT * FROM %s", table)

		var s any
		err = QueryRow(ctx, db, bind, scanner, structTag, &s, query)
		assert.Error(t, err)
		assert.Equal(t, true, errors.Is(err, sql.ErrNoRows), err)
	})
}

func TestQueryArgs(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		table := testutil.TableName(t.Name())
		t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

		createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			username VARCHAR(255),
			age INT,
			active BOOL,
			created_at TIMESTAMP
		)`
		_, err := db.Exec(fmt.Sprintf(createTmpl, table))
		assert.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		insertTmpl := testutil.Rebind(bind, `
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)
		_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		assert.NoError(t, err)

		type User struct {
			Id       int       `db:"id"`
			Username string    `db:"username"`
			Age      int       `db:"age"`
			Active   bool      `db:"active"`
			Created  time.Time `db:"created_at"`
		}

		t.Run("query without args should perform a regular query", func(t *testing.T) {
			expected := []User{
				{1, "Alice", 18, true, ts},
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			var users []User
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf("SELECT * FROM %s", table))
			assert.NoError(t, err)
			assert.Equal(t, 3, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with multiple default placeholders", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? OR id = ?`)
			var users []User
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table), 2, 3)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using default placeholder", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
			var users []User
			ids := []int{2, 3}
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table), ids)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with struct named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := struct{ Id int }{Id: 2}
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with map named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := map[string]any{"id": 2}
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
			var users []User
			arg := map[string]any{"ids": []int{2, 3}}
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should return length 0 if no result is found", func(t *testing.T) {
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
			var users []User
			err = Query(ctx, db, bind, scanner, structTag, &users, fmt.Sprintf(selectTmpl, table))
			assert.NoError(t, err)
			assert.Equal(t, 0, len(users))
		})
	})
}

func TestQueryRowArgs(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		table := testutil.TableName(t.Name())
		t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

		createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			username VARCHAR(255),
			age INT,
			active BOOL,
			created_at TIMESTAMP
		)`
		_, err := db.Exec(fmt.Sprintf(createTmpl, table))
		assert.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		insertTmpl := testutil.Rebind(bind, `
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)
		_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		assert.NoError(t, err)

		type User struct {
			Id       int       `db:"id"`
			Username string    `db:"username"`
			Age      int       `db:"age"`
			Active   bool      `db:"active"`
			Created  time.Time `db:"created_at"`
		}

		t.Run("query row without args should perform a regular query", func(t *testing.T) {
			expected := User{1, "Alice", 18, true, ts}
			var user User
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with multiple default placeholders", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? AND active = ?`)
			var user User
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table), 2, true)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using default placeholder", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
			var user User
			ids := []int{2}
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table), ids)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with struct named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := struct{ Id int }{Id: 2}
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with map named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := map[string]any{"id": 2}
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
			var user User
			arg := map[string]any{"ids": []int{2}}
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should return error if no result is found", func(t *testing.T) {
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
			var user User
			err = QueryRow(ctx, db, bind, scanner, structTag, &user, fmt.Sprintf(selectTmpl, table))
			assert.Error(t, err)
			assert.ErrorIs(t, err, sql.ErrNoRows)
		})
	})
}

func TestExecArgs(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
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

		t.Run("multiple args should perform a regular exec", func(t *testing.T) {
			insertTmpl := testutil.Rebind(bind, `
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(insertTmpl, table),
				1, "Alice", 18,
				2, "Rob", 38,
				3, "John", 4,
			)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 3, int(rows))
		})

		t.Run("1 arg struct should perform a named exec", func(t *testing.T) {
			deleteStmt := "DELETE FROM %s WHERE id = :id"
			arg := struct{ Id int }{Id: 1}
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(deleteStmt, table), arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg map should perform a named exec", func(t *testing.T) {
			deleteStmt := "DELETE FROM %s WHERE id = :id"
			arg := map[string]any{"id": 2}
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(deleteStmt, table), arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg int should perform a regular exec", func(t *testing.T) {
			deleteStmt := testutil.Rebind(bind, "DELETE FROM %s WHERE id = ?")
			arg := 3
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(deleteStmt, table), arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg []struct should perform a named batch insert", func(t *testing.T) {
			type Person struct {
				Id   int
				Name string
				Age  int
			}
			const COUNT = 100
			args := make([]Person, COUNT)
			for i := range COUNT {
				args[i] = Person{i + 1, "Name", 20}
			}
			insertTmpl := `INSERT INTO %s (id, name, age) VALUES (:id, :name, :age)`
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(insertTmpl, table), args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))

			re, err = Exec(ctx, db, bind, structTag, "DELETE FROM "+table)
			assert.NoError(t, err)

			rows, err = re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("1 arg []map should perform a named batch insert", func(t *testing.T) {
			const COUNT = 100
			args := make([]map[string]any, COUNT)
			for i := range COUNT {
				args[i] = map[string]any{"id": i + 1, "name": "Name", "age": 20}
			}
			insertTmpl := `INSERT INTO %s (id, name, age) VALUES (:id, :name, :age)`
			re, err := Exec(ctx, db, bind, structTag, fmt.Sprintf(insertTmpl, table), args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})
	})
}

func TestCustomStructTag(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
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

		insertTmpl := testutil.Rebind(bind, `
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?)`)
		_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
			1, "Alice", "alice@wonderland.com", "123456", 18, true,
			2, "Rob", "rob@google.com", "123456", 38, true,
			3, "John", "john@id.com", "123456", 24, false,
		)
		assert.NoError(t, err)

		type User struct {
			Id     int
			User   string `json:"username"`
			Email  string
			Pw     string `json:"password"`
			Age    int
			Active bool
		}

		scanner := newScanner("json")

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err = QueryRow(ctx, db, bind, scanner, "json", &user, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}
