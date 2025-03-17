package core

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/rafaberaldo/sqlz/binds"
	"github.com/rafaberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const structTag = "db"

var (
	dbMySQL *sql.DB
	dbPGS   *sql.DB
	ctx     = context.Background()
)

func init() {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	if db, err := connect("mysql", dsn); err == nil {
		dbMySQL = db
	}

	dsn = cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
	if db, err := connect("pgx", dsn); err == nil {
		dbPGS = db
	}
}

func connect(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
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
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbMySQL, binds.Question)
	})
	t.Run("PostgreSQL", func(t *testing.T) {
		t.Parallel()
		if dbPGS == nil {
			t.Skip("Skipping test, unable to connect to DB:", t.Name())
		}
		fn(t, dbPGS, binds.Dollar)
	})
}

func TestBasicQueryMethods(t *testing.T) {
	run(t, func(t *testing.T, db *sql.DB, bind binds.Bind) {
		var err error
		var s string
		var ss []string

		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		err = Query(ctx, db, bind, structTag, &ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = QueryRow(ctx, db, bind, structTag, &s, query)
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

		err = Query(ctx, db, bind, structTag, &dst, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)

		err = QueryRow(ctx, db, bind, structTag, &dst, query)
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
		err = QueryRow(ctx, db, bind, structTag, &s, query)
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
			Id       int
			Username string
			Email    string
			Pw       string `db:"password"`
			Age      int
			Active   bool
		}

		t.Run("query without args should perform a regular query", func(t *testing.T) {
			expected := []User{
				{1, "Alice", "alice@wonderland.com", "123456", 18, true},
				{2, "Rob", "rob@google.com", "123456", 38, true},
				{3, "John", "john@id.com", "123456", 24, false},
			}
			var users []User
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf("SELECT * FROM %s", table))
			assert.NoError(t, err)
			assert.Equal(t, 3, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with multiple default placeholders", func(t *testing.T) {
			expected := []User{
				{2, "Rob", "rob@google.com", "123456", 38, true},
				{3, "John", "john@id.com", "123456", 24, false},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? OR id = ?`)
			var users []User
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table), 2, 3)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using default placeholder", func(t *testing.T) {
			expected := []User{
				{2, "Rob", "rob@google.com", "123456", 38, true},
				{3, "John", "john@id.com", "123456", 24, false},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
			var users []User
			ids := []int{2, 3}
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table), ids)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with struct named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", "rob@google.com", "123456", 38, true},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := struct{ Id int }{Id: 2}
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with map named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", "rob@google.com", "123456", 38, true},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := map[string]any{"id": 2}
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", "rob@google.com", "123456", 38, true},
				{3, "John", "john@id.com", "123456", 24, false},
			}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
			var users []User
			arg := map[string]any{"ids": []int{2, 3}}
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should return length 0 if no result is found", func(t *testing.T) {
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
			var users []User
			err = Query(ctx, db, bind, structTag, &users, fmt.Sprintf(selectTmpl, table))
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
			Id       int
			Username string
			Email    string
			Pw       string `db:"password"`
			Age      int
			Active   bool
		}

		t.Run("query row without args should perform a regular query", func(t *testing.T) {
			expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
			var user User
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with multiple default placeholders", func(t *testing.T) {
			expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? AND active = ?`)
			var user User
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table), 2, true)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using default placeholder", func(t *testing.T) {
			expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
			var user User
			ids := []int{2}
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table), ids)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with struct named arg", func(t *testing.T) {
			expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := struct{ Id int }{Id: 2}
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with map named arg", func(t *testing.T) {
			expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := map[string]any{"id": 2}
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using named arg", func(t *testing.T) {
			expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
			var user User
			arg := map[string]any{"ids": []int{2}}
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table), arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should return error if no result is found", func(t *testing.T) {
			selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
			var user User
			err = QueryRow(ctx, db, bind, structTag, &user, fmt.Sprintf(selectTmpl, table))
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

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err = QueryRow(ctx, db, bind, "json", &user, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}
