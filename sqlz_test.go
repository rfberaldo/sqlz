package sqlz_test

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rafaberaldo/sqlz"
	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/rafaberaldo/sqlz/internal/testing/assert"
	"github.com/rafaberaldo/sqlz/internal/testing/testutil"

	_ "github.com/go-sql-driver/mysql"
)

func TestSQLZ(t *testing.T) {
	// tests must be self-contained and able to run in parallel
	type Test func(t *testing.T, db *sqlz.DB, bind parser.Bind)
	tests := []Test{
		basicQueryMethods,
		shouldReturnErrorForWrongQuery,
		shouldReturnNotFound,
		queryShouldReturnCorrect,
		batchInsertStruct,
		batchInsertMap,
		shouldScanBuiltinType,
		namedQueryShouldParseInClause,
		queryShouldParseInClause,
	}

	t.Run("MySQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
		db, err := sqlz.Connect("mysql", dsn)
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
		db, err := sqlz.Connect("pgx", dsn)
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

func basicQueryMethods(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	var err error
	var s string
	var ss []string

	query := "SELECT 'Hello World'"
	expected := "Hello World"
	expectedSlice := []string{"Hello World"}

	err = db.Query(&ss, query)
	assert.NoError(t, "db Query", err)
	assert.Equal(t, "db Query scan", ss, expectedSlice)

	err = db.QueryRow(&s, query)
	assert.NoError(t, "db QueryRow", err)
	assert.Equal(t, "db QueryRow scan", s, expected)

	tx, err := db.Begin()
	assert.NoError(t, "start transaction", err)

	err = tx.Query(&ss, query)
	assert.NoError(t, "tx Query", err)
	assert.Equal(t, "tx Query scan", ss, expectedSlice)

	err = tx.QueryRow(&s, query)
	assert.NoError(t, "tx QueryRow", err)
	assert.Equal(t, "tx QueryRow scan", s, expected)
}

func shouldReturnErrorForWrongQuery(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	var err error
	const query = "WRONG QUERY"
	const str = "WRONG"

	_, err = db.Exec(query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "db Exec", err)

	var i int
	err = db.Query(&i, query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "db Query", err)

	err = db.QueryRow(&i, query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "db QueryRow", err)

	tx, err := db.Begin()
	assert.NoError(t, "start transaction", err)

	_, err = tx.Exec(query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "tx Exec", err)

	err = tx.Query(&i, query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "tx Query", err)

	err = tx.QueryRow(&i, query)
	assert.Equal(t, "should contain 'WRONG' in error",
		strings.Contains(err.Error(), str), true)
	assert.Error(t, "tx QueryRow", err)
}

func shouldReturnNotFound(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	ctx := context.Background()
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	var s string
	query := fmt.Sprintf("SELECT * FROM %s", table)

	err = db.QueryRow(&s, query)
	assert.Error(t, "db QueryRow should error", err)
	assert.Equal(t, "db QueryRow error should be IsNotFound", sqlz.IsNotFound(err), true)

	err = db.QueryRowCtx(ctx, &s, query)
	assert.Error(t, "db QueryRowCtx should error", err)
	assert.Equal(t, "db QueryRowCtx error should be IsNotFound", sqlz.IsNotFound(err), true)

	tx, err := db.Begin()
	assert.NoError(t, "start transaction", err)
	defer tx.Rollback()

	err = tx.QueryRow(&s, query)
	assert.Error(t, "tx QueryRow should error", err)
	assert.Equal(t, "tx QueryRow error should be IsNotFound", sqlz.IsNotFound(err), true)

	err = tx.QueryRowCtx(ctx, &s, query)
	assert.Error(t, "tx QueryRowCtx should error", err)
	assert.Equal(t, "tx QueryRowCtx error should be IsNotFound", sqlz.IsNotFound(err), true)
}

func queryShouldReturnCorrect(t *testing.T, db *sqlz.DB, bind parser.Bind) {
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
	assert.NoError(t, "create table", err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", "alice@wonderland.com", "123456", 18, true,
	)
	assert.NoError(t, "insert", err)

	selectTmpl := testutil.Schema(bind, `SELECT * FROM %s WHERE id = ?`)

	type User struct {
		Id       int
		Username string
		Email    string
		Pw       string `db:"password"`
		Age      *int
		Active   bool
	}

	var user User
	var users []User
	expected := User{1, "Alice", "alice@wonderland.com", "123456", testutil.PtrTo(18), true}
	ctx := context.Background()

	err = db.QueryRow(&user, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, "QueryRow should not error", err)
	assert.Equal(t, "QueryRow result should be correct", user, expected)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, "QueryRowCtx should not error", err)
	assert.Equal(t, "QueryRow ctx result should be correct", user, expected)

	err = db.Query(&users, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, "Query should not error", err)
	assert.Equal(t, "Query result should be correct", users, []User{expected})

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, "QueryCtx should not error", err)
	assert.Equal(t, "QueryCtx result should be correct", users, []User{expected})
}

func batchInsertStruct(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			username VARCHAR(255),
			email VARCHAR(255),
			password VARCHAR(255),
			age INT
		)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	type User struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      *int
	}

	const COUNT = 1000
	args := make([]*User, 0, COUNT)
	for i := range COUNT {
		user := &User{i + 1, "John", "john@id.com", "abc123", testutil.PtrTo(18 + i)}
		args = append(args, user)
	}

	insertTmpl := `
			INSERT INTO %s (id, username, email, password, age)
			VALUES (:id, :username, :email, :password, :age)`
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table), args)
	assert.NoError(t, "insert", err)

	ctx := context.Background()

	var user User
	var users []User
	lastUser := User{COUNT, "John", "john@id.com", "abc123", testutil.PtrTo(COUNT + 17)}

	err = db.Query(&users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, "Query should not error", err)
	assert.Equal(t, "Query should return 1000 records", len(users), COUNT)

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, "QueryCtx should not error", err)
	assert.Equal(t, "QueryCtx should return 1000 records", len(users), COUNT)

	err = db.QueryRow(&user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, "QueryRow should not error", err)
	assert.Equal(t, "QueryRow should return last user", user, lastUser)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, "QueryRowCtx should not error", err)
	assert.Equal(t, "QueryRowCtx should return last user", user, lastUser)
}

func batchInsertMap(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			username VARCHAR(255),
			email VARCHAR(255),
			password VARCHAR(255),
			age INT
		)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	const COUNT = 1000
	args := make([]map[string]any, 0, COUNT)
	for i := range COUNT {
		user := map[string]any{
			"id":       i + 1,
			"username": "John",
			"email":    "john@id.com",
			"password": "abc123",
			"age":      testutil.PtrTo(18 + i)}
		args = append(args, user)
	}

	insertTmpl := `
		INSERT INTO %s (id, username, email, password, age)
		VALUES (:id, :username, :email, :password, :age)`
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table), args)
	assert.NoError(t, "insert batch", err)

	type User struct {
		Id       int
		Username string
		Email    string
		Pw       string `db:"password"`
		Age      *int
	}

	ctx := context.Background()

	var user User
	var users []User
	lastUser := User{COUNT, "John", "john@id.com", "abc123", testutil.PtrTo(COUNT + 17)}

	err = db.Query(&users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, "Query should not error", err)
	assert.Equal(t, "Query should return 1000 records", len(users), COUNT)

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, "QueryCtx should not error", err)
	assert.Equal(t, "QueryCtx should return 1000 records", len(users), COUNT)

	err = db.QueryRow(&user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, "QueryRow should not error", err)
	assert.Equal(t, "QueryRow should return last user", user, lastUser)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, "QueryRowCtx should not error", err)
	assert.Equal(t, "QueryRowCtx should return last user", user, lastUser)
}

func shouldScanBuiltinType(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	insertTmpl := testutil.Schema(bind, `INSERT INTO %s (id, name) VALUES (?, ?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table), 1, "Alice")
	assert.NoError(t, "insert", err)

	selectTmplId := testutil.Schema(bind, `SELECT id FROM %s WHERE id = ?`)
	selectTmplName := testutil.Schema(bind, `SELECT name FROM %s WHERE id = ?`)

	var id int
	err = db.QueryRow(&id, fmt.Sprintf(selectTmplId, table), 1)
	assert.NoError(t, "QueryRow should not error", err)
	assert.Equal(t, "QueryRow scanned string", id, 1)

	var name string
	err = db.QueryRow(&name, fmt.Sprintf(selectTmplName, table), 1)
	assert.NoError(t, "QueryRow should not error", err)
	assert.Equal(t, "QueryRow scanned string", name, "Alice")
}

func namedQueryShouldParseInClause(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
	)
	assert.NoError(t, "insert", err)

	selectTmpl := `SELECT name FROM %s WHERE id IN (:ids)`

	var names []string
	arg := map[string]any{"ids": []int{2, 3}}
	err = db.Query(&names, fmt.Sprintf(selectTmpl, table), arg)
	assert.NoError(t, "Query should not error", err)
	assert.Equal(t, "Query result should match", names, []string{"John", "Carl"})
}

func queryShouldParseInClause(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, "create table", err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
	)
	assert.NoError(t, "insert", err)

	selectTmpl := testutil.Schema(bind, `SELECT name FROM %s WHERE id IN (?)`)

	var names []string
	err = db.Query(&names, fmt.Sprintf(selectTmpl, table), []int{2, 3})
	if bind == parser.BindQuestion {
		assert.NoError(t, "Query should not error", err)
		assert.Equal(t, "Query result should match", names, []string{"John", "Carl"})
	} else {
		assert.Error(t, "Query should error, IN clause for non question bind", err)
	}
}
