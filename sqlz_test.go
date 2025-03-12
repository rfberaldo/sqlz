package sqlz_test

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rafaberaldo/sqlz"
	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/rafaberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	err = db.QueryRow(&s, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)

	tx, err := db.Begin()
	assert.NoError(t, err)

	err = tx.Query(&ss, query)
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	err = tx.QueryRow(&s, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)
}

func shouldReturnErrorForWrongQuery(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	var err error
	const query = "WRONG QUERY"
	const str = "WRONG"

	_, err = db.Exec(query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	var i int
	err = db.Query(&i, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	err = db.QueryRow(&i, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	tx, err := db.Begin()
	assert.NoError(t, err)

	_, err = tx.Exec(query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	err = tx.Query(&i, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	err = tx.QueryRow(&i, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)
}

func shouldReturnNotFound(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	ctx := context.Background()
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	var s string
	query := fmt.Sprintf("SELECT * FROM %s", table)

	err = db.QueryRow(&s, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)

	err = db.QueryRowCtx(ctx, &s, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)

	tx, err := db.Begin()
	assert.NoError(t, err)
	defer tx.Rollback()

	err = tx.QueryRow(&s, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)

	err = tx.QueryRowCtx(ctx, &s, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
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
	assert.NoError(t, err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", "alice@wonderland.com", "123456", 18, true,
	)
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	assert.Equal(t, expected, user)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, expected, user)

	err = db.Query(&users, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, []User{expected}, users)

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, []User{expected}, users)
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
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	ctx := context.Background()

	var user User
	var users []User
	lastUser := User{COUNT, "John", "john@id.com", "abc123", testutil.PtrTo(COUNT + 17)}

	err = db.Query(&users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	err = db.QueryRow(&user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)
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
	assert.NoError(t, err)

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
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	err = db.QueryCtx(ctx, &users, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	err = db.QueryRow(&user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)

	err = db.QueryRowCtx(ctx, &user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)
}

func shouldScanBuiltinType(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	insertTmpl := testutil.Schema(bind, `INSERT INTO %s (id, name) VALUES (?, ?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table), 1, "Alice")
	assert.NoError(t, err)

	selectTmplId := testutil.Schema(bind, `SELECT id FROM %s WHERE id = ?`)
	selectTmplName := testutil.Schema(bind, `SELECT name FROM %s WHERE id = ?`)

	var id int
	err = db.QueryRow(&id, fmt.Sprintf(selectTmplId, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, id)

	var name string
	err = db.QueryRow(&name, fmt.Sprintf(selectTmplName, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, "Alice", name)
}

func namedQueryShouldParseInClause(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
	)
	assert.NoError(t, err)

	selectTmpl := `SELECT name FROM %s WHERE id IN (:ids)`

	var names []string
	arg := map[string]any{"ids": []int{2, 3}}
	err = db.Query(&names, fmt.Sprintf(selectTmpl, table), arg)
	assert.NoError(t, err)
	assert.Equal(t, []string{"John", "Carl"}, names)
}

func queryShouldParseInClause(t *testing.T, db *sqlz.DB, bind parser.Bind) {
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `
		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	insertTmpl := testutil.Schema(bind, `
		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
	)
	assert.NoError(t, err)

	selectTmpl := testutil.Schema(bind, `SELECT name FROM %s WHERE id IN (?)`)

	var names []string
	err = db.Query(&names, fmt.Sprintf(selectTmpl, table), []int{2, 3})
	if bind == parser.BindQuestion {
		assert.NoError(t, err)
		assert.Equal(t, []string{"John", "Carl"}, names)
	} else {
		assert.Error(t, err)
	}
}
