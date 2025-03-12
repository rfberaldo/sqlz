package sqlu_test

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/rafaberaldo/sqlz/internal/testing/testutil"
	"github.com/rafaberaldo/sqlz/sqlu"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSQLU(t *testing.T) {
	// tests must be self-contained and able to run in parallel
	type Test func(t *testing.T, db *sql.DB, bind parser.Bind)
	tests := []Test{
		basicQueryMethods,
		shouldReturnErrorForWrongQuery,
		shouldReturnNotFound,
		queryShouldReturnCorrect,
		batchInsertStruct,
		// batchInsertMap,
		// shouldScanBuiltinType,
		// namedQueryShouldParseInClause,
		// queryShouldParseInClause,
	}
	t.Run("MySQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
		db, err := sqlu.Connect("mysql", dsn)
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
		db, err := sqlu.Connect("pgx", dsn)
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

func basicQueryMethods(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	var err error
	var s string
	var ss []string

	query := "SELECT 'Hello World'"
	expected := "Hello World"
	expectedSlice := []string{"Hello World"}

	ss, err = sqlu.Query[string](db, query)
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	ss, err = sqlu.QueryCtx[string](ctx, db, query)
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	s, err = sqlu.QueryRow[string](db, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)

	s, err = sqlu.QueryRowCtx[string](ctx, db, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)
}

func shouldReturnErrorForWrongQuery(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	var err error
	const query = "WRONG QUERY"
	const str = "WRONG"

	_, err = sqlu.Exec(db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	_, err = sqlu.ExecCtx(ctx, db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	_, err = sqlu.Query[int](db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	_, err = sqlu.QueryCtx[int](ctx, db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	_, err = sqlu.QueryRow[int](db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)

	_, err = sqlu.QueryRowCtx[int](ctx, db, query)
	assert.Equal(t, true, strings.Contains(err.Error(), str))
	assert.Error(t, err)
}

func shouldReturnNotFound(t *testing.T, db *sql.DB, bind parser.Bind) {
	ctx := context.Background()
	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	query := fmt.Sprintf("SELECT * FROM %s", table)

	_, err = sqlu.QueryRow[string](db, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)

	_, err = sqlu.QueryRowCtx[string](ctx, db, query)
	assert.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func queryShouldReturnCorrect(t *testing.T, db *sql.DB, bind parser.Bind) {
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
	_, err := sqlu.Exec(db, fmt.Sprintf(createTmpl, table))
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

	expected := User{1, "Alice", "alice@wonderland.com", "123456", testutil.PtrTo(18), true}
	ctx := context.Background()

	var user User

	user, err = sqlu.QueryRow[User](db, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, expected, user)

	user, err = sqlu.QueryRowCtx[User](ctx, db, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, expected, user)

	users, err := sqlu.Query[User](db, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, []User{expected}, users)

	users, err = sqlu.QueryCtx[User](ctx, db, fmt.Sprintf(selectTmpl, table), 1)
	assert.NoError(t, err)
	assert.Equal(t, []User{expected}, users)
}

func batchInsertStruct(t *testing.T, db *sql.DB, bind parser.Bind) {
	sqlu.SetDefaultBind(bind)
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

	const COUNT = 5
	args := make([]*User, 0, COUNT)
	for i := range COUNT {
		user := &User{i + 1, "John", "john@id.com", "abc123", testutil.PtrTo(18 + i)}
		args = append(args, user)
	}

	insertTmpl := `
			INSERT INTO %s (id, username, email, password, age)
			VALUES (:id, :username, :email, :password, :age)`
	_, err = sqlu.Exec(db, fmt.Sprintf(insertTmpl, table), args)
	assert.NoError(t, err)

	ctx := context.Background()

	lastUser := User{COUNT, "John", "john@id.com", "abc123", testutil.PtrTo(COUNT + 17)}

	users, err := sqlu.Query[User](db, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	users, err = sqlu.QueryCtx[User](ctx, db, fmt.Sprintf(`SELECT * FROM %s`, table))
	assert.NoError(t, err)
	assert.Equal(t, COUNT, len(users))

	user, err := sqlu.QueryRow[User](db, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)

	user, err = sqlu.QueryRowCtx[User](ctx, db, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
	assert.NoError(t, err)
	assert.Equal(t, lastUser, user)
}

// func batchInsertMap(t *testing.T, db *sql.DB, bind parser.Bind) {
// 	table := testutil.TableName(t.Name())
// 	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

// 	createTmpl := `
// 		CREATE TABLE %s (
// 			id INT PRIMARY KEY,
// 			username VARCHAR(255),
// 			email VARCHAR(255),
// 			password VARCHAR(255),
// 			age INT
// 		)`
// 	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
// 	assert.NoError(t, "create table", err)

// 	const COUNT = 1000
// 	users := make([]map[string]any, 0, COUNT)
// 	for i := range COUNT {
// 		user := map[string]any{
// 			"id":       i + 1,
// 			"username": "John",
// 			"email":    "john@id.com",
// 			"password": "abc123",
// 			"age":      testutil.PtrTo(18 + i)}
// 		users = append(users, user)
// 	}

// 	insertTmpl := `
// 		INSERT INTO %s (id, username, email, password, age)
// 		VALUES (:id, :username, :email, :password, :age)`
// 	_, err = db.ExecNamed(fmt.Sprintf(insertTmpl, table), users)
// 	assert.NoError(t, "insert batch", err)

// 	type User struct {
// 		Id       int
// 		Username string
// 		Email    string
// 		Pw       string `db:"password"`
// 		Age      *int
// 	}

// 	ctx := context.Background()

// 	var user User
// 	var users2 []User
// 	lastUser := User{COUNT, "John", "john@id.com", "abc123", testutil.PtrTo(COUNT + 17)}

// 	err = db.Query(&users2, fmt.Sprintf(`SELECT * FROM %s`, table))
// 	assert.NoError(t, "Query should not error", err)
// 	assert.Equal(t, "Query should return 1000 records", len(users), COUNT)

// 	err = db.QueryCtx(ctx, &users2, fmt.Sprintf(`SELECT * FROM %s`, table))
// 	assert.NoError(t, "QueryCtx should not error", err)
// 	assert.Equal(t, "QueryCtx should return 1000 records", len(users), COUNT)

// 	err = db.QueryRowNamed(&user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
// 	assert.NoError(t, "QueryRowNamed should not error", err)
// 	assert.Equal(t, "QueryRowNamed should return last user", user, lastUser)

// 	err = db.QueryRowNamedCtx(ctx, &user, fmt.Sprintf(`SELECT * FROM %s WHERE id = :id`, table), User{Id: COUNT})
// 	assert.NoError(t, "QueryRowNamedCtx should not error", err)
// 	assert.Equal(t, "QueryRowNamedCtx should return last user", user, lastUser)
// }

// func shouldScanBuiltinType(t *testing.T, db *sql.DB, bind parser.Bind) {
// 	table := testutil.TableName(t.Name())
// 	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

// 	createTmpl := `
// 		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
// 	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
// 	assert.NoError(t, "create table", err)

// 	insertTmpl := testutil.Schema(bind, `INSERT INTO %s (id, name) VALUES (?, ?)`)
// 	_, err = db.Exec(fmt.Sprintf(insertTmpl, table), 1, "Alice")
// 	assert.NoError(t, "insert", err)

// 	selectTmplId := testutil.Schema(bind, `SELECT id FROM %s WHERE id = ?`)
// 	selectTmplName := testutil.Schema(bind, `SELECT name FROM %s WHERE id = ?`)

// 	var id int
// 	err = db.QueryRow(&id, fmt.Sprintf(selectTmplId, table), 1)
// 	assert.NoError(t, "QueryRow should not error", err)
// 	assert.Equal(t, "QueryRow scanned string", id, 1)

// 	var name string
// 	err = db.QueryRow(&name, fmt.Sprintf(selectTmplName, table), 1)
// 	assert.NoError(t, "QueryRow should not error", err)
// 	assert.Equal(t, "QueryRow scanned string", name, "Alice")
// }

// func namedQueryShouldParseInClause(t *testing.T, db *sql.DB, bind parser.Bind) {
// 	table := testutil.TableName(t.Name())
// 	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

// 	createTmpl := `
// 		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
// 	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
// 	assert.NoError(t, "create table", err)

// 	insertTmpl := testutil.Schema(bind, `
// 		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
// 	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
// 		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
// 	)
// 	assert.NoError(t, "insert", err)

// 	selectTmpl := `SELECT name FROM %s WHERE id IN (:ids)`

// 	var names []string
// 	arg := map[string]any{"ids": []int{2, 3}}
// 	err = db.QueryNamed(&names, fmt.Sprintf(selectTmpl, table), arg)
// 	assert.NoError(t, "QueryNamed should not error", err)
// 	assert.Equal(t, "QueryNamed result should match", names, []string{"John", "Carl"})
// }

// func queryShouldParseInClause(t *testing.T, db *sql.DB, bind parser.Bind) {
// 	table := testutil.TableName(t.Name())
// 	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

// 	createTmpl := `
// 		CREATE TABLE %s (id INT PRIMARY KEY, name VARCHAR(255))`
// 	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
// 	assert.NoError(t, "create table", err)

// 	insertTmpl := testutil.Schema(bind, `
// 		INSERT INTO %s (id, name) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)`)
// 	_, err = db.Exec(fmt.Sprintf(insertTmpl, table),
// 		1, "Alice", 2, "John", 3, "Carl", 4, "Chad", 5, "Brenda",
// 	)
// 	assert.NoError(t, "insert", err)

// 	selectTmpl := testutil.Schema(bind, `SELECT name FROM %s WHERE id IN (?)`)

// 	var names []string
// 	err = db.Query(&names, fmt.Sprintf(selectTmpl, table), []int{2, 3})
// 	if bind == parser.BindQuestion {
// 		assert.NoError(t, "Query should not error", err)
// 		assert.Equal(t, "Query result should match", names, []string{"John", "Carl"})
// 	} else {
// 		assert.Error(t, "Query should error, IN clause for non question bind", err)
// 	}
// }
