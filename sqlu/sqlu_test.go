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

func TestSQLUtil(t *testing.T) {
	// tests must be self-contained and able to run in parallel
	type Test func(t *testing.T, db *sql.DB, bind parser.Bind)
	tests := []Test{
		basicQueryMethods,
		shouldReturnErrorForWrongQuery,
		shouldReturnNotFoundOnQueryRow,
		queryArgs,
		queryRowArgs,
		execArgs,
	}
	t.Run("MySQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
		db, err := Connect("mysql", dsn)
		if err != nil {
			log.Printf("Skipping MySQL tests: %v", err)
			t.Skip()
		}

		for _, fn := range tests {
			t.Run("sqlu_"+testutil.FuncName(fn), func(t *testing.T) {
				t.Parallel()
				fn(t, db, parser.BindQuestion)
			})
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		dsn := cmp.Or(os.Getenv("POSTGRES_DSN"), testutil.POSTGRES_DSN)
		db, err := Connect("pgx", dsn)
		if err != nil {
			log.Printf("Skipping PostgreSQL tests: %v", err)
			t.Skip()
		}

		for _, fn := range tests {
			t.Run("sqlu_"+testutil.FuncName(fn), func(t *testing.T) {
				t.Parallel()
				fn(t, db, parser.BindDollar)
			})
		}
	})
}

func basicQueryMethods(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

	query := "SELECT 'Hello World'"
	expected := "Hello World"
	expectedSlice := []string{"Hello World"}

	ss, err := Query[string](db, query)
	assert.NoError(t, err)
	assert.Equal(t, expectedSlice, ss)

	s, err := QueryRow[string](db, query)
	assert.NoError(t, err)
	assert.Equal(t, expected, s)
}

func shouldReturnErrorForWrongQuery(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

	const query = "WRONG QUERY"
	const shouldContain = "WRONG"

	_, err := Exec(db, query)
	assert.Error(t, err)
	assert.ErrorContains(t, err, shouldContain)

	_, err = Query[string](db, query)
	assert.Error(t, err)
	assert.ErrorContains(t, err, shouldContain)

	_, err = QueryRow[string](db, query)
	assert.Error(t, err)
	assert.ErrorContains(t, err, shouldContain)
}

func shouldReturnNotFoundOnQueryRow(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

	table := testutil.TableName(t.Name())
	t.Cleanup(func() { db.Exec("DROP TABLE " + table) })

	createTmpl := `CREATE TABLE %s (id INT PRIMARY KEY)`
	_, err := db.Exec(fmt.Sprintf(createTmpl, table))
	assert.NoError(t, err)

	query := fmt.Sprintf("SELECT * FROM %s", table)

	_, err = QueryRow[any](db, query)
	assert.Error(t, err)
	assert.Equal(t, true, errors.Is(err, sql.ErrNoRows), err)
}

func queryArgs(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

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
		t.Parallel()
		expected := []User{
			{1, "Alice", "alice@wonderland.com", "123456", 18, true},
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		users, err := Query[User](db, fmt.Sprintf("SELECT * FROM %s", table))
		assert.NoError(t, err)
		assert.Equal(t, 3, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should work with multiple default placeholders", func(t *testing.T) {
		t.Parallel()
		expected := []User{
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? OR id = ?`)
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table), 2, 3)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should parse IN clause using default placeholder", func(t *testing.T) {
		t.Parallel()
		if bind != parser.BindQuestion {
			t.Skip("skipping because IN clause only supported by '?' placeholders for now")
		}
		expected := []User{
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
		ids := []int{2, 3}
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table), ids)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should work with struct named arg", func(t *testing.T) {
		t.Parallel()
		expected := []User{
			{2, "Rob", "rob@google.com", "123456", 38, true},
		}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
		arg := struct{ Id int }{Id: 2}
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should work with map named arg", func(t *testing.T) {
		t.Parallel()
		expected := []User{
			{2, "Rob", "rob@google.com", "123456", 38, true},
		}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
		arg := map[string]any{"id": 2}
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should parse IN clause using named arg", func(t *testing.T) {
		t.Parallel()
		expected := []User{
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
		arg := map[string]any{"ids": []int{2, 3}}
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(users))
		assert.Equal(t, expected, users)
	})

	t.Run("query should return length 0 if no result is found", func(t *testing.T) {
		t.Parallel()
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
		users, err := Query[User](db, fmt.Sprintf(selectTmpl, table))
		assert.NoError(t, err)
		assert.Equal(t, 0, len(users))
	})
}

func queryRowArgs(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

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
		t.Parallel()
		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		user, err := QueryRow[User](db, fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should work with multiple default placeholders", func(t *testing.T) {
		t.Parallel()
		expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = ? AND active = ?`)
		user, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table), 2, true)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should parse IN clause using default placeholder", func(t *testing.T) {
		t.Parallel()
		if bind != parser.BindQuestion {
			t.Skip("skipping because IN clause only supported by '?' placeholders for now")
		}
		expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (?)`)
		ids := []int{2}
		user, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table), ids)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should work with struct named arg", func(t *testing.T) {
		t.Parallel()
		expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
		arg := struct{ Id int }{Id: 2}
		user, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should work with map named arg", func(t *testing.T) {
		t.Parallel()
		expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = :id`)
		arg := map[string]any{"id": 2}
		user, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should parse IN clause using named arg", func(t *testing.T) {
		t.Parallel()
		expected := User{2, "Rob", "rob@google.com", "123456", 38, true}
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id IN (:ids)`)
		arg := map[string]any{"ids": []int{2}}
		user, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table), arg)
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})

	t.Run("query row should return error if no result is found", func(t *testing.T) {
		t.Parallel()
		selectTmpl := testutil.Rebind(bind, `SELECT * FROM %s WHERE id = 42`)
		_, err := QueryRow[User](db, fmt.Sprintf(selectTmpl, table))
		assert.Error(t, err)
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func execArgs(t *testing.T, db *sql.DB, bind parser.Bind) {
	SetDefaultBind(bind)

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

	// these subtests must run sequentially

	t.Run("multiple args should perform a regular exec", func(t *testing.T) {
		insertTmpl := testutil.Rebind(bind, `
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)
		re, err := Exec(db, fmt.Sprintf(insertTmpl, table),
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
		re, err := Exec(db, fmt.Sprintf(deleteStmt, table), arg)
		assert.NoError(t, err)

		rows, err := re.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, 1, int(rows))
	})

	t.Run("1 arg map should perform a named exec", func(t *testing.T) {
		deleteStmt := "DELETE FROM %s WHERE id = :id"
		arg := map[string]any{"id": 2}
		re, err := Exec(db, fmt.Sprintf(deleteStmt, table), arg)
		assert.NoError(t, err)

		rows, err := re.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, 1, int(rows))
	})

	t.Run("1 arg int should perform a regular exec", func(t *testing.T) {
		deleteStmt := testutil.Rebind(bind, "DELETE FROM %s WHERE id = ?")
		arg := 3
		re, err := Exec(db, fmt.Sprintf(deleteStmt, table), arg)
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
		re, err := Exec(db, fmt.Sprintf(insertTmpl, table), args)
		assert.NoError(t, err)

		rows, err := re.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, COUNT, int(rows))

		re, err = Exec(db, "DELETE FROM "+table)
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
		re, err := Exec(db, fmt.Sprintf(insertTmpl, table), args)
		assert.NoError(t, err)

		rows, err := re.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, COUNT, int(rows))
	})
}
