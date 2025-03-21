package benchmark

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rfberaldo/sqlz"

	_ "github.com/mattn/go-sqlite3"
)

var ctx = context.Background()

func noError(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

func BenchmarkPlaceholderExec(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			age INT
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	input := "INSERT INTO benchmark (name, age) VALUES (?, ?)"
	args := []any{"Alice", 32}

	b.ResetTimer()
	for range b.N {
		_, err := db.Exec(ctx, input, args...)
		noError(b, err)
	}
}

func BenchmarkPlaceholderQueryRow(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	db.Exec(ctx, "INSERT INTO benchmark (name) VALUES (?)", "Alice")

	input := "SELECT name FROM benchmark WHERE id = ?"

	b.ResetTimer()
	for range b.N {
		var name string
		err := db.QueryRow(ctx, &name, input, 1)
		noError(b, err)
	}
}

func BenchmarkNamedQueryRow(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	db.Exec(ctx, "INSERT INTO benchmark (name) VALUES (?)", "Alice")

	input := "SELECT name FROM benchmark WHERE id = :id"
	arg := map[string]any{"id": 1}

	b.ResetTimer()
	for range b.N {
		var name string
		err := db.QueryRow(ctx, &name, input, arg)
		noError(b, err)
	}
}

func BenchmarkBatchInsertStruct(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "john", "john@id.com", "doom", 42})
	}
	input := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`

	b.ResetTimer()
	for range b.N {
		_, err := db.Exec(ctx, input, args)
		noError(b, err)
	}
}

func BenchmarkBatchInsertMap(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	var args []map[string]any
	for range 1000 {
		args = append(args, map[string]any{
			"username": "john",
			"email":    "john@id.com",
			"password": "doom",
			"age":      42,
		})
	}
	input := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`

	b.ResetTimer()
	for range b.N {
		_, err := db.Exec(ctx, input, args)
		noError(b, err)
	}
}

func BenchmarkStructScan(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "john", "john@id.com", "doom", 42})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(ctx, insertTmpl, args)
	noError(b, err)

	input := "SELECT * FROM benchmark"

	b.ResetTimer()
	for range b.N {
		var users []user
		err := db.Query(ctx, &users, input)
		noError(b, err)
	}
}

func BenchmarkStringScan(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		Id   int
		Name string
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "Alice"})
	}
	insertTmpl := `INSERT INTO benchmark (name)	VALUES (:name)`
	_, err = db.Exec(ctx, insertTmpl, args)
	noError(b, err)

	input := "SELECT name FROM benchmark"

	b.ResetTimer()
	for range b.N {
		var names []string
		err := db.Query(ctx, &names, input)
		noError(b, err)
	}
}

func BenchmarkNamedInClause(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "john", "john@id.com", "doom", 42})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(ctx, insertTmpl, args)
	noError(b, err)

	input := "SELECT * FROM benchmark WHERE id IN (:ids)"
	arg := map[string]any{"ids": []int{15, 732, 489, 256, 843, 127, 964,
		378, 591, 204, 876, 345, 689, 432, 517, 923, 671, 308, 754, 192,
		546, 819, 263, 947, 605, 134, 782, 421, 853, 397}}

	b.ResetTimer()
	for range b.N {
		var users []user
		err := db.Query(ctx, &users, input, arg)
		noError(b, err)
	}
}

func BenchmarkPlaceholderInClause(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "john", "john@id.com", "doom", 42})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(ctx, insertTmpl, args)
	noError(b, err)

	input := "SELECT * FROM benchmark WHERE id IN (?)"
	arg := []int{15, 732, 489, 256, 843, 127, 964,
		378, 591, 204, 876, 345, 689, 432, 517, 923, 671, 308, 754, 192,
		546, 819, 263, 947, 605, 134, 782, 421, 853, 397}

	b.ResetTimer()
	for range b.N {
		var users []user
		err := db.Query(ctx, &users, input, arg)
		noError(b, err)
	}
}

func BenchmarkCustomStructTag(b *testing.B) {
	pool, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	db := sqlz.New("sqlite3", pool, &sqlz.Options{StructTag: "json"})

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err = db.Exec(ctx, createTmpl)
	noError(b, err)

	type user struct {
		A int    `json:"id"`
		B string `json:"username"`
		C string `json:"email"`
		D string `json:"password"`
		E int    `json:"age"`
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "john", "john@id.com", "doom", 42})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(ctx, insertTmpl, args)
	noError(b, err)

	input := "SELECT * FROM benchmark WHERE id IN (?)"
	arg := []int{15, 732, 489, 256, 843, 127, 964,
		378, 591, 204, 876, 345, 689, 432, 517, 923, 671, 308, 754, 192,
		546, 819, 263, 947, 605, 134, 782, 421, 853, 397}

	b.ResetTimer()
	for range b.N {
		var users []user
		err := db.Query(ctx, &users, input, arg)
		noError(b, err)
	}
}
