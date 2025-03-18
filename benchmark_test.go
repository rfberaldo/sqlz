package sqlz_test

import (
	"testing"

	"github.com/rafaberaldo/sqlz"
	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

// goos: linux
// goarch: amd64
// pkg: github.com/rafaberaldo/sqlz
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkExec-12    	  312122	      3772 ns/op	     456 B/op	      15 allocs/op
func BenchmarkExec(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	input := "SELECT * FROM benchmark WHERE id = ? AND name = ?"
	args := []any{1, "Alice"}

	for range b.N {
		_, err := db.Exec(input, args...)
		assert.NoError(b, err)
	}
}

func BenchmarkQueryRowNamed(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	db.Exec("INSERT INTO benchmark (name) VALUES (?)", "Alice")

	input := "SELECT * FROM benchmark WHERE id = :id"
	arg := map[string]any{"id": 1}

	var user struct {
		Id   int
		Name string
	}

	for range b.N {
		err := db.QueryRow(&user, input, arg)
		assert.NoError(b, err)
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rafaberaldo/sqlz
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkBatchInsertStruct-12    	     801	   1495043 ns/op	 1179408 B/op	    6087 allocs/op
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
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "user123", "user@example.com", "abc123", 18})
	}
	input := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`

	for range b.N {
		_, err := db.Exec(input, args)
		assert.NoError(b, err)
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rafaberaldo/sqlz
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkStructScan-12    	     403	   4476157 ns/op	 1115058 B/op	   23726 allocs/op
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
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "user123", "user@example.com", "abc123", 18})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(insertTmpl, args)
	assert.NoError(b, err)

	input := "SELECT * FROM benchmark"

	for range b.N {
		var users []user
		err := db.Query(&users, input)
		assert.NoError(b, err)
	}
}

func BenchmarkQueryNativeScan(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL
		)`
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	type user struct {
		Id   int
		Name string
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "Alice"})
	}
	insertTmpl := `INSERT INTO benchmark (name)	VALUES (:name)`
	_, err = db.Exec(insertTmpl, args)
	assert.NoError(b, err)

	input := "SELECT name FROM benchmark"

	for range b.N {
		var names []string
		err := db.Query(&names, input)
		assert.NoError(b, err)
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rafaberaldo/sqlz
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkInClause-12    	   17372	     70964 ns/op	   13936 B/op	     357 allocs/op
func BenchmarkInClause(b *testing.B) {
	db := sqlz.MustConnect("sqlite3", ":memory:")

	createTmpl := `
		CREATE TABLE IF NOT EXISTS benchmark (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT,
			password TEXT,
			age INTEGER
		)`
	_, err := db.Exec(createTmpl)
	assert.NoError(b, err)

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{0, "user123", "user@example.com", "abc123", 18})
	}
	insertTmpl := `INSERT INTO benchmark (username, email, password, age)
		VALUES (:username, :email, :password, :age)`
	_, err = db.Exec(insertTmpl, args)
	assert.NoError(b, err)

	input := "SELECT * FROM benchmark WHERE id IN (:ids)"
	arg := map[string]any{"ids": []int{15, 732, 489, 256, 843, 127, 964,
		378, 591, 204, 876, 345, 689, 432, 517, 923, 671, 308, 754, 192,
		546, 819, 263, 947, 605, 134, 782, 421, 853, 397}}

	for range b.N {
		var users []user
		err := db.Query(&users, input, arg)
		assert.NoError(b, err)
	}
}
