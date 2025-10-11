package sqlz_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rfberaldo/sqlz"
)

var (
	db  *sqlz.DB
	ctx = context.Background()
)

func ExampleNew() {
	pool, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	db := sqlz.New("sqlite3", pool, nil)

	_, err = db.Exec(ctx, "CREATE TABLE user (id INT PRIMARY KEY, name TEXT")
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNew_options() {
	pool, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	// use sqlz.Options as third parameter
	db := sqlz.New("sqlite3", pool, &sqlz.Options{
		StructTag:            "json",
		FieldNameTransformer: strings.ToLower,
		IgnoreMissingFields:  true,
	})

	_, err = db.Exec(ctx, "CREATE TABLE user (id INT PRIMARY KEY, name TEXT")
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_Query() {
	var names []string
	err := db.Query(ctx, "SELECT name FROM user WHERE age > ?", 27).Scan(&names)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v", names)
}

func ExampleDB_Query_named() {
	type Params struct {
		Age int
	}

	var names []string
	params := Params{Age: 27} // or map[string]any{"age": 27}
	err := db.
		Query(ctx, "SELECT name FROM user WHERE age > :age", params).
		Scan(&names)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v", names)
}

func ExampleDB_Query_in_clause() {
	var names []string
	ages := []int{27, 28, 29}
	err := db.
		Query(ctx, "SELECT name FROM user WHERE age IN (?)", ages).
		Scan(&names)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v", names)
}

func ExampleDB_Query_named_in_clause() {
	type Params struct {
		Ages []int
	}

	var names []string
	params := Params{Ages: []int{27, 28, 29}}
	err := db.
		Query(ctx, "SELECT name FROM user WHERE age IN (:ages)", params).
		Scan(&names)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v", names)
}

func ExampleDB_QueryRow() {
	type User struct {
		Username  string
		CreatedAt time.Time
	}
	id := 42
	var user User
	err := db.
		QueryRow(ctx, "SELECT username, created_at FROM user WHERE id = ?", id).
		Scan(&user)
	switch {
	case sqlz.IsNotFound(err):
		log.Printf("no user with id %d\n", id)
	case err != nil:
		log.Fatalf("query error: %v\n", err)
	default:
		log.Printf("username is %q, account created on %s\n", user.Username, user.CreatedAt)
	}
}

func ExampleDB_Exec() {
	id := 42
	result, err := db.Exec(ctx, "UPDATE balances SET balance = balance + 10 WHERE user_id = ?", id)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	if rows != 1 {
		log.Fatalf("expected to affect 1 row, affected %d", rows)
	}
}

func ExampleDB_Exec_batch_insert() {
	type User struct {
		Username  string
		CreatedAt time.Time
	}

	users := []User{
		{"john", time.Now()},
		{"alice", time.Now()},
		{"rob", time.Now()},
		{"brian", time.Now()},
	}

	_, err := db.Exec(ctx, "INSERT INTO user (username, created_at) VALUES (:username, :created_at)", users)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_Begin() {
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Rollback will be ignored if tx has been committed later in the function,
	// remember to return early if there is an error.
	defer tx.Rollback()

	args := map[string]any{"status": "paid", "id": 37}
	_, err = tx.Exec(ctx, "UPDATE user SET status = :status WHERE id = :id", args)
	if err != nil {
		log.Fatal(err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("unable to commit: %v", err)
	}
}

func ExampleDB_BeginTx() {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Fatal(err)
	}

	// Rollback will be ignored if tx has been committed later in the function,
	// remember to return early if there is an error.
	defer tx.Rollback()

	args := map[string]any{"status": "paid", "id": 37}
	_, err = tx.Exec(ctx, "UPDATE user SET status = :status WHERE id = :id", args)
	if err != nil {
		log.Fatal(err)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("unable to commit: %v", err)
	}
}

func ExampleDB_Pool() {
	db.Pool().SetMaxOpenConns(10)
	db.Pool().SetMaxIdleConns(4)
}
