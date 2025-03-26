# sqlz

[![Tests Status](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml/badge.svg?branch=master)](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rfberaldo/sqlz)](https://goreportcard.com/report/github.com/rfberaldo/sqlz)
[![Go Reference](https://pkg.go.dev/badge/github.com/rfberaldo/sqlz.svg)](https://pkg.go.dev/github.com/rfberaldo/sqlz)

sqlz is a thin wrapper around standard library's `database/sql`, specifically `sql.DB` and `sql.Tx`.
It aims to be lightweight and easy to use, with a much smaller API. Scanning is powered by [scany](https://github.com/georgysavva/scany).

## Getting started

### Install

```bash
go get github.com/rfberaldo/sqlz
```

### Connect to database

```go
// there's 2 ways of connecting:

// 1. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:", nil)

// 2. using [sql.Open] and [sqlz.New]
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, nil)
```

## Querying

sqlz has only three main methods, they will behave different depending on the args provided:

```go
Query(ctx context.Context, dst any, query string, args ...any) error
QueryRow(ctx context.Context, dst any, query string, args ...any) error
Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
```

### Query / QueryRow

They both query from database and do the scanning.
`QueryRow` only return one record, and return error if not found.

```go
// struct or map args are treated as a named query
args := map[string]any{"min_age": 18}
var names []int
err := db.Query(ctx, &names, "SELECT name FROM user WHERE age >= :min_age", args)
```

```go
// otherwise it's treated as a native query, placeholder depends on the driver
var name string
err := db.QueryRow(ctx, &name, "SELECT name FROM user WHERE id = ?", 42)
```

```go
// also works with 'IN' clause out of the box
args := []int{4, 8, 16}
var users []struct{Id int; Name string; Age int}
err := db.Query(ctx, &users, "SELECT * FROM user WHERE id IN (?)", args)
```

### Exec

```go
// struct or map args are treated as a named exec
args := map[string]any{"id": 42}
result, err := db.Exec(ctx, "DELETE FROM user WHERE id = :id", args)
```

```go
// array args are treated as a named batch insert
users := []struct{Id int; Name string; Age int}{
  {1, "Alice", 32},
  {2, "Rob", 64},
  {3, "John", 42},
}
result, err := db.Exec(ctx, "INSERT INTO user (id, name, age) VALUES (:id, :name, :age)", users)
```

```go
// otherwise it's treated as a native exec, placeholder depends on the driver
result, err := db.Exec(ctx, "DELETE FROM user WHERE id = ?", 42)
```

### Transactions

Transactions have the same three main methods, other than that it's very similar to standard library.

```go
// error handling is omitted for brevity of this example
arg := map[string]any{"id": 42}

tx, err := db.Begin(ctx)

// Rollback will be ignored if tx has been committed later in the function
// remember to return early if there is an error
defer tx.Rollback()

_, err = tx.Exec(ctx, "DELETE FROM user WHERE id = :id", arg)
_, err = tx.Exec(ctx, "DELETE FROM user_permission WHERE user_id = :id", arg)

// Commit may fail, and nothing will have been committed
err = tx.Commit()
```

### Struct tags

To find the key of a struct property, sqlz first try to find the `db` tag,
if it's not present, it then converts the property name to snake case.
To set a custom tag use [Options](#options).

```go
type User struct {
  Id        int `db:"user_id"` // mapped as 'user_id'
  Name      string             // mapped as 'name'
  CreatedAt time.Time          // mapped as 'created_at'
}
```

### Options

sqlz has a few options, use `sqlz.Options` as third parameter of `New` or `Connect`.
At the moment it's possible to change the default struct tag, and set a logger using `slog`.

```go
// 1. using [sqlz.New]
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, &sqlz.Options{StructTag: "json"})

// 2. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:", &sqlz.Options{
  StructTag: "json",
  Logger: slog.Default(),
})
```

## Dependencies

The only dependency of sqlz is [scany](https://github.com/georgysavva/scany).
All the others listed in [go.mod](go.mod) are testing/dev dependencies.

## Comparison with [sqlx](https://github.com/jmoiron/sqlx)

- sqlz has a smaller scope, it doesn't support prepared statements and all the
scanning work is done by [scany](https://github.com/georgysavva/scany).
- It was designed with a simpler API for everyday use, with fewer concepts and less verbose.
- It supports non-english utf-8 characters in named queries.

### Performance

Take a look at [benchmarks](benchmarks) for more info.
