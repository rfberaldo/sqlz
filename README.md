# sqlz

[![Tests Status](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml/badge.svg?branch=master)](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rfberaldo/sqlz)](https://goreportcard.com/report/github.com/rfberaldo/sqlz)
[![Go Reference](https://pkg.go.dev/badge/github.com/rfberaldo/sqlz.svg)](https://pkg.go.dev/github.com/rfberaldo/sqlz)

**sqlz** is a lightweight, dependency-free Go library that extends the standard [database/sql](https://pkg.go.dev/database/sql) package with named queries, scanning, and batch operations with a simple API.

## Getting started

### Install

```bash
go get github.com/rfberaldo/sqlz
```

### Connect to database

There are two ways to use it:

```go
// 1. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:")

// 2. using [sqlz.New] with a current connection
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, nil)
```

## Querying

**sqlz** has five main methods, they behave different depending on the args provided:

```go
Query(ctx context.Context, query string, args ...any) (*Scanner, error)
QueryRow(ctx context.Context, query string, args ...any) (*Scanner, error)
Select(ctx context.Context, dest any, query string, args ...any) error
Get(ctx context.Context, dest any, query string, args ...any) error
Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
```

### Query / QueryRow

They both query from database and returns a Scanner object. `QueryRow` query for one row.

```go
// struct or map args are treated as a named query
args := map[string]any{"country": "Brazil"}
scanner, err := db.Query(ctx, "SELECT name FROM user WHERE country = :country", args)
var names []string
err = scanner.Scan(&names)
```

```go
// otherwise it's treated as a native query, placeholder depends on the driver
scanner, err := db.QueryRow(ctx, "SELECT name FROM user WHERE id = ?", 42)
var name string
err = scanner.Scan(&name) // returns [sql.ErrNoRows] if not found
```

```go
// also works with 'IN' clause out of the box
args := []int{4, 8, 16}
scanner, err := db.Query(ctx, "SELECT * FROM user WHERE id IN (?)", args)
var users []User
err = scanner.Scan(&users)
```

### Select / Get

They both query from database and do the scanning. `Get` query for one row, and returns [sql.ErrNoRows](https://pkg.go.dev/database/sql#ErrNoRows) if not found.

```go
// struct or map args are treated as a named query
loc := Location{Country: "Brazil"}
var names []string
err := db.Select(ctx, &names, "SELECT name FROM user WHERE country = :country", loc)
```

```go
// otherwise it's treated as a native query, placeholder depends on the driver
var name string
err := db.Get(ctx, &name, "SELECT name FROM user WHERE id = ?", 42)
```

### Exec

```go
// struct or map args are treated as a named exec
args := map[string]any{"id": 42}
result, err := db.Exec(ctx, "DELETE FROM user WHERE id = :id", args)
```

```go
// array args are treated as a named batch insert
users := []User{
  {Id: 1, Name: "Alice", Email: "alice@example.com"},
  {Id: 2, Name: "Rob", Email: "rob@example.com"},
  {Id: 3, Name: "John", Email: "john@example.com"},
}
result, err := db.Exec(ctx, "INSERT INTO user (id, name, email) VALUES (:id, :name, :email)", users)
```

```go
// otherwise it's treated as a native exec, placeholder depends on the driver
result, err := db.Exec(ctx, "DELETE FROM user WHERE id = ?", 42)
```

### Transactions

Transactions have the same five main methods, other than that it's very similar to standard library.

```go
arg := map[string]any{"id": 42}

tx, err := db.Begin(ctx)

// Rollback will be ignored if tx has been committed later in the function,
// remember to return early if there is an error.
defer tx.Rollback()

_, err = tx.Exec(ctx, "DELETE FROM user_permission WHERE user_id = :id", arg)
_, err = tx.Exec(ctx, "DELETE FROM user WHERE id = :id", arg)

// Commit may fail, and nothing will have been committed.
err = tx.Commit()
```

### Struct fields

To find the key of a struct field, by default it first try to find the `db` tag,
if it's not present, it then transforms the field's name to snake case.

```go
type User struct {
  Id        int `db:"user_id"` // mapped as 'user_id'
  Name      string             // mapped as 'name'
  CreatedAt time.Time          // mapped as 'created_at'
}
```

### Custom options

Set custom options using the `New` constructor:

```go
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, &sqlz.Options{
  StructTag: "json",                     // default is "db"
  FieldNameTransformer: strings.ToLower, // default is ToSnakeCase
  IgnoreMissingFields: true,             // default is false
})
```

## Dependencies

**sqlz** has no dependencies, only testing/dev deps: [testify and db drivers](go.mod).

## Comparison with [sqlx](https://github.com/jmoiron/sqlx)

- It was designed with a simpler API for everyday use, with fewer concepts and less verbose.
- It supports non-english utf-8 characters in named queries.

### Performance

Take a look at [benchmarks](benchmarks) for more info.
