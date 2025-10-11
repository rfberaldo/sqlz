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

**sqlz** has three main methods:

```go
Query(ctx context.Context, query string, args ...any) *Scanner
QueryRow(ctx context.Context, query string, args ...any) *Scanner
Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
```

> [!NOTE]
> Error handling is omitted for brevity of these examples.

### Query / QueryRow

They both query from database and returns a Scanner object that can automatically scan rows into structs, maps, slices, etc. `QueryRow` query for one row.
Errors are deferred to scanner for easy chaining, until `Err` or `Scan` is called.

```go
var name string
db.QueryRow(ctx, "SELECT name FROM user WHERE id = ?", 42).Scan(&name)
```

```go
// struct or map args are treated as a named query
loc := Location{Country: "Brazil"}
var names []string
db.Query(ctx, "SELECT name FROM user WHERE country = :country", loc).Scan(&names)
```

```go
// also works with 'IN' clause out of the box
args := []int{4, 8, 16}
var users []User
db.Query(ctx, "SELECT * FROM user WHERE id IN (?)", args).Scan(&users)
// executed as:
// SELECT * FROM user WHERE id IN (?,?,?)
```

### Exec

Exec is very similar to standard library, with added support for named queries and batch inserts.

```go
// named query
args := map[string]any{"id": 42}
db.Exec(ctx, "DELETE FROM user WHERE id = :id", args)
```

```go
// slice arg is treated as a named batch insert
users := []User{
  {Id: 1, Name: "Alice", Email: "alice@example.com"},
  {Id: 2, Name: "Rob", Email: "rob@example.com"},
  {Id: 3, Name: "John", Email: "john@example.com"},
}
query := "INSERT INTO user (id, name, email) VALUES (:id, :name, :email)"
db.Exec(ctx, query, users)
// executed as:
// INSERT INTO user (id, name, email) VALUES (?,?,?), (?,?,?), (?,?,?)
```

### Transactions

Transactions have the methods, and it's also similar to standard library.

```go
tx := db.Begin(ctx)

// Rollback will be ignored if tx has been committed later in the function
defer tx.Rollback()

user := User{Id: 42}
tx.Exec(ctx, "DELETE FROM user_permission WHERE user_id = :id", user)
tx.Exec(ctx, "DELETE FROM user WHERE id = :id", user)

// Commit may fail, and nothing will have been committed
tx.Commit()
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

## Dependencies

**sqlz** has no dependencies, only [testing/dev deps](go.mod).

## Comparison with [sqlx](https://github.com/jmoiron/sqlx)

- It was designed with a simpler API for everyday use, with fewer concepts and less verbose.
- It supports non-english utf-8 characters in named queries.

### Performance

Take a look at [benchmarks](benchmarks) for more info.
