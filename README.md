# sqlz

sqlz is a thin wrapper around standard lib's `database/sql`, specifically `sql.DB` and `sql.Tx`.
It aims to be lightweight and easy to use, with a much smaller API. Scanning is powered by [scany](https://github.com/georgysavva/scany).

## Getting started

### Install

```bash
go get github.com/rafaberaldo/sqlz
```

### Connect to database

```go
// there's 2 ways of connecting:

// 1. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:")

// 2. using [sql.Open] and [sqlz.New]
sqldb, err := sql.Open("sqlite3", ":memory:")
...
db, err := sqlz.New("sqlite3", sqldb)
```

## Querying

sqlz has only three major methods, each having its "with context" counterpart:

```go
Query(dst any, query string, args ...any) error
QueryRow(dst any, query string, args ...any) error
Exec(query string, args ...any) (sql.Result, error)
```

These methods will behave different depending on the args provided.

### Query / QueryRow

They both query from database and do the scanning.
The only difference from them is that `QueryRow` only returns one record.

```go
// struct or map args are treated as a named query
var names []int
args := map[string]any{"min_age": 18}
err := db.Query(&names, "SELECT name FROM user WHERE age >= :min_age", args)
```

```go
// otherwise it's treated as a native query, placeholder depends on the driver
var name string
err := db.QueryRow(&name, "SELECT name FROM user WHERE id = ?", 42)
```

```go
// also works with `IN` clause out of the box
var users []struct{Id int; Name string; Age int}
args := []int{4, 8, 16}
err := db.Query(&users, "SELECT * FROM user WHERE id IN (?)", args)
```

### Exec

```go
// struct or map args are treated as a named exec
args := map[string]any{"id": 42}
result, err := db.Exec("DELETE FROM user WHERE id = :id", args)
```

```go
// array args are treated as a named batch insert
users := []struct{Id int; Name string; Age int}{
  {1, "Alice", 32},
  {2, "Rob", 64},
  {3, "John", 42},
}
result, err := db.Exec("INSERT INTO user (id, name, age) VALUES (:id, :name, :age)", users)
```

```go
// otherwise it's treated as a native exec, placeholder depends on the driver
result, err := db.Exec("DELETE FROM user WHERE id = ?", 42)
```

### Transactions

Transactions have the same three major methods.

```go
arg := map[string]any{"id": 42}

// very similar to standard library
tx, err := db.Begin()
...
defer tx.Rollback() // defer works if you return earlier than Commit
_, err := tx.Exec("DELETE FROM user WHERE id = :id", arg)
...
_, err := tx.Exec("DELETE FROM user_permission WHERE user_id = :id", arg)
...
tx.Commit()
```

### Using custom struct tag for named queries

```go
// e.g you have all your structs like this
type User struct {
  Identifier int `json:"id"`
  Name       int `json:"name"`
  Password   int `json:"pw"`
}

// just set a custom struct tag
db.SetStructTag("json")
```

## Comparison with [sqlx](https://github.com/jmoiron/sqlx)

- sqlz has a smaller scope, it doesn't support prepared statements.
- It was designed with a simpler API for everyday use.
- All the scanning work is done by [scany](https://github.com/georgysavva/scany).

### Performance

TODO
