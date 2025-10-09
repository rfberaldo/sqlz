# sqlz

[![Tests Status](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml/badge.svg?branch=master)](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rfberaldo/sqlz)](https://goreportcard.com/report/github.com/rfberaldo/sqlz)
[![Go Reference](https://pkg.go.dev/badge/github.com/rfberaldo/sqlz.svg)](https://pkg.go.dev/github.com/rfberaldo/sqlz)

**sqlz** is a lightweight, dependency-free Go library that extends the standard [database/sql](https://pkg.go.dev/database/sql) package with named queries, scanning, and batch operations with a simple API.

> Guide documentation: https://rfberaldo.github.io/sqlz/.

## Getting started

### Install

```bash
go get github.com/rfberaldo/sqlz
```

### Setup

There are two ways to use it:

```go
// 1. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:")

// 2. using [sqlz.New] with a current connection
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, nil)
```

## Examples

> [!NOTE]
> Error handling is omitted for brevity of the examples.

### Standard query

```go
var users []User
db.Query(ctx, "SELECT * FROM user WHERE active = ?", true).Scan(&name)
// users variable now contains data from query
```

### Named query

```go
loc := Location{Country: "Brazil"}
var users []User
db.Query(ctx, "SELECT * FROM user WHERE country = :country", loc).Scan(&users)
// users variable now contains data from query
```

### Exec

```go
user := User{Name: "Alice", Email: "alice@wonderland.com"}
db.Exec(ctx, "INSERT INTO user (name, email) VALUES (:name, :email)", user)
```

### Batch insert

```go
users := []User{
  {Name: "Alice", Email: "alice@example.com"},
  {Name: "Rob", Email: "rob@example.com"},
  {Name: "John", Email: "john@example.com"},
}
db.Exec(ctx, "INSERT INTO user (name, email) VALUES (:name, :email)", users)
// executed as "INSERT INTO user (name, email) VALUES (?, ?), (?, ?), (?, ?)"
```

## Dependencies

**sqlz** has no dependencies, only [testing/dev deps](go.mod).

## Comparison with [sqlx](https://github.com/jmoiron/sqlx)

- It was designed with a simpler API for everyday use, with fewer concepts and less verbose.
- It has full support for UTF-8 in named queries.

### Performance

Take a look at [benchmarks](benchmarks) for more info.
