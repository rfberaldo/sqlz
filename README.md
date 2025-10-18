# sqlz

[![Test Status](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml/badge.svg)](https://github.com/rfberaldo/sqlz/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/rfberaldo/sqlz)](https://goreportcard.com/report/github.com/rfberaldo/sqlz)
[![Codecov](https://codecov.io/github/rfberaldo/sqlz/graph/badge.svg?token=RQI8TCN1IO)](https://codecov.io/github/rfberaldo/sqlz)
[![Go Reference](https://pkg.go.dev/badge/github.com/rfberaldo/sqlz.svg)](https://pkg.go.dev/github.com/rfberaldo/sqlz)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

**sqlz** is a lightweight, dependency-free Go library that extends the standard [database/sql](https://pkg.go.dev/database/sql) package, adding support for named queries, struct scanning, and batch operations, while having a clean, minimal API.

It's designed to feel familiar to anyone using [database/sql](https://pkg.go.dev/database/sql), while removing repetitive boilerplate code. It can scan directly into structs, maps, or slices, and run named queries with full UTF-8/multilingual support.

> Documentation: https://rfberaldo.github.io/sqlz/.

## Features

- Named queries for structs and maps.
- Automatic scanning into primitives, structs, maps and slices.
- Automatic expanding "IN" clauses.
- Automatic expanding batch inserts.
- Automatic prepared statement caching.

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
> For brevity of the examples, error handling is omitted.

### Standard query

```go
var users []User
db.Query(ctx, "SELECT * FROM user WHERE active = ?", true).Scan(&users)
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
- It has full support for UTF-8/multilingual named queries.
- It's more performant in most cases, take a look at the [benchmarks](benchmarks) for comparison.
