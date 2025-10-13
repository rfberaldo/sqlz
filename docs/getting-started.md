# Getting started

## Installation

```bash
go get github.com/rfberaldo/sqlz
```

## Setup

There are two ways to use it:

```go
// 1. using [sqlz.Connect]
db, err := sqlz.Connect("sqlite3", ":memory:")

// 2. using [sqlz.New] with a current connection
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, nil)
```

No database drivers are included in the Go standard library or sqlz.
See https://go.dev/wiki/SQLDrivers for a list of third-party drivers.
The returned [DB](https://pkg.go.dev/github.com/rfberaldo/sqlz#DB) object is safe for concurrent use by multiple goroutines and maintains its own pool of idle connections.
Typically, you start a connection once and don't have to close it.

## Examples

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
