# SQLogger

SQLogger is a logger for `database/sql`, it wrapps the underlying connection making it very transparent, it returns a standard `sql.DB` object.
It's like a fork of [sqldb-logger](https://github.com/simukti/sqldb-logger) with `slog` support and more opinionated.

## Getting started

> [!NOTE]
> If you're using it with `sqlz`, just use `sqlz.Options`.

### Install

```bash
go get github.com/rfberaldo/sqlz/sqlogger
```

### Open a database

```go
// there's 2 ways of opening a database:

// 1. using [sqlogger.Open]
db, err := sqlogger.Open("sqlite3", ":memory:", slog.Default(), nil)

// 2. using [sqlogger.New]
// instead of driver name, it expects the [driver.Driver]
db := sqlogger.New(&sqlite3.SQLiteDriver{}, ":memory:", slog.Default(), nil)
```

## Options

Use `sqlogger.Options` as fourth parameter of `New` or `Open`:

```go
db, err := sqlogger.Open("sqlite3", ":memory:", slog.Default(), &sqlogger.Options{
  // options...
})
```
