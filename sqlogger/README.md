# SQLogger

SQLogger is a logger for `database/sql`, it wrapps the underlying connection making it
very transparent, you're still using `sql.DB` object.
It's like a fork of [sqldb-logger](https://github.com/simukti/sqldb-logger)
with `slog` support and more opinionated.

## Getting started

> [!NOTE]
> If you're using it with `sqlz`, just use `sqlz.Options`.

### Connect to database

```go
// there's 2 ways of connecting:

// 1. using [sqlogger.Open]
db, err := sqlogger.Open("sqlite3", ":memory:", slog.Default(), nil)

// 2. using [sqlogger.New]
// instead of driver name, it expects the [driver.Driver]
db := sqlogger.New(&sqlite3.SQLiteDriver{}, ":memory:", slog.Default(), nil)
```
