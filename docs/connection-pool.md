# Connection pool

Query execution requires a connection, and [sql.DB](https://pkg.go.dev/database/sql#DB) is a pool of connections: whenever you make a query, it grabs a connection, executes it, and returns it to the pool.
There are two ways to control the size of the connection pool:

::: code-group
```go [sqlz.DB]
DB.Pool().SetMaxOpenConns(n int)
DB.Pool().SetMaxIdleConns(n int)
```

```go [sql.DB]
DB.SetMaxOpenConns(n int)
DB.SetMaxIdleConns(n int)
```
:::

By default, the pool creates a new connection whenever needed if all existing connections are in use.
[sql.DB.SetMaxOpenConns](https://pkg.go.dev/database/sql#DB.SetMaxOpenConns) imposes a limit on the number of open connections. Past this limit, new database operations will wait for an existing operation to finish.

[sql.DB.SetMaxIdleConns](https://pkg.go.dev/database/sql#DB.SetMaxIdleConns) changes the limit on the maximum number of idle connections the pool maintains.
By default, it keeps two idle connections at any given moment. Increasing the limit can avoid frequent reconnects in programs with significant parallelism.

> [!TIP]
> Read more at the [official documentation](https://go.dev/doc/database/manage-connections).

To prevent accidentally holding connections, ensure every transaction returns its connection via `Commit()` or `Rollback()`;
and every scanner via `Scan()` or `Close()`.
