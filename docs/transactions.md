# Transactions

[Tx](https://pkg.go.dev/github.com/rfberaldo/sqlz#Tx) is returned by `Begin()` and `BeginTx()`.

> [!WARNING]
> Do not use `BEGIN` or `COMMIT` statements directly like `DB.Exec(ctx, "BEGIN")`. Because `DB` is a pool of connections, there's no guarantee that you will receive the same connection that the BEGIN statement was executed on.
> [Read more](https://go.dev/doc/database/execute-transactions).

Transactions have the same methods as `DB`, plus `Commit()` and `Rollback()`.
Usage is similar to the standard library:

```go
tx := db.Begin(ctx)

// Rollback will be ignored if tx has been committed later in the function
defer tx.Rollback()

tx.Exec(ctx, "DELETE FROM user_permission WHERE user_id = :id", user)
tx.Exec(ctx, "DELETE FROM user WHERE id = :id", user)

// Commit may fail, and nothing will have been committed
tx.Commit()
```

A [Tx](https://pkg.go.dev/github.com/rfberaldo/sqlz#Tx) will maintain a single connection for its entire life cycle, releasing it only when `Commit()` or `Rollback()` is called, so always call one of them to avoid leaking connections.

Because a transaction has only one connection, it can only execute one statement at a time.
If you attempt to send data to the server while it's sending you a result (from [Scanner](https://pkg.go.dev/github.com/rfberaldo/sqlz#Scanner) or [sql.Rows](https://pkg.go.dev/database/sql#Rows)), it can potentially corrupt the connection.
