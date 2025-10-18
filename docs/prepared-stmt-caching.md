# Prepared statement caching

By default, **sqlz** automatically caches prepared statements.
Under the hood, it uses an [LRU caching policy](https://en.wikipedia.org/wiki/Cache_replacement_policies#LRU), meaning it will always keep the most frequently used queries prepared.
Default capacity is 16, but it can be [customized](/custom-options).

Setting `StatementCacheCapacity: 0` completely disables this feature.

Finding the sweet spot for the caching capacity will depend on your application.
When increasing the capacity, database memory usage will also increase, while CPU usage will decrease.

Some databases limit the number of prepared statements; [MySQL](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html#sysvar_max_prepared_stmt_count) for instance has a default limit of 16382, while PostgreSQL has no fixed limit.

> [!IMPORTANT]
> Note that internally, each prepared statement is bound to a connection, but [database/sql](https://pkg.go.dev/database/sql) will prepare it on other connections automatically when needed.
> This means that, effectively, the cache capacity is per active connection, which is why the default capacity is conservative.

Limiting the [connection pool](/connection-pool) may have a large impact: statements will eventually be prepared across all active connections, making memory usage predictable.

For example, given a maximum of 16 connections and 16 cache capacity, the **maximum number** of cached statements would be 256.

Transactions have their own cache, and are cleared on `Commit()` or `Rollback()`.

> [!WARNING]
> Note that while having this feature active, database schema changes also require the cache to reset.
> You can just restart the application, or call `DB.ClearStmtCache()` to clear the cache.
