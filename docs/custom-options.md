# Custom options

To set custom options, use the [Options](https://pkg.go.dev/github.com/rfberaldo/sqlz#Options) object with the `New()` constructor.

Shown values are defaults:

```go
pool, err := sql.Open("sqlite3", ":memory:")
db := sqlz.New("sqlite3", pool, &sqlz.Options{
  // StructTag is the reflection tag that will be used to map struct fields.
  StructTag: "db",

  // FieldNameTransformer transforms a struct field name
  // when the struct tag is not found.
  FieldNameTransformer: sqlz.ToSnakeCase,

  // IgnoreMissingFields causes the scanner to ignore missing struct fields
  // rather than returning an error.
  IgnoreMissingFields: false,

  // StatementCacheCapacity sets the maximum number of cached statements,
  // if it's zero, prepared statement caching is completely disabled.
  // Note that each statement may be prepared on each connection in the pool.
  StatementCacheCapacity 16,
})
```
