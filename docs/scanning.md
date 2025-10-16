---
outline: [2,3]
---

# Scanning

**sqlz** can automatically scan query rows into primitives, structs, maps and slices.

[Scanner](https://pkg.go.dev/github.com/rfberaldo/sqlz#Scanner) is returned by both `Query()` and `QueryRow()`, and it's similar to [sql.Rows](https://pkg.go.dev/database/sql#Rows).
Query errors are deferred to the scanner, making it easy to chain methods.

> [!IMPORTANT]
> 1. Scanner behaves differently depending on whether it was called from `Query()` or `QueryRow()`.
> 2. Scanner will not empty the slice before scanning, previous data will be kept.
> 3. Scanner holds the connection until `Scan()` or `Close()` is called, so always call one of them to avoid leaking connections.

## Query Scanner

### Automatic

`Scan()` automatically iterates over rows and scans into destination; it expects a slice as destination.
If the query results are empty, the slice remains unchanged and no error is returned.

```go
var users []User
err := db.Query(ctx, "SELECT * FROM user").Scan(&users)
...
// users variable now contains data from query
```

### Manual

`ScanRow()` and `NextRow()` give you more control over the scanning, especially useful when you want to avoid allocating an entire slice.
For example, if you only need a single row from the table at a time:

```go
// logs might have millions of rows
scanner := db.Query(ctx, "SELECT * FROM logs")

// check for deferred query error
err := scanner.Err()
...

defer scanner.Close()
for scanner.NextRow() {
  var log Log
  err = scanner.ScanRow(&log)
  ...
  processLog(log)
}

// loop might exit for some reason other than EOF,
// so always check whether the loop terminated correctly or not
err = scanner.Err()
...
```

`Err()` returns the deferred error from the query, or the error during `NextRow()`.

## QueryRow Scanner

`Scan()` automatically iterates over rows and scans at most one row into destination.
If the query results are empty, it returns [sql.ErrNoRows](https://pkg.go.dev/database/sql#ErrNoRows).

```go
var user User
err = db.QueryRow(ctx, "SELECT * FROM user WHERE id = ?", 42).Scan(&user)
if err != nil {
  if sqlz.IsNotFound(err) {
    log.Fatal("user not found!")
  }
  log.Fatal(err)
}
```

## Struct scanning

Scanning into a struct is straightforward, but there are a few details to keep in mind.
Under the hood, **sqlz** traverses the struct tree using a [BFS algorithm](https://en.wikipedia.org/wiki/Breadth-first_search) and caches the field mapping for faster slice scanning.

> [!IMPORTANT]
> If a struct implements the [sql.Scanner](https://pkg.go.dev/database/sql#Scanner) interface, **sqlz will not** perform field mapping.

### Field key

To get the key of a struct field, it first tries to find the **"db"** tag;
if it's not present, it then transforms the field name to snake case.

```go
type User struct {
  Id        int `db:"user_id"` // mapped as "user_id"
  Name      string             // mapped as "name"
  CreatedAt time.Time          // mapped as "created_at"
}
```

> [!TIP]
> - Note that the fields must be exported/public in order for **sqlz** to access them, just like [json.Marshal](https://pkg.go.dev/encoding/json#Marshal), and any other marshaler in Go.
> - It's possible to [customize](/custom-options) the default struct tag and/or the transformation function.

### Nested structs

Embedding, nesting, and circular references (up to 10 levels) are supported.
If a nested struct is nil, it will initialize the pointer before scanning into it.

Nested structs are mapped with the struct name as prefix, for example:

```go
type Category struct {
  Id   int
  Name string
}

type Product struct {
  Id       int
  Name     string
  Category Category
}

Product.Id            // mapped as 'id'
Product.Name          // mapped as 'name'
Product.Category.Id   // mapped as 'category_id'
Product.Category.Name // mapped as 'category_name'
```

> [!NOTE]
> - Embedded fields are not prefixed.
> - When mapping from database, separator is an underscore.
> - When mapping from named query, separator is a dot.

If for some reason this behavior is not desired, add the `inline` option to it:

```go
type Category struct {
  Id   int    `db:"cat_id"`
  Name string `db:"cat_name"`
}

type Product struct {
  Id       int
  Name     string
  Category Category `db:",inline"`
}

Product.Id            // mapped as 'id'
Product.Name          // mapped as 'name'
Product.Category.Id   // mapped as 'cat_id'
Product.Category.Name // mapped as 'cat_name'
```

If there are multiple fields with the same name, it will map the shallowest, top-most definition:

```go
type Category struct {
  Id   int
  Name string
}

type Product struct {
  Id       int
  Name     string
  Category Category `db:",inline"`
}

Product.Id            // mapped as 'id'
Product.Name          // mapped as 'name'
Product.Category.Id   // not mapped
Product.Category.Name // not mapped
```
