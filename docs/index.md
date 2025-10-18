---
title: Guide Introduction
---

# Introduction

**sqlz** is a lightweight, dependency-free Go library that extends the standard [database/sql](https://pkg.go.dev/database/sql) package, adding support for named queries, struct scanning, and batch operations, while having a clean, minimal API.

It's designed to feel familiar to anyone using [database/sql](https://pkg.go.dev/database/sql), while removing repetitive boilerplate code. It can scan directly into structs, maps, or slices, and run named queries with full UTF-8/multilingual support.

If you're not familiar working with SQL in Go, I'd suggest taking a look at [the official tutorial](https://go.dev/doc/tutorial/database-access); and if you want to dive deeper, check out [go-database-sql.org](http://go-database-sql.org).

Note that **sqlz is not an ORM**.
It scans data from the database into Go objects, and converts object fields into query arguments, but it can't build database queries based on those objects.
It also doesn't know anything about relationships between objects.

## Features

- Named queries for structs and maps.
- Automatic scanning into primitives, structs, maps and slices.
- Automatic expanding "IN" clauses.
- Automatic expanding batch inserts.
- Automatic prepared statement caching.

## About this documentation

- For brevity, error handling is omitted when it's not relevant.
- Links on types/objects take you to the reference page on [pkg.go.dev](https://pkg.go.dev).
- Objects/methods without the package name, like `DB`, refer to the **sqlz** variant.

## Similar projects

**sqlz** was inspired by [sqlx](https://github.com/jmoiron/sqlx/) and [scany](https://github.com/georgysavva/scany/).
