---
title: Guide Introduction
---

# Introduction

**sqlz** is a lightweight Go library that builds on top of the standard [database/sql](https://pkg.go.dev/database/sql) package, adding first-class support for named queries, struct scanning, and batch operations, while having a clean, minimal API and zero external dependencies.

It's designed to feel familiar to anyone using [database/sql](https://pkg.go.dev/database/sql), while removing repetitive boilerplate. It can scan directly into structs, maps, or slices, run named queries with native UTF-8 support, and handle transactions just like in the standard library.

If you're not familiar working with SQL in Go, I suggest taking a look at [the official tutorial](https://go.dev/doc/tutorial/database-access), and if you want to dive deeper: [go-database-sql.org](http://go-database-sql.org).

**sqlz** is not an ORM.
It scans data into Go objects from the database, and data from objects into query arguments, but it can't build database queries based on those objects.
Also, it doesn't know anything about relationship between objects.

## Features

- Named queries for structs and maps.
- Auto scanning into primitives, structs, maps and slices.
- Auto expanding "IN" clauses and batch insert.
- Customizable.
- Performant.

## About this documentation

- Throughout this documentation, error handling is omitted when not relevant, for brevity of the examples.
- Link on types/objects sends you to the reference page on [pkg.go.dev](https://pkg.go.dev).
- Objects/methods without the package name, like `DB`, refers to the **sqlz variant**.

## Similar projects

**sqlz** was inspired by [sqlx](https://github.com/jmoiron/sqlx/) and [scanny](https://github.com/georgysavva/scany/).
