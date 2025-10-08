package sqlz

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/rfberaldo/sqlz/parser"
)

// Options are optional configs for sqlz.
type Options struct {
	// StructTag is the reflection tag that will be used to map struct fields.
	StructTag string

	// FieldNameTransformer transforms a struct field name,
	// it is only used when the struct tag is not found.
	FieldNameTransformer func(string) string

	// IgnoreMissingFields makes scanner ignore missing struct fields instead of returning error.
	IgnoreMissingFields bool
}

// New returns a [DB] instance using an existing [sql.DB].
// If driverName is not registered in [binds], it panics.
//
// The opts parameter can be set to nil for defaults.
//
// Example:
//
//	pool, err := sql.Open("sqlite3", ":memory:")
//	db := sqlz.New("sqlite3", pool, nil)
func New(driverName string, db *sql.DB, opts *Options) *DB {
	bind := BindByDriver(driverName)
	if bind == parser.BindUnknown {
		panic(fmt.Errorf("sqlz: unable to find bind for '%s', register with [sqlz.Register]", driverName))
	}

	if opts == nil {
		opts = &Options{}
	}

	cfg := &config{
		bind:                 bind,
		structTag:            opts.StructTag,
		fieldNameTransformer: opts.FieldNameTransformer,
		ignoreMissingFields:  opts.IgnoreMissingFields,
	}
	cfg.defaults()

	return &DB{db, &base{cfg}}
}

// Connect opens a database specified by its database driver name and a
// driver-specific data source name, then verify the connection with a ping.
// If driverName is not registered in [binds], it panics.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Connect
// function should be called just once.
func Connect(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sqlz: unable to open sql connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlz: unable to ping connection: %w", err)
	}

	return New(driverName, db, nil), nil
}

// MustConnect is like [Connect], but panics on error.
func MustConnect(driverName, dataSourceName string) *DB {
	db, err := Connect(driverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

// IsNotFound is a helper to check if err contains [sql.ErrNoRows].
func IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
