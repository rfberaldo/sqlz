package sqlz

import (
	"cmp"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rfberaldo/sqlz/internal/binds"
)

const defaultStructTag = "db"

// Options are optional configs for sqlz.
type Options struct {
	StructTag string
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
	bind := binds.BindByDriver(driverName)
	if bind == binds.Unknown {
		panic(fmt.Sprintf("sqlz: unable to find bind for %q, register with [sqlz.Register]", driverName))
	}

	structTag := defaultStructTag
	if opts != nil {
		structTag = cmp.Or(opts.StructTag, defaultStructTag)
	}

	return &DB{db, bind, structTag}
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
		return nil, fmt.Errorf("sqlz: unable to ping: %w", err)
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

const (
	BindAt       = binds.At
	BindColon    = binds.Colon
	BindDollar   = binds.Dollar
	BindQuestion = binds.Question
)

// Register adds a new driver name and its bind to be
// available to [New] and [Connect]. If name is empty, it panics.
func Register(name string, bind binds.Bind) {
	binds.Register(name, binds.Bind(bind))
}
