package sqlz

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/rafaberaldo/sqlz/binder"
)

// New returns a [*DB] instance for a pre-existing [*sql.DB].
//
// Example:
//
//	conn, err := sql.Open("mysql", dsn)
//	db := sqlz.New("mysql", conn)
func New(driverName string, db *sql.DB) (*DB, error) {
	bind := binder.BindByDriver(driverName)
	if bind == binder.Unknown {
		return nil, fmt.Errorf("sqlz: unable to find bind for driver '%v', register using [binder.Register]", driverName)
	}

	return &DB{bind, db}, nil
}

// Connect opens a database specified by its database driver name and a
// driver-specific data source name, then verify the connection with a Ping.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// The returned [*DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Connect
// function should be called just once.
func Connect(driverName, dataSourceName string) (*DB, error) {
	bind := binder.BindByDriver(driverName)
	if bind == binder.Unknown {
		return nil, fmt.Errorf("sqlz: unable to find bind for driver '%v', register using [binder.Register]", driverName)
	}

	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("sqlz: unable to open sql connection: %w", err)
	}

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlz: unable to ping: %w", err)
	}

	return &DB{bind, db}, nil
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
