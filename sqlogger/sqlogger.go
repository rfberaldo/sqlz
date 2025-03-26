package sqlogger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log/slog"
	"math/rand"
	"time"
)

const (
	txKey   = "tx_id"
	connKey = "conn_id"
	stmtKey = "stmt_id"
)

// Logger is an instance of [slog.Logger]
type Logger interface {
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

// Options holds logging options to be used in [Open] or [New].
// A zero Options consists entirely of default values.
type Options struct {
	// IdGenerator is a function that returns a string to be used as id.
	// By default it's a 8-length random string.
	IdGenerator func() string
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// Open may just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// [DB.Ping].
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Open
// function should be called just once.
//
// The logger argument is an instance of [slog.Logger].
//
// If opts is nil, the default options are used.
func Open(driverName, dataSourceName string, logger Logger, opts *Options) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return New(db.Driver(), dataSourceName, logger, opts), nil
}

// New opens a database specified by its database driver and a
// driver-specific data source name.
//
// No database drivers are included in the Go standard library.
// See https://golang.org/s/sqldrivers for a list of third-party drivers.
//
// New may just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// [DB.Ping].
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the New
// function should be called just once.
//
// The logger argument is an instance of [slog.Logger].
//
// If opts is nil, the default options are used.
func New(driver driver.Driver, dataSourceName string, logger Logger, opts *Options) *sql.DB {
	conn := &connector{
		dsn:    dataSourceName,
		driver: driver,
		logger: &sqlogger{logger, randomId},
	}

	if opts == nil {
		return sql.OpenDB(conn)
	}

	if opts.IdGenerator != nil {
		conn.logger.IdGenerator = opts.IdGenerator
	}

	return sql.OpenDB(conn)
}

type sqlogger struct {
	logger      Logger
	IdGenerator func() string
}

func (l *sqlogger) log(
	ctx context.Context,
	level slog.Level,
	msg string,
	start time.Time,
	err error,
	attrs ...slog.Attr,
) {
	l.logger.LogAttrs(ctx, level, msg, buildAttrs(start, err, attrs...)...)
}

func buildAttrs(start time.Time, err error, attrs ...slog.Attr) []slog.Attr {
	_attrs := make([]slog.Attr, 0, len(attrs)+2)
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	_attrs = append(_attrs, attrs...)
	_attrs = append(_attrs, slog.Duration("duration", time.Since(start)))
	return _attrs
}

func valuesFromNamedArgs(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(args))

	for k, v := range args {
		values[k] = v.Value
	}

	return values
}

// randomId generates a string with 8 random characters.
func randomId() string {
	const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 8)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
