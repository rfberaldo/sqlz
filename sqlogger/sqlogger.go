package sqlogger

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"time"
)

const (
	txKey       = "tx_id"
	connKey     = "conn_id"
	stmtKey     = "stmt_id"
	queryKey    = "query"
	errorKey    = "error"
	argsKey     = "args"
	durationKey = "duration"
)

// Logger is an instance of [slog.Logger]
type Logger interface {
	LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr)
}

// Options holds logging options to be used in [Open] or [New].
// A zero Options consists entirely of default values.
type Options struct {
	// IdGenerator is a function that returns a string to be used as id.
	// Default: 6-length random string.
	IdGenerator func() string

	// CleanQuery removes any redundant whitespace before logging.
	// Default: false.
	CleanQuery bool
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
		logger: &sqlogger{logger, randomId, false},
	}

	if opts == nil {
		return sql.OpenDB(conn)
	}

	if opts.IdGenerator != nil {
		conn.logger.idGenerator = opts.IdGenerator
	}

	conn.logger.cleanQuery = opts.CleanQuery

	return sql.OpenDB(conn)
}

type sqlogger struct {
	logger      Logger
	idGenerator func() string
	cleanQuery  bool
}

func (l *sqlogger) log(
	ctx context.Context,
	level slog.Level,
	msg string,
	start time.Time,
	err error,
	attrs ...slog.Attr,
) {
	if errors.Is(err, driver.ErrSkip) {
		return
	}

	l.logger.LogAttrs(ctx, level, msg, l.buildAttrs(start, err, attrs...)...)
}

var attrPriorityByKey = map[string]int{
	errorKey:    0,
	queryKey:    1,
	argsKey:     2,
	connKey:     3,
	stmtKey:     4,
	txKey:       5,
	durationKey: 6,
}

func (l *sqlogger) buildAttrs(start time.Time, err error, attrs ...slog.Attr) []slog.Attr {
	_attrs := make([]slog.Attr, 0, len(attrs)+2)

	if err != nil {
		attrs = append(attrs, slog.Any(errorKey, err))
	}
	_attrs = append(_attrs, slog.Duration(durationKey, time.Since(start)))

	for _, attr := range attrs {
		if l.cleanQuery && attr.Key == queryKey {
			attr.Value = slog.StringValue(cleanQuery(attr.Value.String()))
			_attrs = append(_attrs, attr)
			continue
		}

		if attr.Key == argsKey && attr.Value.String() == "[]" {
			continue
		}

		_attrs = append(_attrs, attr)
	}

	slices.SortFunc(_attrs, func(a, b slog.Attr) int {
		return cmp.Compare(attrPriorityByKey[a.Key], attrPriorityByKey[b.Key])
	})

	return _attrs
}

func cleanQuery(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func valuesFromNamedArgs(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(args))

	for k, v := range args {
		values[k] = v.Value
	}

	return values
}

// randomId generates a string with 6 random characters.
func randomId() string {
	const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 6)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
