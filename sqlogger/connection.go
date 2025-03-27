package sqlogger

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

// connection implements
// [driver.Conn]
// [driver.ConnBeginTx]
// [driver.ConnPrepareContext]
// [driver.Pinger]
// [driver.Execer]
// [driver.ExecerContext]
// [driver.Queryer]
// [driver.QueryerContext]
// [driver.SessionResetter]
// [driver.NamedValueChecker]
type connection struct {
	driver.Conn
	id     string
	logger *sqlogger
}

// Begin implements [driver.Conn]
func (c *connection) Begin() (driver.Tx, error) {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug
	id := c.logger.idGenerator()
	attrs := append(c.logData(), slog.String(txKey, id))

	tx, err := c.Conn.Begin()
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Begin", start, err, attrs...)

	return &transaction{tx, id, c.id, c.logger}, err
}

// Prepare implements [driver.Conn]
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug
	id := c.logger.idGenerator()
	attrs := append(c.logData(), slog.String(stmtKey, id), slog.String(queryKey, query))

	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Prepare", start, err, attrs...)

	return &statement{stmt, id, c.id, query, c.logger}, err
}

// Close implements [driver.Conn]
func (c *connection) Close() error {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug

	err := c.Conn.Close()
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Close", start, err, c.logData()...)

	return err
}

// BeginTx implements [driver.ConnBeginTx]
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	drvTx, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelDebug
	id := c.logger.idGenerator()
	attrs := append(c.logData(), slog.String(txKey, id))

	tx, err := drvTx.BeginTx(ctx, opts)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "BeginTx", start, err, attrs...)

	return &transaction{tx, id, c.id, c.logger}, err
}

// PrepareContext implements [driver.ConnPrepareContext]
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	driverPrep, ok := c.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelDebug
	id := c.logger.idGenerator()
	attrs := append(c.logData(), slog.String(stmtKey, id), slog.String(queryKey, query))

	stmt, err := driverPrep.PrepareContext(ctx, query)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "PrepareContext", start, err, attrs...)

	return &statement{stmt, id, c.id, query, c.logger}, err
}

// Ping implements [driver.Pinger]
func (c *connection) Ping(ctx context.Context) error {
	driverPinger, ok := c.Conn.(driver.Pinger)
	if !ok {
		return driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelDebug

	err := driverPinger.Ping(ctx)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Ping", start, err, c.logData()...)

	return err
}

// Exec implements [driver.Execer]
func (c *connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	driverExecer, ok := c.Conn.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}

	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(c.logData(), slog.String(queryKey, query), slog.Any(argsKey, args))

	res, err := driverExecer.Exec(query, args)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Exec", start, err, attrs...)

	return res, err
}

// ExecContext implements [driver.ExecerContext]
func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	driverExecerContext, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(c.logData(), slog.String(queryKey, query), slog.Any(argsKey, valuesFromNamedArgs(args)))

	res, err := driverExecerContext.ExecContext(ctx, query, args)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "ExecContext", start, err, attrs...)

	return res, err
}

// Query implements [driver.Queryer]
func (c *connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	driverQueryer, ok := c.Conn.(driver.Queryer)
	if !ok {
		return nil, driver.ErrSkip
	}

	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(c.logData(), slog.String(queryKey, query), slog.Any(argsKey, args))

	rows, err := driverQueryer.Query(query, args)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "Query", start, err, attrs...)

	return rows, err
}

// QueryContext implements [driver.QueryerContext]
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	driverQueryerContext, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(c.logData(), slog.String(queryKey, query), slog.Any(argsKey, valuesFromNamedArgs(args)))

	rows, err := driverQueryerContext.QueryContext(ctx, query, args)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "QueryContext", start, err, attrs...)

	return rows, err
}

// ResetSession implements [driver.SessionResetter]
func (c *connection) ResetSession(ctx context.Context) error {
	resetter, ok := c.Conn.(driver.SessionResetter)
	if !ok {
		return driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelDebug

	err := resetter.ResetSession(ctx)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "ResetSession", start, err, c.logData()...)

	return err
}

// CheckNamedValue implements [driver.NamedValueChecker]
func (c *connection) CheckNamedValue(nm *driver.NamedValue) error {
	checker, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}

	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug

	err := checker.CheckNamedValue(nm)
	if err != nil {
		lvl = slog.LevelError
	}

	c.logger.log(ctx, lvl, "CheckNamedValue", start, err, c.logData()...)

	return err
}

func (c *connection) logData() []slog.Attr {
	return []slog.Attr{
		slog.String(connKey, c.id),
	}
}
