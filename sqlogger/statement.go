package sqlogger

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

// statement implements
// [driver.Stmt]
// [driver.StmtExecContext]
// [driver.StmtQueryContext]
// [driver.NamedValueChecker]
// [driver.ColumnConverter]
type statement struct {
	driver.Stmt
	id     string
	connId string
	query  string
	logger *sqlogger
}

// Close implements [driver.Stmt]
func (s *statement) Close() error {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug

	err := s.Stmt.Close()
	if err != nil {
		lvl = slog.LevelError
	}

	s.logger.log(ctx, lvl, "StmtClose", start, err, s.logAttrs()...)

	return err
}

// NumInput implements [driver.Stmt]
func (s *statement) NumInput() int {
	return s.Stmt.NumInput()
}

// Exec implements [driver.Stmt]
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(s.logAttrs(), slog.Any(argsKey, args))

	res, err := s.Stmt.Exec(args)
	if err != nil {
		lvl = slog.LevelError
	}

	s.logger.log(ctx, lvl, "StmtExec", start, err, attrs...)

	return res, err
}

// Query implements [driver.Stmt]
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(s.logAttrs(), slog.Any(argsKey, args))

	rows, err := s.Stmt.Query(args)
	if err != nil {
		lvl = slog.LevelError
	}

	s.logger.log(ctx, lvl, "StmtQuery", start, err, attrs...)

	return rows, err
}

// ExecContext implements [driver.StmtExecContext]
func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmtExecer, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(s.logAttrs(), slog.Any(argsKey, valuesFromNamedArgs(args)))

	res, err := stmtExecer.ExecContext(ctx, args)
	if err != nil {
		lvl = slog.LevelError
	}

	s.logger.log(ctx, lvl, "StmtExecContext", start, err, attrs...)

	return res, err
}

// QueryContext implements [driver.StmtQueryContext]
func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	stmtQueryer, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}

	start := time.Now()
	lvl := slog.LevelInfo
	attrs := append(s.logAttrs(), slog.Any(argsKey, valuesFromNamedArgs(args)))

	rows, err := stmtQueryer.QueryContext(ctx, args)
	if err != nil {
		lvl = slog.LevelError
	}

	s.logger.log(ctx, lvl, "StmtQueryContext", start, err, attrs...)

	return rows, err
}

// CheckNamedValue implements [driver.NamedValueChecker]
func (s *statement) CheckNamedValue(nm *driver.NamedValue) error {
	checker, ok := s.Stmt.(driver.NamedValueChecker)
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

	s.logger.log(ctx, lvl, "StmtCheckNamedValue", start, err, s.logAttrs()...)

	return err
}

// ColumnConverter implements [driver.ColumnConverter]
func (s *statement) ColumnConverter(idx int) driver.ValueConverter {
	if converter, ok := s.Stmt.(driver.ColumnConverter); ok {
		return converter.ColumnConverter(idx)
	}

	return driver.DefaultParameterConverter
}

func (s *statement) logAttrs() []slog.Attr {
	return []slog.Attr{
		slog.String(connKey, s.connId),
		slog.String(stmtKey, s.id),
		slog.String(queryKey, s.query),
	}
}
