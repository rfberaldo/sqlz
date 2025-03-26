package sqlogger

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"time"
)

// transaction implements [driver.Tx]
type transaction struct {
	driver.Tx
	id     string
	connId string
	logger *sqlogger
}

func (tx *transaction) Commit() error {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug

	err := tx.Tx.Commit()
	if err != nil {
		lvl = slog.LevelError
	}

	tx.logger.log(ctx, lvl, "Commit", start, err, tx.logAttrs()...)

	return err
}

func (tx *transaction) Rollback() error {
	ctx := context.Background()
	start := time.Now()
	lvl := slog.LevelDebug

	err := tx.Tx.Rollback()
	if err != nil {
		lvl = slog.LevelError
	}

	tx.logger.log(ctx, lvl, "Rollback", start, err, tx.logAttrs()...)

	return err
}

func (tx *transaction) logAttrs() []slog.Attr {
	return []slog.Attr{
		slog.String(txKey, tx.id),
		slog.String(connKey, tx.connId),
	}
}
