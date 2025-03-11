package sqlz

import (
	"context"
	"log/slog"
	"time"
)

func log(l *slog.Logger, msg string, start time.Time, err error, attrs ...slog.Attr) {
	if l == nil {
		return
	}

	var lvl slog.Level
	switch {
	case err == nil:
		lvl = slog.LevelInfo
	case IsNotFound(err):
		lvl = slog.LevelWarn
	default:
		lvl = slog.LevelError
	}

	var logAttrs []slog.Attr
	if err != nil {
		logAttrs = append(logAttrs, slog.String("error", err.Error()))
	}
	logAttrs = append(logAttrs, attrs...)
	logAttrs = append(logAttrs, slog.Duration("duration", time.Since(start)))

	l.LogAttrs(
		context.Background(),
		lvl,
		msg,
		logAttrs...,
	)
}
