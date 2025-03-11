package sqlz

import (
	"log/slog"
	"sync/atomic"

	"github.com/rafaberaldo/sqlz/internal/parser"
)

var defaultSQLZ atomic.Pointer[SQLZ]

func init() {
	defaultSQLZ.Store(New(parser.BindQuestion))
}

type SQLZ struct {
	bind   parser.Bind
	logger *slog.Logger
}

// New creates an instance of [*SQLZ].
func New(bind parser.Bind, opts ...Option) *SQLZ {
	sqlz := &SQLZ{bind: bind}
	for _, opt := range opts {
		opt(sqlz)
	}
	return sqlz
}

type Option func(*SQLZ)

func WithLogger(logger *slog.Logger) Option {
	return func(s *SQLZ) {
		s.logger = logger
	}
}

func SetDefault(s *SQLZ) {
	defaultSQLZ.Store(s)
}

// defs returns the default [SQLZ].
func defs() *SQLZ { return defaultSQLZ.Load() }
