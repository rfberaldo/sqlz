package sqlz

import (
	"cmp"

	"github.com/rfberaldo/sqlz/internal/parser"
)

const (
	defaultStructTag         = "db"
	defaultBind              = parser.BindQuestion
	defaultStmtCacheCapacity = 16
)

var (
	defaultFieldNameTransformer = ToSnakeCase
)

// config contains flags that are used across internal objects.
type config struct {
	defaultsApplied      bool
	bind                 parser.Bind
	structTag            string
	fieldNameTransformer func(string) string
	ignoreMissingFields  bool
	stmtCacheCapacity    int
}

// applyDefaults returns a cfg with defaults applied, if not set.
func applyDefaults(cfg *config) *config {
	if cfg == nil {
		cfg = &config{}
	}

	// make it easy to create custom configs during tests and avoid data racing
	if cfg.defaultsApplied {
		return cfg
	}

	cfg.defaultsApplied = true

	cfg.bind = cmp.Or(cfg.bind, defaultBind)
	cfg.structTag = cmp.Or(cfg.structTag, defaultStructTag)
	cfg.stmtCacheCapacity = cmp.Or(cfg.stmtCacheCapacity, defaultStmtCacheCapacity)

	if cfg.fieldNameTransformer == nil {
		cfg.fieldNameTransformer = defaultFieldNameTransformer
	}

	return cfg
}
