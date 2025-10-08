package sqlz

import (
	"cmp"

	"github.com/rfberaldo/sqlz/parser"
)

// config contains flags that are used across internal objects.
type config struct {
	bind                 parser.Bind
	structTag            string
	fieldNameTransformer func(string) string
	ignoreMissingFields  bool
}

// defaults sets config defaults if not set.
func (cfg *config) defaults() {
	cfg.bind = cmp.Or(cfg.bind, parser.BindQuestion)
	cfg.structTag = cmp.Or(cfg.structTag, defaultStructTag)

	if cfg.fieldNameTransformer == nil {
		cfg.fieldNameTransformer = ToSnakeCase
	}
}
