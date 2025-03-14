package sqlz

import (
	"github.com/rafaberaldo/sqlz/internal/parser"
)

var bindByDriverName = map[string]parser.Bind{
	"azuresql":         parser.BindAt,
	"sqlserver":        parser.BindAt,
	"godror":           parser.BindColon,
	"goracle":          parser.BindColon,
	"oci8":             parser.BindColon,
	"ora":              parser.BindColon,
	"cloudsqlpostgres": parser.BindDollar,
	"cockroach":        parser.BindDollar,
	"nrpostgres":       parser.BindDollar,
	"pgx":              parser.BindDollar,
	"postgres":         parser.BindDollar,
	"pq-timeouts":      parser.BindDollar,
	"ql":               parser.BindDollar,
	"mysql":            parser.BindQuestion,
	"nrmysql":          parser.BindQuestion,
	"nrsqlite3":        parser.BindQuestion,
	"sqlite3":          parser.BindQuestion,
}

const (
	BindAt       = parser.BindAt       // BindAt is the placeholder '@p1'
	BindColon    = parser.BindColon    // BindColon is the placeholder ':name'
	BindDollar   = parser.BindDollar   // BindDollar is the placeholder '$1'
	BindQuestion = parser.BindQuestion // BindQuestion is the placeholder '?'
)

func RegisterDriverName(driverName string, bind parser.Bind) {
	// TODO
}
