package sqlz

import "github.com/rafaberaldo/sqlz/binder"

var bindByDriverName = map[string]binder.Bind{
	"azuresql":         binder.At,
	"sqlserver":        binder.At,
	"godror":           binder.Colon,
	"goracle":          binder.Colon,
	"oci8":             binder.Colon,
	"ora":              binder.Colon,
	"cloudsqlpostgres": binder.Dollar,
	"cockroach":        binder.Dollar,
	"nrpostgres":       binder.Dollar,
	"pgx":              binder.Dollar,
	"postgres":         binder.Dollar,
	"pq-timeouts":      binder.Dollar,
	"ql":               binder.Dollar,
	"mysql":            binder.Question,
	"nrmysql":          binder.Question,
	"nrsqlite3":        binder.Question,
	"sqlite3":          binder.Question,
}

func RegisterDriverName(driverName string, bind binder.Bind) {
	// TODO
}
