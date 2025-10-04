package sqlz

import (
	"sync"

	"github.com/rfberaldo/sqlz/parser"
)

var bindByDriverName sync.Map

func init() {
	bindByDriverName.Store("azuresql", parser.BindAt)
	bindByDriverName.Store("sqlserver", parser.BindAt)

	bindByDriverName.Store("godror", parser.BindColon)
	bindByDriverName.Store("goracle", parser.BindColon)
	bindByDriverName.Store("oci8", parser.BindColon)
	bindByDriverName.Store("ora", parser.BindColon)

	bindByDriverName.Store("cloudsqlpostgres", parser.BindDollar)
	bindByDriverName.Store("cockroach", parser.BindDollar)
	bindByDriverName.Store("nrpostgres", parser.BindDollar)
	bindByDriverName.Store("pgx", parser.BindDollar)
	bindByDriverName.Store("postgres", parser.BindDollar)
	bindByDriverName.Store("pq-timeouts", parser.BindDollar)
	bindByDriverName.Store("ql", parser.BindDollar)

	bindByDriverName.Store("mysql", parser.BindQuestion)
	bindByDriverName.Store("nrmysql", parser.BindQuestion)
	bindByDriverName.Store("nrsqlite3", parser.BindQuestion)
	bindByDriverName.Store("sqlite3", parser.BindQuestion)
}

// Register adds a new driver name and its bind to be
// available to [BindByDriver], panics if the name is empty
// or if the bind is Unknown.
func Register(name string, bind parser.Bind) {
	if name == "" {
		panic("sqlz: driver name cannot be empty")
	}

	if bind == parser.BindUnknown {
		panic("sqlz: bind cannot be unknown")
	}

	bindByDriverName.Store(name, bind)
}

// BindByDriver return the [parser.Bind] corresponding to driver name.
// If it's not found, [Register] a new driver name.
func BindByDriver(name string) parser.Bind {
	val, ok := bindByDriverName.Load(name)
	if !ok {
		return parser.BindUnknown
	}
	return val.(parser.Bind)
}
