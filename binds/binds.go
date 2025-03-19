// Package binds contains the Bind type that is used to represent the
// placeholder used by different drivers.
package binds

import (
	"sync"
)

type Bind byte

const (
	Unknown  Bind = iota
	At            // placeholder '@p1'
	Colon         // placeholder ':name'
	Dollar        // placeholder '$1'
	Question      // placeholder '?'
)

var bindByDriverName sync.Map

func init() {
	bindByDriverName.Store("azuresql", At)
	bindByDriverName.Store("sqlserver", At)

	bindByDriverName.Store("godror", Colon)
	bindByDriverName.Store("goracle", Colon)
	bindByDriverName.Store("oci8", Colon)
	bindByDriverName.Store("ora", Colon)

	bindByDriverName.Store("cloudsqlpostgres", Dollar)
	bindByDriverName.Store("cockroach", Dollar)
	bindByDriverName.Store("nrpostgres", Dollar)
	bindByDriverName.Store("pgx", Dollar)
	bindByDriverName.Store("postgres", Dollar)
	bindByDriverName.Store("pq-timeouts", Dollar)
	bindByDriverName.Store("ql", Dollar)

	bindByDriverName.Store("mysql", Question)
	bindByDriverName.Store("nrmysql", Question)
	bindByDriverName.Store("nrsqlite3", Question)
	bindByDriverName.Store("sqlite3", Question)
}

// Register adds a new driver name and its bind to be
// availble to [BindByDriver].
func Register(name string, bind Bind) {
	bindByDriverName.Store(name, bind)
}

// BindByDriver return the [Bind] corresponding to driver name.
// If it's not found, [Register] a new driver name.
func BindByDriver(name string) Bind {
	val, ok := bindByDriverName.Load(name)
	if !ok {
		return Unknown
	}
	return val.(Bind)
}
