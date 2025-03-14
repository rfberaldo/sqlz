package binder

type Bind byte

const (
	_        Bind = iota
	At            // BindAt is the placeholder '@p1'
	Colon         // BindColon is the placeholder ':name'
	Dollar        // BindDollar is the placeholder '$1'
	Question      // BindQuestion is the placeholder '?'
)
