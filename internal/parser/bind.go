package parser

type Bind byte

const (
	_ Bind = iota
	BindAt
	BindColon
	BindDollar
	BindQuestion
)
