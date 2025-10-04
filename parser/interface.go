package parser

import (
	"fmt"
)

// Parse return a new query replacing named parameters with binds,
// and a slice of ordered identifiers.
func Parse(bind Bind, input string) (string, []string) {
	p := &Parser{bind: bind, input: input}
	return p.parse(false)
}

// ParseQuery is like [Parse], but only return the query.
func ParseQuery(bind Bind, input string) string {
	p := &Parser{bind: bind, input: input}
	output, _ := p.parse(true)
	return output
}

// ParseIdents is like [Parse], but only return a slice of ordered identifiers.
func ParseIdents(bind Bind, input string) []string {
	p := &Parser{bind: bind, input: input}
	_, idents := p.parse(false)
	return idents
}

var ErrNoInClause = fmt.Errorf("no in clause, no slices to spread")

// ParseInClause is like [Parse], but also receives a slice of args,
// the args are spread if they have slices, which are used within "IN" clause.
func ParseInClause(bind Bind, input string, args ...any) (string, []any, error) {
	countByIndex, spreadArgs, err := spreadSliceValues(args...)
	if err != nil {
		return "", nil, err
	}

	if len(countByIndex) == 0 {
		return "", args, ErrNoInClause
	}

	p := &Parser{
		bind:                 bind,
		input:                input,
		inClauseCountByIndex: countByIndex,
	}
	output, _ := p.parse(true)

	if len(spreadArgs) != p.bindCount {
		return "", nil, fmt.Errorf(
			"sqlz/parser: wrong number of arguments (bindvars=%v arguments=%v)",
			p.bindCount, len(spreadArgs),
		)
	}

	return output, spreadArgs, nil
}

// ParseInClauseNative is like [ParseInClause], but for native (non-named) queries.
func ParseInClauseNative(bind Bind, input string, args ...any) (string, []any, error) {
	countByIndex, spreadArgs, err := spreadSliceValues(args...)
	if err != nil {
		return "", nil, err
	}

	if len(countByIndex) == 0 {
		return input, args, nil
	}

	p := &Parser{bind: bind, input: input}
	output := p.parseIn(countByIndex)

	if len(spreadArgs) != p.bindCount {
		return "", nil, fmt.Errorf(
			"sqlz/parser: wrong number of arguments (bindvars=%v arguments=%v)",
			p.bindCount, len(spreadArgs),
		)
	}

	return output, spreadArgs, nil
}
