// This file contains the exported entry points for invoking the parser.

package parser

import (
	"fmt"
)

// ParseNamed return a new query replacing named parameters with binds,
// and a slice of ordered identifiers.
func ParseNamed(bind Bind, input string) (string, []string) {
	p := &Parser{bind: bind, input: input}
	p.readChar()
	return p.parseNamed(namedOptions{})
}

// ParseQuery is like [ParseNamed], but only return the query.
func ParseQuery(bind Bind, input string) string {
	p := &Parser{bind: bind, input: input}
	p.readChar()
	output, _ := p.parseNamed(namedOptions{skipIdents: true})
	return output
}

// ParseIdents is like [ParseNamed], but only return a slice of
// ordered identifiers.
func ParseIdents(bind Bind, input string) []string {
	p := &Parser{bind: bind, input: input}
	p.readChar()
	_, idents := p.parseNamed(namedOptions{skipQuery: true})
	return idents
}

// ErrNoSlices is used internally to know when to use a previously-parsed query.
// If there's no slices to spread, means there's no `IN` clause in query.
var ErrNoSlices = fmt.Errorf("sqlz: no slices to spread")

// ParseInNamed is like [ParseNamed], but also receives a slice of ordered args,
// the args are spread if they have slices, which are used within `IN` clause.
// ParseInNamed return a new query replacing named parameters with binds,
// and the spreaded args.
func ParseInNamed(bind Bind, input string, args []any) (string, []any, error) {
	countByIndex, spreadArgs, err := spreadSliceValues(args...)
	if err != nil {
		return "", nil, err
	}

	// do not parse if it doesn't have slice values
	if len(countByIndex) == 0 {
		return "", args, ErrNoSlices
	}

	p := &Parser{
		bind:                 bind,
		input:                input,
		inClauseCountByIndex: countByIndex,
	}
	p.readChar()
	output, _ := p.parseNamed(namedOptions{skipIdents: true})

	if len(spreadArgs) != p.bindCount {
		return "", nil, fmt.Errorf(
			"sqlz: wrong number of arguments (bindvars=%v arguments=%v)",
			p.bindCount, len(spreadArgs),
		)
	}

	return output, spreadArgs, nil
}

// ParseIn is like [ParseInNamed], but for non-named queries.
// Only works for [BindQuestion] bindvar.
func ParseIn(bind Bind, input string, args ...any) (string, []any, error) {
	countByIndex, spreadArgs, err := spreadSliceValues(args...)
	if err != nil {
		return "", nil, err
	}

	// do not parse if it doesn't have slice values
	if len(countByIndex) == 0 {
		return input, args, nil
	}

	if bind != BindQuestion {
		return "", nil, fmt.Errorf("sqlz: a slice was passed as an argument but the driver doesn't support")
	}

	p := &Parser{bind: bind, input: input, inClauseCountByIndex: countByIndex}
	p.readChar()
	output := p.parseIn()

	if bind == BindQuestion && len(spreadArgs) != p.bindCount {
		return "", nil, fmt.Errorf(
			"sqlz: wrong number of arguments (bindvars=%v arguments=%v)",
			p.bindCount, len(spreadArgs),
		)
	}

	return output, spreadArgs, nil
}
