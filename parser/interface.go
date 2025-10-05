package parser

import (
	"fmt"
	"reflect"
)

// Parse transforms a named query into native query, respecting the bind param,
// returning the transformed query and a slice of identifiers.
func Parse(bind Bind, query string) (string, []string) {
	p := &Parser{bind: bind, input: query}
	return p.parse(false)
}

// ParseQuery is like [Parse], but only return the query.
func ParseQuery(bind Bind, query string) string {
	p := &Parser{bind: bind, input: query}
	output, _ := p.parse(true)
	return output
}

// ParseIdents is like [Parse], but only return a slice of identifiers.
func ParseIdents(bind Bind, query string) []string {
	p := &Parser{bind: bind, input: query}
	_, idents := p.parse(false)
	return idents
}

// ParseInClause expands any binds in the query, respecting the bind param,
// that correspond to a slice in args to the length of that slice,
// and then appends those slice elements to a new arglist.
func ParseInClause(bind Bind, query string, args []any) (string, []any, error) {
	countByIndex, spreadArgs, err := spreadSlices(args)
	if err != nil {
		return "", nil, err
	}

	if len(countByIndex) == 0 {
		return query, args, nil
	}

	p := &Parser{
		bind:                 bind,
		input:                query,
		inClauseCountByIndex: countByIndex,
	}
	output := p.parseInNative()

	if len(spreadArgs) != p.bindCount {
		return "", nil, fmt.Errorf(
			"sqlz/parser: wrong number of arguments (bindvars=%v arguments=%v)",
			p.bindCount, len(spreadArgs),
		)
	}

	return output, spreadArgs, nil
}

func spreadSlices(args []any) (map[int]int, []any, error) {
	inClauseCountByIndex := make(map[int]int)
	outArgs := make([]any, 0, len(args))

	for i, arg := range args {
		argValue := reflect.Indirect(reflect.ValueOf(arg))

		if shouldSpread(argValue) {
			length := argValue.Len()
			if length == 0 {
				return nil, nil, fmt.Errorf("sqlz/parser: empty slice passed to 'IN' clause")
			}
			inClauseCountByIndex[i] = length
			for j := range length {
				outArgs = append(outArgs, argValue.Index(j).Interface())
			}
			continue
		}

		outArgs = append(outArgs, arg)
	}

	return inClauseCountByIndex, outArgs, nil
}

// byteSliceType is the [reflect.Type] of []byte
var byteSliceType = reflect.TypeOf([]byte{})

func shouldSpread(argValue reflect.Value) bool {
	if !argValue.IsValid() {
		return false
	}

	// []byte is a [driver.Value] type so it should not be expanded
	if argValue.Type() == byteSliceType {
		return false
	}

	// if it's slice then it's part of "IN" clause and have to spread
	return argValue.Kind() == reflect.Slice
}
