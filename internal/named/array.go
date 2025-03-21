package named

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/rfberaldo/sqlz/internal/parser"
)

func (n *Named) processArray(query string, arg any) (string, []any, error) {
	argValue := reflect.ValueOf(arg)
	if argValue.Len() == 0 {
		return "", nil, fmt.Errorf("sqlz: slice is length 0: %#v", arg)
	}

	// get the type of the first element, rest must be the same
	elValue := argValue.Index(0)
	elKind := elValue.Kind()

	if elKind == reflect.Ptr {
		elValue = elValue.Elem()
		elKind = elValue.Kind()
	}

	switch elKind {
	case reflect.Map:
		if !canCastToMap(elValue.Interface()) {
			return "", nil, fmt.Errorf("sqlz: unsupported map type: %T", elValue.Interface())
		}
		return n.arrayValues(query, argValue, elValue)

	case reflect.Struct:
		return n.arrayValues(query, argValue, elValue)

	default:
		return "", nil, fmt.Errorf("sqlz: unsupported array type: %T", arg)
	}
}

func (n *Named) arrayValues(query string, argValue, elValue reflect.Value) (string, []any, error) {
	args, err := n.arrayArgs(parser.ParseIdents(n.bind, query), argValue, elValue)
	if err != nil {
		return "", nil, err
	}

	q, err := expandInsertSyntax(query, argValue.Len())
	if err != nil {
		return "", nil, err
	}

	return parser.ParseQuery(n.bind, q), args, nil
}

func (n *Named) arrayArgs(idents []string, argValue, elValue reflect.Value) ([]any, error) {
	outArgs := make([]any, 0, len(idents)*argValue.Len())

	for i := range argValue.Len() {
		args := make([]any, 0, len(idents))
		var err error

		switch elValue.Kind() {
		case reflect.Map:
			args, err = n.mapValues(idents, argValue.Index(i).Interface().(map[string]any))

		case reflect.Struct:
			args, err = n.structValues(idents, argValue.Index(i).Interface())
		}

		if err != nil {
			return nil, err
		}
		outArgs = append(outArgs, args...)
	}

	return outArgs, nil
}

var regValues = regexp.MustCompile(`(?i)\)\s*VALUES\s*\(`)

// expandInsertSyntax multiply the last part of a INSERT query by length
func expandInsertSyntax(query string, length int) (string, error) {
	loc := regValues.FindStringIndex(query)
	if loc == nil {
		return "", fmt.Errorf("sqlz: slice is only supported in INSERT query with \"VALUES\" clause")
	}

	startPos := loc[1] - 1 // position of '(' after 'VALUES'
	endPos := 0            // position of last ')'

	// this is done because the ending might have semicolon, tabs, spaces etc
	for i, ch := range query[startPos:] {
		if ch == ')' {
			endPos = startPos + i + 1
		}
	}

	if endPos == 0 {
		return "", fmt.Errorf("sqlz: could not parse batch INSERT, missing ending parenthesis")
	}

	values := query[startPos:endPos]
	queryWithoutValues := query[:startPos]

	var sb strings.Builder
	sb.Grow(len(queryWithoutValues) + (len(values)+1)*length)
	sb.WriteString(queryWithoutValues)

	for i := range length {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(values)
	}

	return sb.String(), nil
}
