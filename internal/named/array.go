package named

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/rafaberaldo/sqlz/internal/parser"
)

var (
	regValues = regexp.MustCompile(`(?i)\)\s*VALUES\s*\(`)
)

func (n *Named) processArray(query string, arg any) (string, []any, error) {
	argValue := reflect.ValueOf(arg)
	if argValue.Len() == 0 {
		return "", nil, fmt.Errorf("sqlz: slice is length 0: %#v", arg)
	}

	// get the type of the first element, rest should be same
	elValue := argValue.Index(0)
	elKind := elValue.Kind()

	if elKind == reflect.Ptr {
		elValue = elValue.Elem()
		elKind = elValue.Kind()
	}

	switch elKind {
	case reflect.Map:
		if !canCastToMap(elValue.Interface()) {
			return "", nil, fmt.Errorf("sqlz: unsupported map type: %T", arg)
		}
		return n.namedAnyArray(query, argValue, elValue)

	case reflect.Struct:
		return n.namedAnyArray(query, argValue, elValue)

	default:
		return "", nil, fmt.Errorf("sqlz: unsupported array type: %T", arg)
	}
}

func (n *Named) namedAnyArray(query string, argValue, elValue reflect.Value) (string, []any, error) {
	args, err := n.getArrayArgs(parser.ParseIdents(n.bind, query), argValue, elValue)
	if err != nil {
		return "", nil, err
	}

	q, err := fixInsertSyntax(query, argValue.Len()-1)
	if err != nil {
		return "", nil, err
	}

	return parser.ParseQuery(n.bind, q), args, nil
}

func (n *Named) getArrayArgs(idents []string, argValue, elValue reflect.Value) ([]any, error) {
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

// fixInsertSyntax multiply the last part of a INSERT query by length
func fixInsertSyntax(query string, length int) (string, error) {
	query = strings.TrimSuffix(query, ";")
	loc := regValues.FindStringIndex(query)
	if loc == nil {
		return "", fmt.Errorf("sqlz: slice is only supported in INSERT query")
	}

	var sb strings.Builder
	sb.WriteString(query)
	values := query[loc[1]-1:]

	for range length {
		sb.WriteByte(',')
		sb.WriteString(values)
	}

	return sb.String(), nil
}
