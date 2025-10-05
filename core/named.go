package core

import (
	"cmp"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/rfberaldo/sqlz/parser"
	"github.com/rfberaldo/sqlz/reflectutil"
)

type NamedOptions struct {
	// Bind is the bind type the result query will have.
	Bind parser.Bind

	// StructTag is the reflection tag that will be used to map struct fields.
	StructTag string

	// FieldNameMapper is a func that maps a struct field name to the database column.
	// It is only used when the struct tag is not found.
	FieldNameMapper func(string) string
}

type namedQuery struct {
	bind         parser.Bind
	structMapper *reflectutil.StructMapper
	args         []any
}

func ProcessNamed(query string, arg any, opts *NamedOptions) (string, []any, error) {
	if opts == nil {
		opts = &NamedOptions{}
	}

	opts.Bind = cmp.Or(opts.Bind, parser.BindQuestion)
	opts.StructTag = cmp.Or(opts.StructTag, DefaultStructTag)
	if opts.FieldNameMapper == nil {
		opts.FieldNameMapper = SnakeCaseMapper
	}

	return (&namedQuery{
		bind: opts.Bind,
		structMapper: reflectutil.NewStructMapper(
			opts.StructTag,
			opts.FieldNameMapper),
	}).process(query, arg)
}

func (n *namedQuery) process(query string, arg any) (string, []any, error) {
	argValue := reflect.Indirect(reflect.ValueOf(arg))
	if !argValue.IsValid() {
		return "", nil, fmt.Errorf("sqlz: argument in named query is nil pointer")
	}

	switch kind := argValue.Kind(); kind {
	case reflect.Map, reflect.Struct:
		return n.processOne(query, argValue, kind)

	case reflect.Slice:
		return n.processSlice(query, argValue)
	}

	return "", nil, fmt.Errorf("sqlz: unsupported arg type: %T", arg)
}

func (n *namedQuery) processOne(query string, argValue reflect.Value, kind reflect.Kind) (string, []any, error) {
	query, idents := parser.Parse(n.bind, query)
	var err error

	switch kind {
	case reflect.Map:
		err = n.mapMap(idents, argValue)

	case reflect.Struct:
		err = n.mapStruct(idents, argValue)
	}

	if err != nil {
		return "", nil, err
	}

	query, n.args, err = parser.ParseInClause(n.bind, query, n.args)
	if err != nil {
		return "", nil, err
	}

	return query, n.args, nil
}

// mapStruct return all the values from arg, following the idents order.
// Returned values can be used in a query if they do not have "IN" clause,
// in other words, values can not be slices.
func (n *namedQuery) mapStruct(idents []string, argValue reflect.Value) error {
	if n.args == nil {
		n.args = make([]any, 0, len(idents))
	}
	n.args = n.args[:0]

	for _, ident := range idents {
		v := n.structMapper.FieldByKey(ident, argValue)
		if !v.IsValid() {
			return fmt.Errorf("sqlz: field not found: '%s' (maybe unexported?)", ident)
		}

		v = reflect.Indirect(v)
		if v.IsValid() {
			n.args = append(n.args, v.Interface())
		} else {
			n.args = append(n.args, nil)
		}
	}

	return nil
}

// mapMap return all the values from arg, following the idents order.
// Returned values can be used in a query if they do not have "IN" clause,
// in other words, values can not be slices.
func (n *namedQuery) mapMap(idents []string, argValue reflect.Value) error {
	m, err := AssertMap(argValue.Interface())
	if err != nil {
		return err
	}

	if n.args == nil {
		n.args = make([]any, 0, len(idents))
	}
	n.args = n.args[:0]

	for _, ident := range idents {
		value, ok := GetMapValue(ident, m)
		if !ok {
			return fmt.Errorf("sqlz: could not find '%s' in %+v", ident, m)
		}
		n.args = append(n.args, value)
	}
	return nil
}

type mapperFunc = func(idents []string, argValue reflect.Value) error

func (n *namedQuery) processSlice(query string, sliceValue reflect.Value) (string, []any, error) {
	if sliceValue.Len() == 0 {
		return "", nil, fmt.Errorf("sqlz: slice is zero length: %s", sliceValue.Type())
	}

	elValue := reflect.Indirect(sliceValue.Index(0))
	if !elValue.IsValid() {
		return "", nil, fmt.Errorf("sqlz: slice contains nil pointers: %s", sliceValue.Type())
	}

	switch elValue.Kind() {
	case reflect.Map:
		return n.sliceValues(query, sliceValue, n.mapMap)

	case reflect.Struct:
		return n.sliceValues(query, sliceValue, n.mapStruct)

	default:
		return "", nil, fmt.Errorf("sqlz: unsupported slice type: %s", sliceValue.Type())
	}
}

func (n *namedQuery) sliceValues(query string, sliceValue reflect.Value, mapper mapperFunc) (string, []any, error) {
	idents := parser.ParseIdents(n.bind, query)
	args, err := n.sliceArgs(idents, sliceValue, mapper)
	if err != nil {
		return "", nil, err
	}

	// if bind is '?', parse query before expanding
	if n.bind == parser.BindQuestion {
		q := parser.ParseQuery(n.bind, query)
		q, err := expandInsertSyntax(q, sliceValue.Len())
		return q, args, err
	}

	q, err := expandInsertSyntax(query, sliceValue.Len())
	return parser.ParseQuery(n.bind, q), args, err
}

func (n *namedQuery) sliceArgs(idents []string, sliceValue reflect.Value, mapper mapperFunc) ([]any, error) {
	outArgs := make([]any, 0, len(idents)*sliceValue.Len())
	for i := range sliceValue.Len() {
		if err := mapper(idents, sliceValue.Index(i)); err != nil {
			return nil, err
		}

		outArgs = append(outArgs, n.args...)
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

	i := loc[1] - 1                   // position of '(' after 'VALUES'
	j := endingParensIndex(query[i:]) // position of ending ')'
	if j == -1 {
		return "", fmt.Errorf("sqlz: could not parse batch INSERT, missing ending parenthesis")
	}
	j += i + 1

	beginning := query[:j]
	values := query[i:j]
	ending := query[j:]

	length -= 1
	var sb strings.Builder
	sb.Grow(len(query) + (len(values)+1)*length)

	sb.WriteString(beginning)
	for range length {
		sb.WriteByte(',')
		sb.WriteString(values)
	}
	sb.WriteString(ending)

	return sb.String(), nil
}

// endingParensIndex find the ending parenthesis of a string starting with '(',
// returns -1 if not found.
//
//	endingParensIndex("(NOW())") // Output: 6
func endingParensIndex(s string) int {
	if len(s) <= 1 || s[0] != '(' {
		return -1
	}

	count := 0
	for i, ch := range s {
		if ch == '(' {
			count++
			continue
		}
		if ch == ')' {
			count--
			if count == 0 {
				return i
			}
		}
	}

	return -1
}
