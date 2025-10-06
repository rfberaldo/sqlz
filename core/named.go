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
	bind            parser.Bind
	structTag       string
	fieldNameMapper func(string) string
	fieldIndexByKey map[string][]int

	// result
	query string
	args  []any
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

	n := &namedQuery{
		bind:            opts.Bind,
		structTag:       opts.StructTag,
		fieldNameMapper: opts.FieldNameMapper,
	}

	if err := n.process(query, arg); err != nil {
		return "", nil, err
	}

	return n.query, n.args, nil
}

func (n *namedQuery) process(query string, arg any) error {
	argValue := reflect.Indirect(reflect.ValueOf(arg))
	if !argValue.IsValid() {
		return fmt.Errorf("sqlz/named: argument is nil pointer")
	}

	switch kind := argValue.Kind(); kind {
	case reflect.Map, reflect.Struct:
		return n.processOne(query, argValue, kind)

	case reflect.Slice:
		return n.processSlice(query, argValue)
	}

	return fmt.Errorf("sqlz/named: unsupported argument type: %T", arg)
}

func (n *namedQuery) processOne(query string, argValue reflect.Value, kind reflect.Kind) error {
	query, idents := parser.Parse(n.bind, query)
	var err error

	switch kind {
	case reflect.Map:
		err = n.bindMapArgs(idents, argValue)

	case reflect.Struct:
		err = n.bindStructArgs(idents, argValue)
	}

	if err != nil {
		return err
	}

	n.query, n.args, err = parser.ParseInClause(n.bind, query, n.args)
	if err != nil {
		return err
	}

	return nil
}

func (n *namedQuery) structValue(v reflect.Value) any {
	v = reflect.Indirect(v)
	if !v.IsValid() {
		return nil
	}

	// not testing pointer receiver, as [driver.Valuer] must have value receiver
	if v.Type().Implements(valuerType) {
		return v.Interface()
	}

	// this helps allocating less than necessary
	return reflectutil.TypedValue(v)
}

// bindStructArgs maps idents to the argValue struct fields, binding their values,
// binded args may have slices, meaning an "IN" clause.
func (n *namedQuery) bindStructArgs(idents []string, argValue reflect.Value) error {
	argValue = reflect.Indirect(argValue)
	if !argValue.IsValid() {
		return fmt.Errorf("sqlz/named: argument is nil pointer")
	}

	if n.args == nil {
		n.args = make([]any, 0, len(idents))
	}

	if n.fieldIndexByKey == nil {
		n.fieldIndexByKey = reflectutil.StructFieldMap(
			argValue.Type(),
			n.structTag,
			n.fieldNameMapper,
		)
	}

	for _, ident := range idents {
		index, ok := n.fieldIndexByKey[ident]
		if !ok {
			return fmt.Errorf("sqlz/named: field not found: '%s' (maybe unexported?)", ident)
		}
		v, err := argValue.FieldByIndexErr(index)
		if err != nil {
			return fmt.Errorf("sqlz/named: field is nil pointer: '%s'", ident)
		}
		n.args = append(n.args, n.structValue(v))
	}

	return nil
}

// bindMapArgs maps idents to the argValue map keys, binding their values,
// binded args may have slices, meaning an "IN" clause.
func (n *namedQuery) bindMapArgs(idents []string, argValue reflect.Value) error {
	m, err := AssertMap(argValue.Interface())
	if err != nil {
		return err
	}

	if n.args == nil {
		n.args = make([]any, 0, len(idents))
	}

	for _, ident := range idents {
		value, ok := GetMapValue(ident, m)
		if !ok {
			return fmt.Errorf("sqlz/named: could not find '%s' in %+v", ident, m)
		}
		n.args = append(n.args, value)
	}
	return nil
}

type binderFunc = func(idents []string, argValue reflect.Value) error

func (n *namedQuery) processSlice(query string, sliceValue reflect.Value) error {
	if sliceValue.Len() == 0 {
		return fmt.Errorf("sqlz/named: slice is zero length: %s", sliceValue.Type())
	}

	elType := reflectutil.Deref(sliceValue.Type().Elem())
	switch elType.Kind() {
	case reflect.Map:
		return n.bindSliceArgs(query, sliceValue, n.bindMapArgs)

	case reflect.Struct:
		return n.bindSliceArgs(query, sliceValue, n.bindStructArgs)

	default:
		return fmt.Errorf("sqlz/named: unsupported slice type: %s", sliceValue.Type())
	}
}

func (n *namedQuery) bindSliceArgs(query string, sliceValue reflect.Value, binder binderFunc) error {
	idents := parser.ParseIdents(n.bind, query)
	if n.args == nil {
		n.args = make([]any, 0, len(idents)*sliceValue.Len())
	}

	for i := range sliceValue.Len() {
		if err := binder(idents, sliceValue.Index(i)); err != nil {
			return err
		}
	}

	var err error

	// if bind is '?', parse query before expanding
	if n.bind == parser.BindQuestion {
		n.query = parser.ParseQuery(n.bind, query)
		n.query, err = expandInsertSyntax(n.query, sliceValue.Len())
		return err
	}

	n.query, err = expandInsertSyntax(query, sliceValue.Len())
	if err != nil {
		return err
	}

	n.query = parser.ParseQuery(n.bind, n.query)

	return nil
}

var regValues = regexp.MustCompile(`(?i)\)\s*VALUES\s*\(`)

// expandInsertSyntax multiply the 'VALUES' part of a INSERT query by count.
func expandInsertSyntax(query string, count int) (string, error) {
	loc := regValues.FindStringIndex(query)
	if loc == nil {
		return "", fmt.Errorf("sqlz/named: slice is only supported in INSERT query with 'VALUES' clause")
	}

	openIdx := loc[1] - 1
	closeIdx := endingParensIndex(query[openIdx:])
	if closeIdx == -1 {
		return "", fmt.Errorf("sqlz/named: could not parse batch INSERT, missing ending parenthesis")
	}
	closeIdx += openIdx + 1

	beginning := query[:closeIdx]
	values := strings.Repeat(","+query[openIdx:closeIdx], count-1)
	ending := query[closeIdx:]

	return beginning + values + ending, nil
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
