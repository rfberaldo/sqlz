package named

import (
	"fmt"
	"reflect"

	"github.com/rafaberaldo/sqlz/internal/parser"
)

type Named struct {
	bind             parser.Bind
	cacheIdByKey     map[reflectCache]int
	cacheLastFullKey string
}

type reflectCache struct {
	fullKey string // the entire key
	key     string // the current key
}

// Compile return a new query replacing named parameters with binds,
// and a slice of ordered arguments.
func Compile(bind parser.Bind, query string, arg any) (string, []any, error) {
	n := &Named{bind: bind, cacheIdByKey: make(map[reflectCache]int)}
	return n.compile(query, arg)
}

func (n *Named) compile(query string, arg any) (string, []any, error) {
	if query == "" {
		return "", nil, fmt.Errorf("sqlz: query cannot be blank")
	}
	if arg == nil {
		return "", nil, fmt.Errorf("sqlz: argument cannot be nil on named query")
	}

	kind := reflect.TypeOf(arg).Kind()
	switch kind {
	case reflect.Map, reflect.Struct:
		return n.process(query, arg, kind)

	case reflect.Array, reflect.Slice:
		return n.processArray(query, arg)
	}

	return "", nil, fmt.Errorf("sqlz: unsupported arg type: %T", arg)
}

func (n *Named) process(query string, arg any, kind reflect.Kind) (string, []any, error) {
	q, idents := parser.ParseNamed(n.bind, query)
	args := make([]any, 0, len(idents))
	var err error

	switch kind {
	case reflect.Map:
		if !canCastToMap(arg) {
			return "", nil, fmt.Errorf("sqlz: unsupported map type: %T", arg)
		}
		args, err = n.mapValues(idents, arg.(map[string]any))

	case reflect.Struct:
		args, err = n.structValues(idents, arg)
	}

	if err != nil {
		return "", nil, err
	}

	qq, args, err := parser.ParseInNamed(n.bind, query, args)
	switch err {
	case nil:
		return qq, args, nil

	// if there's no slices to spread, then the query doesn't have `IN` clause,
	// return the previously-parsed query.
	case parser.ErrNoSlices:
		return q, args, nil

	default:
		return "", nil, err
	}
}

func (n *Named) saveReflectIndex(key string, i int) {
	k := reflectCache{n.cacheLastFullKey, key}
	n.cacheIdByKey[k] = i
}

func (n *Named) reflectIndex(key string) (int, bool) {
	k := reflectCache{n.cacheLastFullKey, key}
	i, ok := n.cacheIdByKey[k]
	return i, ok
}
