package named

import (
	"fmt"
	"reflect"
	"strings"
)

// mapValues return all the values from arg, following the idents order.
// Returned values can be used in a query if they do not have `IN` clause,
// in other words, values can not be slices.
func (n *Named) mapValues(idents []string, arg any) ([]any, error) {
	outArgs := make([]any, 0, len(idents))
	for _, ident := range idents {
		value, ok := n.mapValue(ident, arg.(map[string]any))
		if !ok {
			return nil, fmt.Errorf("sqlz: could not find name `%s` in %+v", ident, arg)
		}
		outArgs = append(outArgs, value)
	}
	return outArgs, nil
}

// mapValue recursively find the value of a dot notation key string
func (n *Named) mapValue(key string, arg map[string]any) (any, bool) {
	if !strings.Contains(key, ".") {
		value, ok := arg[key]
		return value, ok
	}

	splits := strings.SplitN(key, ".", 2)
	maybeMap, ok := arg[splits[0]]
	if !ok {
		return nil, false
	}

	if !canCastToMap(maybeMap) {
		return nil, false
	}

	return n.mapValue(splits[1], maybeMap.(map[string]any))
}

// canCastToMap check if it is possible to convert arg to map[string]any
func canCastToMap(arg any) bool {
	m := make(map[string]any)
	mtype := reflect.TypeOf(m)
	return reflect.TypeOf(arg).ConvertibleTo(mtype)
}
