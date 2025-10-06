package reflectutil

import (
	"fmt"
	"reflect"
	"strings"
)

// structMapper is a helper to map struct fields index by tag/name.
type structMapper struct {
	tag        string
	nameMapper func(string) string
	indexByKey map[string][]int
}

// StructFieldMap maps the structType fields, tag is the struct tag to search for,
// and nameMapper is used to map field names in case the tag was not found.
func StructFieldMap(structType reflect.Type, tag string, nameMapper func(string) string) map[string][]int {
	structType = DerefType(structType)
	if structType.Kind() != reflect.Struct {
		panic(fmt.Errorf("sqlz/reflectutil: reflect.Type must be a struct or pointer to struct, got %s", structType))
	}

	sm := &structMapper{tag, nameMapper, make(map[string][]int)}
	sm.traverse(structType)

	return sm.indexByKey
}

type node struct {
	t     reflect.Type
	path  strings.Builder
	index []int
}

func (n *node) writePath(s string) {
	if n.path.Len() > 0 {
		n.path.WriteRune('.')
	}
	n.path.WriteString(s)
}

func (n node) spawn(t reflect.Type) node {
	return node{
		t,
		n.path,
		append(make([]int, 0, len(n.index)+1), n.index...),
	}
}

// traverse maps the struct field indexes, using BFS algorithm starting on t.
func (sm *structMapper) traverse(t reflect.Type) {
	queue := append(
		make([]node, 0, t.NumField()),
		node{t: t, index: make([]int, 0, 1)},
	)

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		for i := range parent.t.NumField() {
			field := parent.t.Field(i)
			fieldType := DerefType(field.Type)
			curr := parent.spawn(fieldType)

			if !field.IsExported() {
				continue
			}

			name, ok := FieldTag(field, sm.tag)
			if !ok {
				name = sm.nameMapper(field.Name)
			}

			curr.index = append(curr.index, field.Index...)
			curr.writePath(name)

			if fieldType.Kind() == reflect.Struct {
				queue = append(queue, curr)

				if field.Anonymous {
					continue
				}
			}

			if _, exists := sm.indexByKey[name]; !exists {
				sm.indexByKey[name] = curr.index
			}

			key := curr.path.String()
			if _, exists := sm.indexByKey[key]; !exists {
				sm.indexByKey[key] = curr.index
			}
		}
	}
}

// FieldTag returns the tag from a struct field, removing any optional args.
func FieldTag(field reflect.StructField, structTag string) (string, bool) {
	tagValue, ok := field.Tag.Lookup(structTag)
	if !ok {
		return "", false
	}

	// check for possible comma as in "...,omitempty"
	if i := strings.Index(tagValue, ","); i > -1 {
		tagValue = tagValue[:i]
	}

	if tagValue != "" && tagValue != "-" {
		return tagValue, true
	}

	return "", false
}

// FieldByIndex returns the struct field from v, initializing any nested nil struct.
func FieldByIndex(v reflect.Value, index []int) reflect.Value {
	v = reflect.Indirect(v)
	if !v.IsValid() {
		panic("sqlz/reflectutil: reflect.Value is nil pointer")
	}

	if v.Kind() != reflect.Struct {
		panic(fmt.Errorf("sqlz/reflectutil: reflect.Value must a struct: %s", v.Type()))
	}

	if !v.CanAddr() {
		panic(fmt.Errorf("sqlz/reflectutil: reflect.Value must be an addressable struct: %s", v.Type()))
	}

	fv, err := v.FieldByIndexErr(index)
	if err == nil {
		return fv
	}

	initNested(v, index)
	return v.FieldByIndex(index)
}

func initNested(v reflect.Value, index []int) {
	if len(index) == 0 {
		return
	}

	v = reflect.Indirect(v)

	fv := v.Field(index[0])
	fv = Deref(fv)
	if IsNilStruct(fv) {
		fv.Set(reflect.New(fv.Type().Elem()))
	}

	initNested(fv, index[1:])
}
