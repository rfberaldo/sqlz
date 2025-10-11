package reflectutil

import (
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
	structType = Deref(structType)
	if structType.Kind() != reflect.Struct {
		panic("sqlz/reflectutil: reflect.Type must be a struct, got " + structType.String())
	}

	sm := &structMapper{tag, nameMapper, make(map[string][]int)}
	sm.traverse(structType)

	return sm.indexByKey
}

type node struct {
	t     reflect.Type
	path  []string
	index []int
}

func (n *node) spawn(t reflect.Type) node {
	return node{
		t,
		append(make([]string, 0, len(n.path)+1), n.path...),
		append(make([]int, 0, len(n.index)+1), n.index...),
	}
}

// traverse maps the struct field indexes, using BFS algorithm starting on t.
func (sm *structMapper) traverse(t reflect.Type) {
	visited := make(map[reflect.Type]uint8)
	queue := append(
		make([]node, 0, t.NumField()),
		node{t, make([]string, 0, 1), make([]int, 0, 1)},
	)

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		if count := visited[parent.t]; count == 255 {
			continue
		}

		for i := range parent.t.NumField() {
			field := parent.t.Field(i)
			fieldType := Deref(field.Type)

			// circular reference
			if fieldType == parent.t {
				visited[fieldType]++
			}

			curr := parent.spawn(fieldType)

			if !field.IsExported() {
				continue
			}

			name, ok := FieldTag(field, sm.tag)
			if !ok {
				name = sm.nameMapper(field.Name)
			}

			curr.index = append(curr.index, field.Index...)
			curr.path = append(curr.path, name)

			if fieldType.Kind() == reflect.Struct {
				queue = append(queue, curr)

				if field.Anonymous {
					continue
				}
			}

			if _, exists := sm.indexByKey[name]; !exists {
				sm.indexByKey[name] = curr.index
			}

			key := strings.Join(curr.path, ".")
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

// FieldByIndex returns the struct field from v, initializing any nested nil pointers.
func FieldByIndex(v reflect.Value, index []int) reflect.Value {
	v = reflect.Indirect(v)
	if !v.IsValid() {
		panic("sqlz/reflectutil: reflect.Value is nil pointer")
	}

	if v.Kind() != reflect.Struct {
		panic("sqlz/reflectutil: reflect.Value must a struct, got " + v.Type().String())
	}

	if !v.CanAddr() {
		panic("sqlz/reflectutil: reflect.Value must be an addressable struct")
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

	fv := v.Field(index[0])
	fv = Init(fv)

	initNested(fv, index[1:])
}
