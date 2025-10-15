package reflectutil

import (
	"reflect"
	"strings"
)

// structMapper is a helper to map struct fields index by tag/name.
type structMapper struct {
	tag        string
	sep        string
	nameMapper func(string) string
	indexByKey map[string][]int
}

// StructFieldMap maps the structType fields, tag is the struct tag to search for,
// sep is the sepatator for nested structs, and nameMapper transforms the
// field name in case the tag was not found.
func StructFieldMap(structType reflect.Type, tag, sep string, nameMapper func(string) string) map[string][]int {
	structType = Deref(structType)
	if structType.Kind() != reflect.Struct {
		panic("sqlz/reflectutil: reflect.Type must be a struct, got " + structType.String())
	}

	sm := &structMapper{tag, sep, nameMapper, make(map[string][]int)}
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

const maxCircular = 10

// traverse maps the struct field indexes, using BFS algorithm starting on t.
func (sm *structMapper) traverse(t reflect.Type) {
	visited := make(map[reflect.Type]int8)
	queue := append(
		make([]node, 0, t.NumField()),
		node{t, make([]string, 0, 1), make([]int, 0, 1)},
	)

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		if count := visited[parent.t]; count == maxCircular {
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

			name, inline := fieldTag(field, sm.tag)
			if name == "" {
				name = sm.nameMapper(field.Name)
			}

			curr.index = append(curr.index, field.Index...)
			if !field.Anonymous && !inline {
				curr.path = append(curr.path, name)

				key := strings.Join(curr.path, sm.sep)
				if _, exists := sm.indexByKey[key]; !exists {
					sm.indexByKey[key] = curr.index
				}
			}

			if fieldType.Kind() == reflect.Struct {
				queue = append(queue, curr)
			}
		}
	}
}

func fieldTag(field reflect.StructField, structTag string) (tag string, inline bool) {
	tag = field.Tag.Get(structTag)

	// test with >= 1 in case of a tag named "inline"
	inline = strings.LastIndex(tag, "inline") >= 1

	// check for possible comma as in "...,omitempty"
	if i := strings.Index(tag, ","); i > -1 {
		tag = tag[:i]
	}

	// don't want to ignore "-" like [json.Marshall], some users may use "json"
	// tag but still want to scan from database.
	if tag == "-" {
		return "", inline
	}

	return tag, inline
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
