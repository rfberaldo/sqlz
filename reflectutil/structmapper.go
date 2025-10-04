package reflectutil

import (
	"reflect"
	"strings"
)

// StructMapper is a helper to map struct fields with keys, usually column names.
type StructMapper struct {
	tag             string
	fieldNameMapper func(string) string
	indexByKey      map[string][]int
}

// NewStructMapper returns a StructMapper, tag is the [reflect.StructTag] used to find fields.
// Each struct type must have its own StructMapper, otherwise caching won't work properly.
// fieldNameMapper is used to process the name of the field, if the struct tag was not found.
func NewStructMapper(tag string, fieldNameMapper func(string) string) *StructMapper {
	if fieldNameMapper == nil {
		fieldNameMapper = func(s string) string { return strings.ToLower(s) }
	}

	return &StructMapper{tag, fieldNameMapper, make(map[string][]int)}
}

// FieldByKey returns the struct field with the given key, must match struct tag or name.
// Key is also used for caching, and should be unique, supports dot notation for nested structs.
// It returns the zero [reflect.Value] if not found.
// It panics if v is not a struct or pointer to struct.
func (sm *StructMapper) FieldByKey(key string, v reflect.Value) reflect.Value {
	v = reflect.Indirect(v)
	if !v.IsValid() {
		panic("sqlz: reflect.Value is a nil pointer")
	}
	if v.Kind() != reflect.Struct {
		panic("sqlz: reflect.Value must be a struct or pointer to struct")
	}

	if index, ok := sm.indexByKey[key]; ok {
		if fv, err := v.FieldByIndexErr(index); err == nil {
			return fv
		}
	}

	dotNotation := strings.ContainsRune(key, '.')
	matcher := func(path []string) bool {
		if !dotNotation {
			s := path[len(path)-1]
			return key == s || key == sm.fieldNameMapper(s)
		}

		keys := strings.Split(key, ".")
		if len(keys) != len(path) {
			return false
		}
		for i := range len(keys) {
			if keys[i] != path[i] && keys[i] != sm.fieldNameMapper(path[i]) {
				return false
			}
		}
		return true
	}

	sv := StructValue{v, make([]int, 0, 1), make([]string, 0, 1)}
	sv = walkStruct(sm.tag, sv, matcher)

	if len(sv.index) > 0 {
		sm.indexByKey[key] = sv.index
	}

	return sv.Value
}

type StructValue struct {
	reflect.Value
	index []int
	path  []string
}

func (sv *StructValue) append(i int, s string) {
	sv.index = append(sv.index, i)
	sv.path = append(sv.path, s)
}

func (sv *StructValue) pop() {
	sv.index = sv.index[:len(sv.index)-1]
	sv.path = sv.path[:len(sv.path)-1]
}

func walkStruct(tag string, sv StructValue, match func([]string) bool) StructValue {
	for i := range sv.NumField() {
		field := sv.Type().Field(i)
		fieldValue := StructValue{sv.Field(i), sv.index, sv.path}

		if !field.IsExported() {
			continue
		}

		name := FieldName(field, tag)

		if match(append(fieldValue.path, name)) {
			fieldValue.append(i, name)
			return fieldValue
		}

		fieldValue.Value = Deref(fieldValue.Value)

		// create instance in case of an addressable nil pointer
		if IsNilStruct(fieldValue.Value) && fieldValue.CanAddr() {
			fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
			fieldValue.Value = reflect.Indirect(fieldValue.Value)
		}

		if fieldValue.Kind() == reflect.Struct {
			fieldValue.append(i, name)
			if v := walkStruct(tag, fieldValue, match); v.IsValid() {
				return v
			}
			fieldValue.pop()
		}
	}

	return StructValue{}
}

// FieldName extracts the name for a struct field, prioritizing structTag.
func FieldName(field reflect.StructField, structTag string) string {
	tagValue, ok := field.Tag.Lookup(structTag)
	if !ok {
		return field.Name
	}

	// check for possible comma as in "...,omitempty"
	if i := strings.Index(tagValue, ","); i > -1 {
		tagValue = tagValue[:i]
	}

	if tagValue != "" && tagValue != "-" {
		return tagValue
	}

	return field.Name
}
