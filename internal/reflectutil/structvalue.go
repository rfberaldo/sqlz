package reflectutil

import (
	"reflect"
	"strings"
)

// StructValue abstracts [reflect.Value] of struct kind, adds caching and finding field by struct tags.
// A new StructValue should be created for each struct type, otherwise caching will panic.
type StructValue struct {
	tag        string
	nameMapper func(string) string
	indexByKey map[string][]int
}

func NewStructValue(tag string, nameMapper func(string) string) *StructValue {
	return &StructValue{tag, nameMapper, make(map[string][]int)}
}

// FieldByTagName recursively finds a field in a struct by tag or name that satisfies match func.
// Key will be used to find the field, and also for caching, should be unique.
// Returns a zeroed [reflect.Value] if not found.
// Panics if StructValue is not a struct.
func (v *StructValue) FieldByTagName(key string, rval *reflect.Value) reflect.Value {
	// if index, ok := v.indexByKey[key]; ok {
	// 	return (*rval).FieldByIndex(index)
	// }

	matcher := func(s string) bool {
		return s == key || v.nameMapper(s) == key
	}
	fv, _ := walkStruct(v.tag, rval, matcher, []int{})
	// if len(index) > 0 {
	// 	v.indexByKey[key] = index
	// }
	return fv
}

func walkStruct(tag string, rval *reflect.Value, match func(string) bool, index []int) (reflect.Value, []int) {
	for i := range rval.NumField() {
		field := rval.Type().Field(i)
		fieldValue := rval.Field(i)

		if !field.IsExported() {
			continue
		}

		name := FieldName(field, tag)

		if match(name) {
			index = append(index, i)
			return fieldValue, index
		}

		fieldValue = DerefValue(fieldValue)

		// create instance in case of nil struct, this makes impossible to use caching
		if IsNilStruct(fieldValue) {
			fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
			fieldValue = DerefValue(fieldValue)
		}

		if fieldValue.Kind() == reflect.Struct {
			index = append(index, i)
			if v, idx := walkStruct(tag, &fieldValue, match, index); v.IsValid() {
				return v, idx
			}
			index = index[:len(index)-1]
		}
	}

	return reflect.Value{}, []int{}
}

// FieldName extracts the name for a struct field, prioritizing tag.
func FieldName(field reflect.StructField, tag string) string {
	tagValue := field.Tag.Get(tag)

	if tagValue != "-" && tagValue != "" {
		// check for possible comma as in "...,omitempty"
		commaIdx := strings.Index(tagValue, ",")

		if commaIdx == -1 {
			return tagValue
		}

		if tagValue[:commaIdx] != "" {
			return tagValue[:commaIdx]
		}
	}

	return field.Name
}
