package reflectutil

import (
	"fmt"
	"reflect"
)

type Type uint8

const (
	Invalid   Type = 0
	Primitive Type = 1 << iota
	Map
	Struct
	Slice
	SlicePrimitive = Slice | Primitive
	SliceMap       = Slice | Map
	SliceStruct    = Slice | Struct
)

func TypeOfAny(v any) Type {
	return TypeOf(reflect.TypeOf(v))
}

func TypeOf(t reflect.Type) Type {
	switch t.Kind() {
	case reflect.Map:
		return Map

	case reflect.Struct:
		return Struct

	case reflect.Slice:
		if et := TypeOf(t.Elem()); et > 0 {
			return Slice | et
		}

	case reflect.Pointer:
		return TypeOf(t.Elem())

	case
		reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String,
		reflect.Interface:
		return Primitive
	}

	return Invalid
}

func (t Type) String() string {
	switch t {
	case Invalid:
		return "invalid"
	case Primitive:
		return "primitive"
	case Map:
		return "map"
	case Struct:
		return "struct"
	case Slice:
		return "slice"
	case SlicePrimitive:
		return "[]primitive"
	case SliceMap:
		return "[]map"
	case SliceStruct:
		return "[]struct"
	}

	panic(fmt.Errorf("sqlz/reflectutil: unexpected type %d", t))
}

// Deref recursively de-references a [reflect.Value], nil pointers are preserved.
func Deref(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return v
		}
		return Deref(v.Elem())
	}

	return v
}

func IsNilStruct(v reflect.Value) bool {
	if v.Kind() != reflect.Pointer {
		return false
	}

	return v.IsNil() && v.Type().Elem().Kind() == reflect.Struct
}

func IsNilMap(v reflect.Value) bool {
	return v.Kind() == reflect.Map && v.IsNil()
}
