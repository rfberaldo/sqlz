package reflectutil

import (
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

func TypeOf(v any) Type {
	return typeOf(reflect.TypeOf(v))
}

func typeOf(t reflect.Type) Type {
	switch t.Kind() {
	case reflect.Map:
		return Map

	case reflect.Struct:
		return Struct

	case reflect.Slice:
		if et := typeOf(t.Elem()); et > 0 {
			return Slice | et
		}

	case reflect.Pointer:
		return typeOf(t.Elem())

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

// DerefValue de-references a [reflect.Value], nil pointers are preserved.
func DerefValue(v reflect.Value) reflect.Value {
	if k := v.Kind(); k == reflect.Pointer || k == reflect.Interface {
		if v.IsNil() {
			return v
		}
		return DerefValue(v.Elem())
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
