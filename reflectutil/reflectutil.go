package reflectutil

import (
	"fmt"
	"reflect"
)

// Type is similar to [reflect.Kind], but adds support for type of slices.
// [reflect.Func], [reflect.Chan], [reflect.Array] and [reflect.UnsafePointer] are considered Invalid.
// Nil is considered Primitive.
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

// TypeOfAny recursively returns the Type of arg, nil is considered Primitive.
func TypeOfAny(arg any) Type {
	return TypeOf(reflect.TypeOf(arg))
}

// TypeOf recursively returns the Type of t, nil is considered Primitive.
func TypeOf(t reflect.Type) Type {
	if t == nil {
		return Primitive
	}

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

// Deref recursively de-references a [reflect.Value], preserving nil pointers.
func Deref(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return v
		}
		return Deref(v.Elem())
	}

	return v
}

func DerefType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		return DerefType(t.Elem())
	}
	return t
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

// TypedValue returns v's value using typed functions,
// like Bool(), String(), etc; fallsback to Interface().
func TypedValue(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()

	case reflect.String:
		return v.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int())

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uint(v.Uint())

	case reflect.Float32, reflect.Float64:
		return v.Float()

	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes() // []byte
		}
	}

	return v.Interface()
}
