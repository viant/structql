package structql

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type field struct {
	mapKind   mapKind
	src       *xunsafe.Field
	dest      *xunsafe.Field
	aggregate bool
	cp        func(src, dest unsafe.Pointer)
}

func (f *field) configure() error {
	if f.dest.Kind() == f.src.Kind() {
		f.mapKind = mapKindDirect

		switch f.dest.Kind() {
		case reflect.String, reflect.Int, reflect.Int64, reflect.Float64, reflect.Float32, reflect.Bool:
			f.mapKind = mapKindDirectPrimitive
		}
		return nil
	}
	return f.computeCastedCopy()
}

func (f *field) translateInt(src, dest unsafe.Pointer) {
	*(*int)(dest) = *(*int)(src)
}

func (f *field) translateIntToIntPtr(src, dest unsafe.Pointer) {
	srcValue := *(*int)(src)
	*(**int)(dest) = &srcValue
}

func (f *field) translateIntPtrToInt(src unsafe.Pointer, dest unsafe.Pointer) {
	srcValue := *(**int)(src)
	if srcValue == nil {
		return
	}
	*(*int)(dest) = *srcValue
}

func (f *field) translateIntPtrToIntPtr(src unsafe.Pointer, dest unsafe.Pointer) {
	srcValue := *(**int)(src)
	if srcValue == nil {
		return
	}
	*(**int)(dest) = *(**int)(src)
}

func (f *field) translateIntToInts(src unsafe.Pointer, dest unsafe.Pointer) {
	srcValue := *(*int)(src)
	destSlice := (*[]int)(dest)
	*destSlice = append(*destSlice, srcValue)
}

func (f *field) translateStringToStrings(src unsafe.Pointer, dest unsafe.Pointer) {
	srcValue := *(*string)(src)
	destSlice := (*[]string)(dest)
	*destSlice = append(*destSlice, srcValue)
}

func (f *field) computeCastedCopy() error {
	f.mapKind = mapKindTranslate
	srcKind := f.src.Kind()
	destKind := f.dest.Kind()
	isSourcePtr := srcKind == reflect.Ptr
	if isSourcePtr {
		srcKind = f.src.Elem().Kind()
	}
	isDestPtr := destKind == reflect.Ptr
	if isSourcePtr {
		destKind = f.dest.Elem().Kind()
	}
	switch destKind {
	case reflect.Slice:
		switch f.dest.Type.Elem().Kind() {
		case reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64:
			f.cp = f.translateIntToInts
		case reflect.String:
			f.cp = f.translateStringToStrings
		}

	case reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64:
		switch srcKind {
		case reflect.Int, reflect.Uint, reflect.Int64, reflect.Uint64:
			if !isSourcePtr && !isDestPtr {
				f.cp = f.translateInt
			} else if isDestPtr {
				f.cp = f.translateIntToIntPtr
			} else if isSourcePtr {
				f.cp = f.translateIntPtrToInt
			} else {
				f.cp = f.translateIntPtrToIntPtr
			}

		case reflect.Float64:
		case reflect.Float32:
		case reflect.String:

		}
	case reflect.String:
	case reflect.Bool:
	case reflect.Float64:
	case reflect.Float32:

	}
	if f.cp == nil {
		return fmt.Errorf("unsupported structology field translation %s -> %s", f.src.Type.String(), f.dest.Type.String())
	}
	return nil
}
