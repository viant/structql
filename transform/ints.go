package transform

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
)

func AsInts(target reflect.Type, name string) (func(value interface{}) []int, error) {
	switch target.Kind() {
	case reflect.Slice:
		xSlice := xunsafe.NewSlice(target)
		switch target.Elem().Kind() {
		case reflect.Ptr:
			switch target.Elem().Elem().Kind() {
			case reflect.Struct:
				xField := xunsafe.FieldByName(target.Elem().Elem(), name)
				if xField == nil {
					return nil, fmt.Errorf("failed to lookup field: %s on %s", name, target.Elem().Elem().String())
				}
				return func(value interface{}) []int {
					slicePtr := xunsafe.AsPointer(value)
					if xSlice.Len(slicePtr) == 0 {
						return nil
					}
					item := xSlice.ValuePointerAt(slicePtr, 0)
					itemPtr := xunsafe.AsPointer(item)
					result := xField.Interface(itemPtr)
					return result.([]int)
				}, nil
			}
		case reflect.Struct:
			xField := xunsafe.FieldByName(target.Elem(), name)
			if xField == nil {
				return nil, fmt.Errorf("failed to lookup field: %s on %s", name, target.Elem().String())
			}
			return func(value interface{}) []int {
				slicePtr := xunsafe.AsPointer(value)
				if xSlice.Len(slicePtr) == 0 {
					return nil
				}
				item := xSlice.ValuePointerAt(slicePtr, 0)
				itemPtr := xunsafe.AsPointer(item)
				result := xField.Interface(itemPtr)
				return result.([]int)
			}, nil
		}

	case reflect.Struct:
		xField := xunsafe.FieldByName(target, name)
		if xField == nil {
			return nil, fmt.Errorf("failed to lookup field: %s on %s", name, target.String())
		}
		return func(value interface{}) []int {
			itemPtr := xunsafe.AsPointer(value)
			result := xField.Interface(itemPtr)
			return result.([]int)
		}, nil
	case reflect.Ptr:
		switch target.Elem().Kind() {
		case reflect.Struct:
			xField := xunsafe.FieldByName(target, name)
			if xField == nil {
				return nil, fmt.Errorf("failed to lookup field: %s on %s", name, target.String())
			}
			return func(value interface{}) []int {
				itemPtr := xunsafe.AsPointer(value)
				result := xField.Interface(itemPtr)
				return result.([]int)
			}, nil
		}
	}
	return nil, fmt.Errorf("failed to build AsInts: unsupported type: %s", target.String())
}
