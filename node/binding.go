package node

import (
	"fmt"
	"github.com/viant/structql/parser/in"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
)

type (
	// Value represents a value in a binding.
	Value struct {
		Position    int
		Placeholder bool
		Value       interface{}
	}

	//Group represents a group of values in a binding.
	Group struct {
		Name      string
		Type      reflect.Type
		From      int
		To        int
		InInts    *in.Ints
		InStrings *in.Strings
	}

	//Binding represents a binding of values and groups.

	Binding struct {
		Values       []*Value
		Groups       []*Group
		Count        int
		ContextField *xunsafe.Field
	}

	//Values represents a set of values and a binding.
	Values struct {
		Bindings *Binding
		Values   []interface{}
	}
)

// Count returns the number of values in a group.
func (g *Group) Count() int {
	return g.To - g.From
}

func (b *Binding) AddPlaceholder() {
	b.Groups = append(b.Groups, &Group{
		Name: b.ContextField.Name,
		Type: b.ContextField.Type,
		From: b.Count,
		To:   b.Count + 1,
	})
	b.Count++
}
func (b *Binding) AddPlaceholders(count int) *Group {
	b.Groups = append(b.Groups, &Group{
		Name: b.ContextField.Name,
		Type: b.ContextField.Type,
		From: b.Count,
		To:   b.Count + count,
	})
	b.Count += count
	return b.Groups[len(b.Groups)-1]
}

func (b *Binding) Expand(expr string, values []interface{}) (string, error) {
	index := 0
	if len(b.Groups) == 0 {
		return expr, nil
	}
	for _, group := range b.Groups {
		value := ""
		gType := group.Type
		if gType.Kind() == reflect.Ptr {
			gType = gType.Elem()
		}
		switch gType.Kind() {
		case reflect.String:
			value = fmt.Sprintf(`"%v"`, values[index])
		case reflect.Int:
			value = fmt.Sprintf(`%v`, values[index])
		default:
			return "", fmt.Errorf("unsupported binding type %v", gType)
		}
		expr = strings.Replace(expr, "?", value, 1)
		count := group.Count()
		if group.InInts != nil {
			groupValues := make([]int, 0, count)
			for i := index; i < index+count; i++ {
				intValue, err := asInt(values[i])
				if err != nil {
					return "", fmt.Errorf("invalid %v value %v %w", group.Name, values[i], err)
				}
				groupValues = append(groupValues, intValue)
			}
			group.InInts.Set(groupValues)
		}

		if group.InStrings != nil {
			groupValues := make([]string, 0, count)
			for i := index; i < index+count; i++ {
				groupValues = append(groupValues, asString(values[i]))
			}
			group.InStrings.Set(groupValues)
		}
		index += count
	}
	return expr, nil
}

func asString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return v
	case *string:
		if v == nil {
			return ""
		}
		return *v
	}
	return fmt.Sprintf("%v", i)
}

func asInt(value interface{}) (int, error) {
	intValue, ok := value.(int)
	if ok {
		return intValue, nil
	}
	ptrValue, ok := value.(*int)
	if ok {
		if ptrValue == nil {
			return 0, nil
		}
		return *ptrValue, nil
	}
	sValue, ok := value.(string)
	return strconv.Atoi(sValue)
}

func LookupFieldType(holder string, ownerType reflect.Type) func(name string) *xunsafe.Field {
	return func(name string) *xunsafe.Field {
		if name == holder {
			return nil
		}
		if ownerType.Kind() == reflect.Ptr {
			ownerType = ownerType.Elem()
		}
		return xunsafe.FieldByName(ownerType, name)
	}
}
