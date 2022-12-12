package structql

import (
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type mapKind int

const (
	mapKindUnspecified = mapKind(iota)
	mapKindDirectPrimitive
	mapKindDirect
	mapKindTranslate
	mapKindExpr
)

type (
	//Mapper represents struct mapper
	Mapper struct {
		fields    []field
		dest      reflect.Type
		aggregate bool
		groupBy   []string
	}
)

//Map maps source to appender
func (m *Mapper) Map(walker *Walker, source interface{}, appender *xunsafe.Appender) error {
	ctx := NewContext(m, appender, m.aggregate)
	return walker.mapNode(ctx, walker.root, source)
}

//MapStruct maps struct
func (m *Mapper) MapStruct(srcItemPtr unsafe.Pointer, destItemPtr unsafe.Pointer) error {
	switch len(m.fields) {
	case 1:
		m.fields[0].Map(srcItemPtr, destItemPtr)
		break
	case 2:
		m.fields[0].Map(srcItemPtr, destItemPtr)
		m.fields[1].Map(srcItemPtr, destItemPtr)
		break
	case 3:
		m.fields[0].Map(srcItemPtr, destItemPtr)
		m.fields[1].Map(srcItemPtr, destItemPtr)
		m.fields[2].Map(srcItemPtr, destItemPtr)
	default:
		for j := 0; j < len(m.fields); j++ {
			m.fields[j].Map(srcItemPtr, destItemPtr)
		}
	}
	return nil
}

//Map map fields
func (f *field) Map(src, dest unsafe.Pointer) {
	f.copy(src, dest)

}

func (f *field) copy(src unsafe.Pointer, dest unsafe.Pointer) {
	if f.mapKind == mapKindDirectPrimitive {
		source := f.src.Interface(src)
		f.dest.Set(dest, source)
		return
	}
	if f.mapKind == mapKindDirect {
		source := f.src.Interface(src)
		f.dest.SetValue(dest, source)
		return
	}
	f.translate(src, dest)
}

func (f *field) translate(source, dest unsafe.Pointer) {
	f.cp(source, dest)
}

//NewMapper creates a mapper
func NewMapper(source reflect.Type, dest reflect.Type, sel *query.Select) (*Mapper, error) {
	ret := &Mapper{fields: make([]field, len(sel.List))}
	hasDest := dest != nil
	var destFields []reflect.StructField
	for i := range sel.List {
		item := sel.List[i]
		fieldMap := &ret.fields[i]
		if err := mapSourceField(source, item, fieldMap); err != nil {
			return nil, err
		}
		if item.Alias == "" {
			item.Alias = fieldMap.src.Name
		}
		if fieldMap.aggregate {
			ret.aggregate = fieldMap.aggregate
		}

		if !hasDest {
			fieldName := item.Alias
			if fieldMap.src.Tag != "" {
				tag := string(fieldMap.src.Tag)
				//TODO detect case format and replace accordingly
				tag = strings.ReplaceAll(tag, fieldMap.src.Name, item.Alias)
				fieldMap.src.Tag = reflect.StructTag(tag)
			}
			fieldType := fieldMap.src.Type
			if fieldMap.dest != nil {
				fieldType = fieldMap.dest.Type
			}
			destFields = append(destFields, reflect.StructField{Name: fieldName, Type: fieldType, Tag: fieldMap.src.Tag, PkgPath: fieldMap.src.PkgPath()})
			dest = reflect.StructOf(destFields)
		}
		if err := mapDestField(dest, item, fieldMap); err != nil {
			return nil, err
		}

		if err := fieldMap.configure(); err != nil {
			return nil, err
		}
	}
	ret.dest = dest
	return ret, nil
}

func mapSourceField(source reflect.Type, item *query.Item, fieldMap *field) error {
	switch actual := item.Expr.(type) {
	case *expr.Selector:
		if fieldMap.src = xunsafe.FieldByName(source, actual.Name); fieldMap.src == nil {
			return fmt.Errorf("failed to lookup source field: '%s' at %s", actual.Name, source.String())
		}
	case *expr.Ident:
		if fieldMap.src = xunsafe.FieldByName(source, actual.Name); fieldMap.src == nil {
			return fmt.Errorf("failed to lookup source field: '%s' at %s", actual.Name, source.String())
		}
	case *expr.Call:
		funName := sqlparser.Stringify(actual.X)
		switch strings.ToUpper(funName) {
		case "ARRAY_AGG":
			fieldMap.aggregate = true
			args := actual.Args
			if len(args) != 1 {
				return fmt.Errorf("invalid ARRAY_AGG args count, %v, expected 1", len(args))
			}
			colName := sqlparser.Stringify(actual.Args[0])
			if fieldMap.src = xunsafe.FieldByName(source, colName); fieldMap.src == nil {
				return fmt.Errorf("failed to lookup source field: '%s' at %s", colName, source.String())
			}
			destName := item.Alias
			if item.Alias == "" {
				destName = fieldMap.src.Name
			}
			fieldMap.dest = &xunsafe.Field{Name: destName, Type: reflect.SliceOf(fieldMap.src.Type)}
		}

	default:
		return fmt.Errorf("mapping not supported yet: %T", actual)
	}
	return nil
}

func mapDestField(source reflect.Type, item *query.Item, fieldMap *field) error {
	if fieldMap.dest != nil {
		return nil
	}
	if fieldMap.dest = xunsafe.FieldByName(source, item.Alias); fieldMap.src == nil {
		return fmt.Errorf("failed to lookup dest field: '%s' at %s", item.Alias, source.String())
	}
	return nil
}
