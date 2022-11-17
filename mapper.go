package structql

import (
	"fmt"
	"github.com/viant/sqlx/metadata/ast/expr"
	"github.com/viant/sqlx/metadata/ast/query"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type mapKind int

const (
	mapKindDirect = mapKind(iota)
	mapKindCast
	mapKindExpr
)

type (
	//Mapper represents struct mapper
	Mapper struct {
		fields []field
		dest   reflect.Type
	}

	field struct {
		mapKind
		src  *xunsafe.Field
		dest *xunsafe.Field
	}
)

//Map maps source to appender
func (m *Mapper) Map(walker *Walker, source interface{}, appender *xunsafe.Appender) error {
	return walker.mapNode(m, walker.root, source, appender)
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
	//TODO analize kind
	value := f.src.Interface(src)
	f.dest.SetValue(dest, value)
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
		if !hasDest {
			if fieldMap.src.Tag != "" {
				tag := string(fieldMap.src.Tag)
				//TODO detect case format and replace accordingly
				tag = strings.ReplaceAll(tag, fieldMap.src.Name, item.Alias)
				fieldMap.src.Tag = reflect.StructTag(tag)
			}
			destFields = append(destFields, reflect.StructField{Name: item.Alias, Type: fieldMap.src.Type, Tag: fieldMap.src.Tag, PkgPath: fieldMap.src.PkgPath()})
			dest = reflect.StructOf(destFields)
		}
		if err := mapDestField(dest, item, fieldMap); err != nil {
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
	default:
		return fmt.Errorf("mapping not supported yet: %T", actual)
	}
	return nil
}

func mapDestField(source reflect.Type, item *query.Item, fieldMap *field) error {
	if fieldMap.dest = xunsafe.FieldByName(source, item.Alias); fieldMap.src == nil {
		return fmt.Errorf("failed to lookup dest field: '%s' at %s", item.Alias, source.String())
	}
	return nil
}
