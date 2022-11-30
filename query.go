package structql

import (
	"fmt"
	"github.com/viant/sqlx/metadata/ast/parser"
	"github.com/viant/sqlx/metadata/ast/query"
	sparser "github.com/viant/structql/parser"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//Query represents a selector
type Query struct {
	query     string
	sel       *query.Select
	source    reflect.Type
	destSlice *xunsafe.Slice
	node      *Node
	mapper    *Mapper
	walker    *Walker
	CompType  reflect.Type
}

//Type returns dest slice type
func (s *Query) Type() reflect.Type {
	return s.destSlice.Type
}

//Select returns selection result
func (s *Query) Select(source interface{}) (interface{}, error) {
	destSlicePtrValue := reflect.New(s.destSlice.Type)
	sourceLen := s.walker.Count(source)
	destSlicePtrValue.Elem().Set(reflect.MakeSlice(s.destSlice.Type, 0, sourceLen))
	destSlicePtr := destSlicePtrValue.Interface()
	destPtr := xunsafe.AsPointer(destSlicePtr)
	appender := s.destSlice.Appender(destPtr)
	if err := s.mapper.Map(s.walker, source, appender); err != nil {
		return nil, err
	}
	return destSlicePtr, nil
}

//First returns the first selection result
func (s *Query) First(source interface{}) (interface{}, error) {
	destSlicePtrValue := reflect.New(s.destSlice.Type)
	sourceLen := s.walker.Count(source)
	destSlicePtrValue.Elem().Set(reflect.MakeSlice(s.destSlice.Type, 0, sourceLen))
	destSlicePtr := destSlicePtrValue.Interface()
	destPtr := xunsafe.AsPointer(destSlicePtr)
	appender := s.destSlice.Appender(destPtr)
	if err := s.mapper.Map(s.walker, source, appender); err != nil {
		return nil, err
	}
	if s.destSlice.Len(destPtr) == 0 {
		return nil, nil
	}
	return s.destSlice.ValuePointerAt(destPtr, 0), nil
}

func unwrapStruct(p reflect.Type) reflect.Type {
	if p == nil {
		return nil
	}
	switch p.Kind() {
	case reflect.Struct:
		return p
	case reflect.Ptr:
		return unwrapStruct(p.Elem())
	case reflect.Slice:
		return unwrapStruct(p.Elem())
	}
	return nil
}

//NewQuery returns a selector
func NewQuery(query string, source, dest reflect.Type) (*Query, error) {
	var err error
	if unwrapStruct(source) == nil {
		return nil, fmt.Errorf("invalid source type: %s", source.String())
	}
	ret := &Query{query: query, source: source}
	if ret.sel, err = parser.ParseQuery(query); err != nil {
		return nil, err
	}
	from := strings.Trim(parser.Stringify(ret.sel.From.X), "`")
	sel, err := sparser.ParseSelector(from)
	if err != nil {
		return nil, err
	}
	if ret.node, err = NewNode(source, sel); err != nil {
		return nil, err
	}
	src := unwrapStruct(ret.node.LeafType())
	if ret.mapper, err = NewMapper(src, unwrapStruct(dest), ret.sel); err != nil {
		return nil, err
	}
	ret.walker = NewWalker(ret.node)
	ret.CompType = ret.mapper.dest
	if dest == nil {
		dest = reflect.PtrTo(ret.CompType)
	}
	if dest.Kind() != reflect.Slice {
		dest = reflect.SliceOf(dest)
	}
	ret.destSlice = xunsafe.NewSlice(dest)

	if ret.sel.Qualify != nil {
		leaf := ret.node.Leaf()
		if leaf.expr != nil {
			return nil, fmt.Errorf("[] expr and WHERE clause can not be used for the same node")
		}
		if leaf.expr, leaf.exprSel, err = compileCriteria("t", ret.sel.Qualify, leaf.ownerType); err != nil {
			return nil, err
		}
	}
	return ret, nil
}
