package sql

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type (
	field struct {
		*xunsafe.Field
		index int
	}
	mapper struct {
		byPos  map[int]*field
		byName map[string]*field
	}
)

func (m *mapper) lookup(name string) *xunsafe.Field {
	ret, ok := m.byName[name]
	if !ok {
		name = strings.ReplaceAll(strings.ToLower(name), "_", "")
		ret, _ = m.byName[name]
	}
	if ret == nil {
		return nil
	}
	return ret.Field
}

func newMapper(recordType reflect.Type, list query.List) (*mapper, error) {
	m := &mapper{byPos: make(map[int]*field), byName: make(map[string]*field)}
	if list.IsStarExpr() {
		for i := 0; i < recordType.NumField(); i++ {
			aField := recordType.Field(i)
			m.byPos[i] = &field{index: i, Field: xunsafe.NewField(aField)}
			fuzzName := strings.ReplaceAll(strings.ToLower(aField.Name), "_", "")
			m.byName[aField.Name] = &field{index: i, Field: xunsafe.NewField(aField)}
			m.byName[fuzzName] = &field{index: i, Field: xunsafe.NewField(aField)}
		}
		return m, nil
	}

	fieldPos := map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		aField := recordType.Field(i)
		fieldPos[aField.Name] = i
		fuzzName := strings.ReplaceAll(strings.ToLower(aField.Name), "_", "")
		fieldPos[fuzzName] = i
		m.byName[aField.Name] = &field{index: i, Field: xunsafe.NewField(aField)}
		m.byName[fuzzName] = &field{index: i, Field: xunsafe.NewField(aField)}
	}

	for i := 0; i < len(list); i++ {
		item := list[i]
		switch actual := item.Expr.(type) {
		case *expr.Ident:
			if pos, ok := fieldPos[actual.Name]; ok {
				m.byPos[i] = &field{index: pos, Field: xunsafe.NewField(recordType.Field(pos))}
				continue
			}
			fuzzName := strings.ReplaceAll(strings.ToLower(actual.Name), "_", "")
			pos, ok := fieldPos[fuzzName]
			if !ok {
				return nil, fmt.Errorf("unable to match column: %v in type: %s", actual.Name, recordType.Name())
			}
			m.byPos[i] = &field{index: pos, Field: xunsafe.NewField(recordType.Field(pos))}
		}
	}
	return m, nil
}
