package sql

import (
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type (
	field struct {
		*xunsafe.Field
		index int
	}
	mapper struct {
		byPos  map[int]*field
		byName map[string]*field
		values map[int]interface{}
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

func newMapper(recordType reflect.Type, list query.List, args *[]driver.NamedValue) (*mapper, error) {
	m := &mapper{byPos: make(map[int]*field), byName: make(map[string]*field), values: make(map[int]interface{})}

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
		case *expr.Literal:
			if err := m.updatedLiteralExpr(actual, i); err != nil {
				return nil, err
			}
			fuzzName := item.Alias
			pos, ok := fieldPos[fuzzName]
			if !ok {
				return nil, fmt.Errorf("unable to match column: %v in type: %s", item.Alias, recordType.Name())
			}
			m.byPos[i] = &field{index: pos, Field: xunsafe.NewField(recordType.Field(pos))}
		case *expr.Call:

			name := strings.ToLower(sqlparser.Stringify(actual.X))
			switch name {
			case "cast":
				err := m.handleCast(actual, args, item, i)
				if err != nil {
					return nil, err
				}
			case "now", "current_timestamp":
				m.values[i] = time.Now()

			default:
				return nil, fmt.Errorf("unsupported function: %v", name)
			}
			fuzzName := item.Alias
			pos, ok := fieldPos[fuzzName]
			if !ok {
				return nil, fmt.Errorf("unable to match column: %v in type: %s", item.Alias, recordType.Name())
			}
			m.byPos[i] = &field{index: pos, Field: xunsafe.NewField(recordType.Field(pos))}

		default:
			return nil, fmt.Errorf("unsupported ast type: %T", actual)
		}
	}
	return m, nil
}

func (m *mapper) handleCast(actual *expr.Call, args *[]driver.NamedValue, item *query.Item, i int) error {
	raw := strings.ToLower(actual.Raw)
	if len(actual.Args) != 1 {
		return fmt.Errorf("unsupported cast argument: %v", actual.Args)
	}
	if idx := strings.LastIndex(raw, " as "); idx != -1 {
		raw = strings.TrimSpace(raw[idx+4 : len(raw)-1])
	}

	switch raw {
	case "char":
		if _, ok := actual.Args[0].(*expr.Placeholder); ok {
			if len(*args) == 0 {
				return fmt.Errorf("missing cast argument %v", item.Alias)
			}

			switch (*args)[0].Value.(type) {
			case string:
				m.values[i] = (*args)[0].Value.(string)
			case time.Time:
				m.values[i] = (*args)[0].Value.(time.Time).Format(time.RFC3339)
			case *time.Time:
				if ts := (*args)[0].Value; ts != nil {
					m.values[i] = ts.(*time.Time).Format(time.RFC3339)
				}

			default:
				m.values[i] = (*args)[0].Value
			}
			*args = (*args)[1:]
		}
	case "int":
		if _, ok := actual.Args[0].(*expr.Placeholder); ok {
			if len(*args) == 0 {
				return fmt.Errorf("missing cast argument %v", item.Alias)
			}
			intValue := 0
			var err error
			switch (*args)[0].Value.(type) {
			case int:
				intValue = (*args)[0].Value.(int)
			case int64:
				intValue = int((*args)[0].Value.(int64))
			case string:
				if intValue, err = strconv.Atoi((*args)[0].Value.(string)); err != nil {
					return fmt.Errorf("%v invalid int: %v %w", item.Alias, (*args)[0].Value, err)
				}
			default:
				return fmt.Errorf("%v unsupported int argument type: %T", item.Alias, (*args)[0].Value)
			}
			m.values[i] = intValue
			*args = (*args)[1:]
		} else {
			return fmt.Errorf("%v unsupported cast argument type: %T", item.Alias, actual.Args[0])
		}
	case "bool":
		if _, ok := actual.Args[0].(*expr.Placeholder); ok {
			if len(*args) == 0 {
				return fmt.Errorf("missing cast argument %v", item.Alias)
			}
			boolValue := false
			var err error
			switch (*args)[0].Value.(type) {
			case bool:
				boolValue = (*args)[0].Value.(bool)
			case string:
				if boolValue, err = strconv.ParseBool((*args)[0].Value.(string)); err != nil {
					return fmt.Errorf("%v invalid bool: %v %w", item.Alias, (*args)[0].Value, err)
				}
			default:
				return fmt.Errorf("%v unsupported int argument type: %T", item.Alias, (*args)[0].Value)
			}
			m.values[i] = boolValue
			*args = (*args)[1:]
		} else {
			return fmt.Errorf("%v unsupported cast argument type: %T", item.Alias, actual.Args[0])
		}
	case "time", "datetime", "timestamp":
		if _, ok := actual.Args[0].(*expr.Placeholder); ok {
			if len(*args) == 0 {
				return fmt.Errorf("missing cast argument %v", item.Alias)
			}

			var tValue *time.Time
			switch (*args)[0].Value.(type) {
			case time.Time:
				t := (*args)[0].Value.(time.Time)
				tValue = &t
			case *time.Time:
				tValue = (*args)[0].Value.(*time.Time)

			case string:
				t, err := time.Parse(time.RFC3339, (*args)[0].Value.(string))
				if err != nil {
					return fmt.Errorf("%v invalid time: %v %w", item.Alias, (*args)[0].Value, err)
				}
				tValue = &t
			default:
				return fmt.Errorf("%v unsupported int argument type: %T", item.Alias, (*args)[0].Value)
			}
			if tValue == nil {
				m.values[i] = nil
			} else {
				m.values[i] = *tValue
			}
			*args = (*args)[1:]
		} else {
			return fmt.Errorf("%v unsupported cast argument type: %T", item.Alias, actual.Args[0])
		}

	case "float":
		if _, ok := actual.Args[0].(*expr.Placeholder); ok {
			if len(*args) == 0 {
				return fmt.Errorf("missing cast argument %v", item.Alias)
			}

			floatValue := 0.0
			var err error
			switch (*args)[0].Value.(type) {
			case int:
				floatValue = (*args)[0].Value.(float64)
			case string:
				if floatValue, err = strconv.ParseFloat((*args)[0].Value.(string), 64); err != nil {
					return fmt.Errorf("%v invalid float: %v %w", item.Alias, (*args)[0].Value, err)
				}
			default:
				return fmt.Errorf("%v unsupported int argument type: %T", item.Alias, (*args)[0].Value)
			}
			m.values[i] = floatValue
			*args = (*args)[1:]
		} else {
			return fmt.Errorf("%v unsupported cast argument type: %T", item.Alias, actual.Args[0])
		}

	default:
		return fmt.Errorf("unsupported cast type: %v", raw)
	}
	return nil
}

func (m *mapper) updatedLiteralExpr(actual *expr.Literal, i int) error {
	actual.Value = strings.Trim(actual.Value, `"`)
	switch actual.Kind {
	case "string":
		m.values[i] = actual.Value
	case "int":
		value, err := strconv.Atoi(actual.Value)
		if err != nil {
			return err
		}
		m.values[i] = value
	case "float":
		value, err := strconv.ParseFloat(actual.Value, 64)
		if err != nil {
			return err
		}
		m.values[i] = value
	case "bool":
		value, err := strconv.ParseBool(actual.Value)
		if err != nil {
			return err
		}
		m.values[i] = value
	}
	return nil
}
