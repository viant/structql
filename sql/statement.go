package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/x"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

var maxWaitTime = 30 * time.Second

// Statement abstraction implements database/sql driver.Statement interface
type (
	Statement struct {
		fs         afs.Service
		BaseURL    string
		SQL        string
		Kind       sqlparser.Kind
		types      *x.Registry
		query      *query.Select
		recordType reflect.Type
		mapper     map[int]int
		numInput   int
	}
)

// Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	named := asNamedValues(args)
	return s.ExecContext(context.Background(), named)
}

// ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch s.Kind {
	case sqlparser.KindRegisterType:
		return s.handleRegisterType(args)
	case sqlparser.KindSelect:
		return nil, fmt.Errorf("unsupported query type: %v", s.Kind)
	}
	return nil, nil
}

func (s *Statement) prepareSelect(SQL string) error {
	var err error
	if s.query, err = sqlparser.ParseQuery(SQL); err != nil {
		return err
	}

	rawExpr, ok := s.query.From.X.(*expr.Raw)
	if ok {
		s.remapSubQuery(rawExpr)
	}

	return nil
}

func (s *Statement) remapSubQuery(rawExpr *expr.Raw) {
	subQuery := rawExpr.X.(*query.Select)
	if s.query.List.IsStarExpr() {
		s.query.List = subQuery.List
	} else {
		var columMap = make(map[string]*query.Item)
		for _, item := range subQuery.List {
			name := itemName(item)
			columMap[name] = item
		}
		list := make(query.List, 0)
		for i := range s.query.List {
			item := s.query.List[i]
			name := itemName(item)
			if col, ok := columMap[name]; ok {
				list = append(list, col)
			} else {
				list = append(list, item)
			}
		}
		s.query.List = list
	}
	s.query.From = subQuery.From
	if s.query.Qualify == nil {
		s.query.Qualify = subQuery.Qualify
	}
	if s.query.Limit == nil {
		s.query.Limit = subQuery.Limit
	}
	if s.query.GroupBy == nil {
		s.query.GroupBy = subQuery.GroupBy
	}
	if s.query.Qualify == nil {
		s.query.Qualify = subQuery.Qualify
	}
}

func itemName(item *query.Item) string {
	name := item.Alias
	if name == "" {
		name = sqlparser.Stringify(item.Expr)
		if idx := strings.Index(name, "."); idx != -1 {
			name = name[idx+1:]
		}
	}
	return strings.ToLower(name)
}

func (s *Statement) handleRegisterType(args []driver.NamedValue) (driver.Result, error) {
	register, err := sqlparser.ParseRegisterType(s.SQL)
	if err != nil {
		return nil, err
	}
	spec := strings.TrimSpace(register.Spec)
	var rType reflect.Type
	if spec == "?" {
		rType = reflect.TypeOf(args[0].Value)
		if rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
	} else {
		aType := xreflect.NewType(register.Name, xreflect.WithTypeDefinition(spec))
		if rType, err = aType.LoadType(xreflect.NewTypes()); err != nil {
			return nil, err
		}
	}
	aType := x.NewType(rType, x.WithName(register.Name))
	aType.PkgPath = ""
	if register.Global {
		Register(aType)
	}
	s.types.Register(aType)
	return &result{}, nil
}

// Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	named := asNamedValues(args)
	return s.QueryContext(context.TODO(), named)
}

func asNamedValues(args []driver.Value) []driver.NamedValue {
	var named []driver.NamedValue
	for i := range args {
		named = append(named, driver.NamedValue{Ordinal: i, Value: args[i]})
	}
	return named
}

// QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	switch s.Kind {
	case sqlparser.KindSelect:
	default:
		return nil, fmt.Errorf("unsupported query type: %v", s.Kind)
	}

	var res resources
	var name string
	switch actual := s.query.From.X.(type) {
	case *expr.Ident:
		URL := url.Join(s.BaseURL, actual.Name)
		name = actual.Name
		if res, err = newResources(ctx, withFS(s.fs), withURL(URL), withArgs(args)); err != nil {
			return nil, err
		}
	case *expr.Parenthesis:

	case *expr.Call:

	}

	return s.executeSelect(ctx, name, res, args)
}

// CheckNamedValue checks supported globalTypes (all for now)
func (s *Statement) CheckNamedValue(named *driver.NamedValue) error {
	return nil
}

// NumInput returns numinput
func (s *Statement) NumInput() int {
	return s.numInput
}

// Close closes statement
func (s *Statement) Close() error {

	return nil
}

func (s *Statement) checkQueryParameters() {
	//this is very basic parameter detection, need to be improved
	aQuery := strings.ToLower(s.SQL)
	count := checkQueryParameters(aQuery)
	s.numInput = count
}

func (s *Statement) executeSelect(ctx context.Context, name string, resources resources, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	if aType := s.types.Lookup(name); aType != nil {
		s.recordType = aType.Type
	} else {
		if s.recordType, err = s.autodetectType(ctx, resources); err != nil {
			return nil, err
		}
	}
	criteria := ""
	falsePredicate := false
	if s.query.Qualify != nil && s.query.Qualify.X != nil {
		criteria = sqlparser.Stringify(s.query.Qualify.X)
		falsePredicate = isFalsePredicate(s.query.Qualify)
	}

	aMapper, err := newMapper(s.recordType, s.query.List, &args)
	if err != nil {
		return nil, err
	}

	row := reflect.New(s.recordType).Interface()
	rows := &Rows{
		zeroRecord:       unsafe.Slice((*byte)(xunsafe.AsPointer(row)), s.recordType.Size()),
		record:           row,
		recordType:       s.recordType,
		isFalsePredicate: falsePredicate,
		resources:        resources,
		mapper:           aMapper,
		query:            s.query,
	}

	if criteria != "" {
		if err = rows.initCriteria(s.query.Qualify, args); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (s *Statement) autodetectType(ctx context.Context, res resources) (reflect.Type, error) {
	if s.query.List.IsStarExpr() {
		return nil, fmt.Errorf("autodetectType not implemented for *")
	}
	var fields = make([]reflect.StructField, 0)
	for _, item := range s.query.List {
		var field reflect.StructField
		switch actual := item.Expr.(type) {
		case *expr.Literal:
			switch actual.Kind {
			case "string":
				field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf("")}
			case "int":
				field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(0)}
			case "float":
				field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(0.0)}
			}
		case *expr.Call:
			name := strings.ToLower(sqlparser.Stringify(actual.X))
			switch name {
			case "cast":
				raw := strings.ToLower(actual.Raw)
				if idx := strings.LastIndex(raw, " as "); idx != -1 {
					raw = strings.TrimSpace(raw[idx+4 : len(raw)-1])
				}
				switch raw {
				case "char":
					field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf("")}
				case "int":
					field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(0)}
				case "bool":
					field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(true)}
				case "float":
					field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(0.0)}
				default:
					return nil, fmt.Errorf("unsupported cast type: %v", raw)
				}
			case "now", "current_timestamp":
				field = reflect.StructField{Name: item.Alias, Type: reflect.TypeOf(time.Time{})}
			default:
				return nil, fmt.Errorf("unsupported function: %v", name)
			}
		default:
			return nil, fmt.Errorf("unsupported type: %T", actual)
		}
		fields = append(fields, field)
	}
	return reflect.StructOf(fields), nil
}

func checkQueryParameters(query string) int {
	count := 0
	inQuote := false
	for i, c := range query {
		switch c {
		case '\'':
			if i > 1 && inQuote && query[i-1] == '\\' {
				continue
			}
			inQuote = !inQuote
		case '?', '@':
			if !inQuote {
				count++
			}
		}
	}
	return count
}

func isFalsePredicate(qualify *expr.Qualify) bool {
	binary, ok := qualify.X.(*expr.Binary)
	if !ok {
		return false
	}
	if binary.Op == "=" {
		if leftLiteral, ok := binary.X.(*expr.Literal); ok {
			if rightLiteral, ok := binary.Y.(*expr.Literal); ok {
				return !(leftLiteral.Value == rightLiteral.Value)

			}
		}
	}
	return false
}
