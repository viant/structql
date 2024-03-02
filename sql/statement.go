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

	return nil
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
	aType.Package = ""
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
	query := strings.ToLower(s.SQL)
	count := checkQueryParameters(query)
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
	if s.query.Qualify != nil && s.query.Qualify.X != nil {
		criteria = sqlparser.Stringify(s.query.Qualify.X)
	}
	aMapper, err := newMapper(s.recordType, s.query.List)
	if err != nil {
		return nil, err
	}

	row := reflect.New(s.recordType).Interface()

	rows := &Rows{
		zeroRecord: unsafe.Slice((*byte)(xunsafe.AsPointer(row)), s.recordType.Size()),
		record:     row,
		recordType: s.recordType,
		resources:  resources,
		mapper:     aMapper,
		query:      s.query,
	}

	if criteria != "" {
		if err = rows.initCriteria(s.query.Qualify, args); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (s *Statement) autodetectType(ctx context.Context, res resources) (reflect.Type, error) {
	return nil, fmt.Errorf("autodetectType not yet implemented")
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
