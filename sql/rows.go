package sql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/viant/igo"
	"github.com/viant/igo/exec"
	iexpr "github.com/viant/igo/exec/expr"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/structql/node"
	"github.com/viant/structql/parser"

	"github.com/viant/sqlparser/query"
	"github.com/viant/xunsafe"
	"gopkg.in/yaml.v3"
	"io"
	"reflect"
	"unsafe"
)

// Rows represents rows driver
type Rows struct {
	criteria       *iexpr.Bool
	scope          *igo.Scope
	recordSelector *exec.Selector
	recordType     reflect.Type
	mapper         *mapper
	query          *query.Select
	resources      []*resource
	resourceIndex  int
	zeroRecord     []byte
	record         interface{}
}

func (r *Rows) nextResource() bool {
	if r.resourceIndex >= len(r.resources) {
		return false
	}
	if r.resourceIndex > 0 {
		r.resources[r.resourceIndex].Close()
	}
	r.resourceIndex++
	return r.resourceIndex < len(r.resources)

}

func (r *Rows) resource() *resource {
	return r.resources[r.resourceIndex]
}

// Columns returns query columns
func (r *Rows) Columns() []string {
	var result []string
	if r.query.List.IsStarExpr() {
		for i := range r.mapper.byPos {
			result = append(result, r.recordType.Field(i).Name)
		}
		return result
	}
	for _, column := range r.query.List {
		switch actual := column.Expr.(type) {
		case *expr.Ident:
			result = append(result, actual.Name)
		case *expr.Literal:
			result = append(result, column.Alias)
		case *expr.Call:
			result = append(result, column.Alias)
		default:
			return nil
		}
	}
	return result
}

// Close closes rows
func (r *Rows) Close() error {
	for _, res := range r.resources {
		if res != nil {
			res.Close()
		}
	}
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	if r.resourceIndex >= len(r.resources) {
		return io.EOF
	}
	if len(dest) != len(r.mapper.byPos)+len(r.mapper.values) {
		return fmt.Errorf("expected %v, but had %v", len(r.mapper.byPos)+len(r.mapper.values), len(dest))
	}
	res := r.resource()
	has, err := res.Next()
	if err != nil {
		return err
	}
	if !has {
		if r.nextResource() {
			has, err = r.resource().Next()
			if err != nil {
				return err
			}
		}
	}
	if !has {
		return io.EOF
	}

	copy(unsafe.Slice((*byte)(xunsafe.AsPointer(r.record)), r.recordType.Size()), r.zeroRecord)
	resource := r.resource()
	switch resource.format {
	case "json":
		if err = json.Unmarshal(resource.line, r.record); err != nil {
			return err
		}
	case "yaml":
		if err = yaml.Unmarshal(resource.line, r.record); err != nil {
			return err
		}
	}

	ptr := xunsafe.AsPointer(r.record)
	if r.criteria != nil {
		if err := r.criteria.State.SetValue("r", r.record); err != nil {
			return err
		}
		if !r.criteria.Compute() {
			return r.Next(dest)
		}
	}
	for i, aField := range r.mapper.byPos {
		dest[i] = aField.Value(ptr)
	}
	if len(r.mapper.values) > 0 {
		for i, v := range r.mapper.values {
			dest[i] = v
		}
	}
	return nil
}

// hasNext returns true if there is next row to fetch.
func (r *Rows) hasNext() bool {

	return false
}

// ColumnTypeScanType returns column scan type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	aField, ok := r.mapper.byPos[index]
	if !ok {
		return nil
	}
	return aField.Type
}

// ColumnTypeDatabaseTypeName returns column database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	rType := r.ColumnTypeScanType(index)
	switch rType.Kind() {
	case reflect.Int:
		return "INT"
	case reflect.Float64:
		return "DECIMAL"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "STRING"
	case reflect.Slice:
		switch rType.Elem().Kind() {
		case reflect.Int:
			return "INTS"
		case reflect.Float64:
			return "DECIMALS"
		case reflect.String:
			return "STRINGS"
		case reflect.Uint8:
			return "BYTES"
		}
	}
	return ""
}

// ColumnTypeNullable returns if column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	rType := r.ColumnTypeScanType(index)
	return rType.Kind() == reflect.Pointer, true
}

func (r *Rows) initCriteria(criteria *expr.Qualify, args []driver.NamedValue) error {
	var values = &node.Values{Bindings: &node.Binding{}}
	for _, v := range args {
		values.Values = append(values.Values, v.Value)
	}
	scope := igo.NewScope()
	r.scope = scope
	goExpr, err := parser.AsBinaryGoExpr("r.", criteria, r.mapper.lookup, values)
	if err != nil {
		return fmt.Errorf("failed to compile criteria: %w", err)
	}
	for _, group := range values.Bindings.Groups {
		if group.InStrings != nil {
			var strs []string
			for _, v := range values.Values[group.From:group.To] {
				strs = append(strs, v.(string))
			}
			group.InStrings.Set(strs)
			scope.RegisterFunc("In"+group.Name, group.InStrings.In)
		}

		if group.InInts != nil {
			var ints []int
			for _, v := range values.Values[group.From:group.To] {
				ints = append(ints, v.(int))
			}
			group.InInts.Set(ints)
			scope.RegisterFunc("In"+group.Name, group.InInts.In)
		}
	}
	if r.recordSelector, err = scope.DefineVariable("r", r.recordType); err != nil {
		return err
	}
	if r.criteria, err = scope.BoolExpression(goExpr); err != nil {
		return err
	}
	return nil
}
