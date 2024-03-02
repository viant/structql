package parser

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	node2 "github.com/viant/structql/node"
	"github.com/viant/structql/parser/in"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

// AsBinaryGoExpr converts SQL criteria to golang expr
func AsBinaryGoExpr(holder string, node node.Node, lookup func(name string) *xunsafe.Field, values *node2.Values) (string, error) {
	output := strings.Builder{}
	if err := qualifyExpr(holder, node, lookup, &output, values.Bindings, ""); err != nil {
		return "", err
	}
	expr := strings.TrimSpace(output.String())
	return values.Bindings.Expand(expr, values.Values)
}

func qualifyExpr(holder string, node node.Node, lookup func(name string) *xunsafe.Field, output *strings.Builder, binding *node2.Binding, op string) error {
	switch actual := node.(type) {
	case *expr.Qualify:
		if err := qualifyExpr(holder, actual.X, lookup, output, binding, ""); err != nil {
			return err
		}
	case *expr.Ident:

		name := actual.Name
		binding.ContextField = lookup(actual.Name)
		if binding.ContextField != nil {
			name = binding.ContextField.Name
		}
		if binding.ContextField == nil {
			return fmt.Errorf("unknown field: %v", actual.Name)
		}
		fType := binding.ContextField.Type

		if strings.ToLower(op) == "in" {
			return nil
		}
		if fType.Kind() == reflect.Ptr && op != "is" {
			output.WriteString(holder + name + " != nil AND *")
		}
		output.WriteString(holder + name)
	case *expr.Placeholder:
		if actual.Name == "?" {
			binding.AddPlaceholder()
			output.WriteByte('?')
		}

	case *expr.Literal:
		switch actual.Kind {
		case "string":
			output.WriteByte('"')
			value := strings.Trim(actual.Value, `"'`)
			output.WriteString(value)
			output.WriteByte('"')
		case "null":
			output.WriteString("nil")
		default:
			output.WriteString(actual.Value)
		}
	case *expr.Binary:
		output.WriteString(" ")
		if err := qualifyExpr(holder, actual.X, lookup, output, binding, actual.Op); err != nil {
			return err
		}
		output.WriteString(" ")
		switch strings.ToLower(actual.Op) {
		case "and":
			output.WriteString("&&")
		case "or":
			output.WriteString("||")
		case "=":
			output.WriteString("==")
		case "is":
			output.WriteString("==")
		case "in":
			y := actual.Y.(*expr.Parenthesis)
			group := binding.AddPlaceholders(strings.Count(y.Raw, "?"))
			switch binding.ContextField.Type.Kind() {
			case reflect.String:
				group.InStrings = in.NewStrings()
				output.WriteString("In" + group.Name)
			case reflect.Int:
				group.InInts = in.NewInts()
				output.WriteString("In" + group.Name)
			default:
				return fmt.Errorf("unsupported type: %v for in operator", binding.ContextField.Type.Kind())
			}
			fieldName := group.Name
			if holder != "" {
				fieldName = holder + fieldName
			}
			output.WriteString(fmt.Sprintf("(%s,?)", fieldName))
			return nil
		default:
			output.WriteString(actual.Op)
		}
		output.WriteString(" ")
		if err := qualifyExpr(holder, actual.Y, lookup, output, binding, ""); err != nil {
			return err
		}

	}
	return nil
}
