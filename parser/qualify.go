package parser

import (
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"strings"
)

//AsBinaryGoExpr converts SQL criteria to golang expr
func AsBinaryGoExpr(holder string, node node.Node) string {
	output := strings.Builder{}
	qualifyExpr(holder, node, &output)
	return strings.TrimSpace(output.String())
}

func qualifyExpr(holder string, node node.Node, output *strings.Builder) {
	switch actual := node.(type) {
	case *expr.Qualify:
		qualifyExpr(holder, actual.X, output)
	case *expr.Ident:
		output.WriteString(holder + actual.Name)
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
		qualifyExpr(holder, actual.X, output)
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
		default:
			output.WriteString(actual.Op)
		}
		output.WriteString(" ")
		qualifyExpr(holder, actual.Y, output)

	}
}
