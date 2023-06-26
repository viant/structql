package structql

import (
	"fmt"
	"github.com/viant/sqlparser"
	sparser "github.com/viant/structql/parser"
	"reflect"
	"strings"
)

//IsStructQuery returns true if dest result in struct
func IsStructTypeQuery(query string, source reflect.Type) (bool, error) {
	var err error
	if unwrapStruct(source) == nil {
		return false, fmt.Errorf("invalid source type: %s", source.String())
	}
	ret := &Query{query: query, source: source}
	if ret.sel, err = sqlparser.ParseQuery(query); err != nil {
		return false, fmt.Errorf("failed to parse %w, %v", err, query)
	}
	from := strings.Trim(sqlparser.Stringify(ret.sel.From.X), "`")
	sel, err := sparser.ParseSelector(from)
	if err != nil {
		return false, fmt.Errorf("invalid from: %w, %v", err, from)
	}
	if ret.node, err = NewNode(source, sel); err != nil {
		return false, err
	}
	return unwrapStruct(ret.node.LeafType()) != nil, nil
}
