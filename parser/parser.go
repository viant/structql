package parser

import (
	"fmt"
	"github.com/viant/parsly"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/structql/node"
	"github.com/yuin/gopher-lua/parse"
	"strings"
)

//ParseSelector parses selector
func ParseSelector(expr string) (*node.Selector, error) {
	root := &node.Selector{}
	expr = strings.Trim(expr, "`")
	cursor := parsly.NewCursor("", []byte(expr), 0)
	err := parseSelector(cursor, root)
	if err != nil {
		return root, fmt.Errorf("%s", expr)
	}

	return root, err
}

func parseSelector(cursor *parsly.Cursor, parent *node.Selector) error {
	selector := parent
outer:
	for cursor.Pos < len(cursor.Input) {
		match := cursor.MatchAfterOptional(whitespaceMatcher, identifierMatcher, selectorSeparatorMatcher)
		switch match.Code {
		case identifier:
			selector.Name = match.Text(cursor)
			pos := cursor.Pos
			selector.Child = &node.Selector{}
			if match = cursor.MatchOne(conditionalBlockMatcher); match.Code == conditionalBlock {
				block := match.Text(cursor)
				qualify, err := ParseQualify(selector.Name, []byte(block[1:len(block)-1]), pos+1)
				if err != nil {
					return err
				}
				selector.Child.Criteria = qualify.X
				selector.Child.Holder = selector.Name
			}

		case selectorSeparator:
			if selector.Name != "" {
				child := &node.Selector{}
				selector.Child = child
				selector = child
			}
		case parse.EOF:
			break outer
		default:
			return cursor.NewError(identifierMatcher, selectorSeparatorMatcher)
		}
	}
	return nil
}

//ParseQualify parses SQL crtieria
func ParseQualify(path string, cond []byte, offset int) (*expr.Qualify, error) {
	cursor := parsly.NewCursor(path, cond, offset)
	qualify := &expr.Qualify{}
	return qualify, sqlparser.ParseQualify(cursor, qualify)
}
