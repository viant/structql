package parser

import (
	"github.com/viant/parsly"
	pmatcher "github.com/viant/parsly/matcher"
)

const (
	whitespaceCode int = iota
	selectorSeparator
	identifier
	conditionalBlock
)

var whitespaceMatcher = parsly.NewToken(whitespaceCode, "whitespace", pmatcher.NewWhiteSpace())
var selectorSeparatorMatcher = parsly.NewToken(selectorSeparator, "/", pmatcher.NewByte('/'))
var identifierMatcher = parsly.NewToken(identifier, "Ident", NewIdentity())
var conditionalBlockMatcher = parsly.NewToken(conditionalBlock, "[]", pmatcher.NewBlock('[', ']', '\\'))
