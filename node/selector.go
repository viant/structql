package node

import "github.com/viant/sqlx/metadata/ast/node"

//Selector represents a selector
type Selector struct {
	Name     string
	Criteria node.Node
	Holder   string
	Child    *Selector
}
