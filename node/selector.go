package node

import "github.com/viant/sqlparser/node"

//Selector represents a selector
type Selector struct {
	Name     string
	Criteria node.Node
	Holder   string
	Child    *Selector
}
