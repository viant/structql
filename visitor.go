package structql

//Visitor represents a visitor
type Visitor func(value interface{}) error

//NodeVisitor represents a node visitor (struct or leaf node)
type NodeVisitor func(node *Node, value interface{}) error
