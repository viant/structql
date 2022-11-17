package structql

import (
	"fmt"
	"github.com/viant/igo"
	"github.com/viant/igo/exec"
	"github.com/viant/igo/exec/expr"
	snode "github.com/viant/sqlx/metadata/ast/node"
	"github.com/viant/structql/node"
	"github.com/viant/structql/parser"
	"github.com/viant/xunsafe"
	"reflect"
)

type nodeKind int

const (
	nodeKindUnknown = nodeKind(0)
	nodeKindObject  = nodeKind(2)
	nodeKindArray   = nodeKind(3)
)

//Node represents a node
type Node struct {
	kind      nodeKind
	IsLeaf    bool
	ownerType reflect.Type
	xField    *xunsafe.Field
	xSlice    *xunsafe.Slice
	selector  *node.Selector
	child     *Node
	expr      *expr.Bool
	exprSel   *exec.Selector
}

//Type returns node Type
func (n *Node) Type() reflect.Type {
	return n.ownerType
}

//Leaf returns leaf node
func (n *Node) Leaf() *Node {
	if n.child != nil {
		return n.child.Leaf()
	}
	return n
}

//LeafType returns leaf type
func (n *Node) LeafType() reflect.Type {
	if n.child != nil {
		return n.child.LeafType()
	}
	return n.ownerType
}

//When applied expr or returns true if not defined
func (n *Node) When(value interface{}) bool {
	if n.expr == nil {
		return true
	}
	state := n.expr.NewState()
	n.exprSel.SetValue(state.Pointer(), value)
	result := n.expr.ComputeWithState(state)
	state.Release()
	return result
}

//LeafOwnerType returns leaf type
func (n *Node) LeafOwnerType() reflect.Type {
	if n.child != nil {
		if n.child.IsLeaf {
			return n.child.ownerType
		}
		return n.child.LeafType()
	}
	return n.ownerType
}

//NewNode creates a node
func NewNode(ownerType reflect.Type, sel *node.Selector) (*Node, error) {
	var err error
	aNode := &Node{selector: sel}
	aNode.ownerType = ownerType

	rawType := aNode.ownerType
	if aNode.ownerType.Kind() == reflect.Ptr {
		rawType = aNode.ownerType.Elem()
	}
	aNode.IsLeaf = sel.Child == nil
	switch rawType.Kind() {
	case reflect.Slice:
		aNode.kind = nodeKindArray
		aNode.IsLeaf = false
		aNode.xSlice = xunsafe.NewSlice(rawType)
		if aNode.child, err = NewNode(aNode.ownerType.Elem(), sel); err != nil {
			return nil, err
		}
	case reflect.Struct:
		aNode.kind = nodeKindObject
		if sel.Name != "" {
			if aNode.xField = xunsafe.FieldByName(rawType, sel.Name); aNode.xField == nil {
				return nil, fmt.Errorf("failed to lookup field: '%v' on %v", sel.Name, rawType.Name())
			}
			if aNode.child, err = NewNode(aNode.xField.Type, sel.Child); err != nil {
				return nil, err
			}
		}
		if sel.Criteria != nil {
			if aNode.expr, aNode.exprSel, err = compileCriteria(sel.Holder, sel.Criteria, aNode.ownerType); err != nil {
				return nil, err
			}
		}

	default:
		if aNode.IsLeaf {
			return aNode, nil
		}
		return nil, fmt.Errorf("unsupported type:%s", ownerType.String())
	}
	return aNode, err
}

func compileCriteria(holder string, criteria snode.Node, ownerType reflect.Type) (*expr.Bool, *exec.Selector, error) {
	var err error
	goExpr := parser.AsBinaryGoExpr(holder+".", criteria)
	scope := igo.NewScope()
	exprSel, err := scope.DefineVariable(holder, ownerType)
	if err != nil {
		return nil, nil, err
	}
	expr, err := scope.BoolExpression(goExpr)
	return expr, exprSel, err
}
