package structql

import (
	"github.com/viant/xunsafe"
)

//Walker represents struct walker
type Walker struct {
	root *Node
}

//Count counts leaf node
func (w *Walker) Count(value interface{}) int {
	return w.count(w.root, value)
}

//Traverse walks the node
func (w *Walker) Traverse(aNode *Node, value interface{}, visitor interface{}) error {
	nodeVisitor, _ := visitor.(NodeVisitor)
	leafVisitor, _ := visitor.(Visitor)
	return w.traverse(aNode, value, leafVisitor, nodeVisitor)
}

func (w *Walker) traverse(aNode *Node, value interface{}, visitor Visitor, nodeVisitor NodeVisitor) error {
	if !aNode.When(value) {
		return nil
	}
	if nodeVisitor != nil && aNode.kind == nodeKindObject {
		if err := nodeVisitor(aNode, value); err != nil {
			return err
		}
	}
	if aNode.IsLeaf {
		if visitor == nil {
			return nil
		}
		return visitor(visitor)
	}

	ptr := xunsafe.AsPointer(value)
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.traverse(aNode.child, item, visitor, nodeVisitor)
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			if err := w.traverse(aNode.child, item, visitor, nodeVisitor); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Walker) count(aNode *Node, value interface{}) int {
	if !aNode.When(value) {
		return 0
	}
	ptr := xunsafe.AsPointer(value)
	var result = 0
	var item interface{}
	if aNode.IsLeaf {
		return 1
	}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.count(aNode.child, item)
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			result += w.count(aNode.child, item)
		}
	}
	return result
}

func (w *Walker) mapNode(mapper *Mapper, aNode *Node, value interface{}, appender *xunsafe.Appender) error {
	if !aNode.When(value) {
		return nil
	}

	srcPtr := xunsafe.AsPointer(value)
	if aNode.IsLeaf {
		destItem := appender.Add()
		destItemPtr := xunsafe.AsPointer(destItem)
		if err := mapper.MapStruct(srcPtr, destItemPtr); err != nil {
			return err
		}
		return nil
	}
	var srcItem interface{}
	switch aNode.kind {
	case nodeKindObject:
		srcItem = aNode.xField.Interface(srcPtr)
		return w.mapNode(mapper, aNode.child, srcItem, appender)
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(srcPtr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(srcPtr, i)
			if err := w.mapNode(mapper, aNode.child, item, appender); err != nil {
				return err
			}
		}
	}
	return nil
}

//NewWalker creates a struct walker
func NewWalker(root *Node) *Walker {
	return &Walker{root: root}
}
