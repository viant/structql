package sql

import "github.com/viant/x"

var globalTypes = x.NewRegistry()

func Register(aType *x.Type) {
	globalTypes.Register(aType)
}
