package structql

import (
	"github.com/viant/xunsafe"
)

type Context struct {
	value    interface{}
	group    map[string]interface{}
	mapper   *Mapper
	appender *xunsafe.Appender
}

func (c *Context) Next(source interface{}) interface{} {
	if !c.mapper.aggregate {
		return c.appender.Add()
	}
	if len(c.group) == 0 {
		if c.value == nil {
			c.value = c.appender.Add()
		}
		return c.value
	}
	panic("group by not supproted yet")
}

func NewContext(mapper *Mapper, appender *xunsafe.Appender, aggregate bool) *Context {
	if !aggregate {
		return &Context{mapper: mapper, appender: appender}
	}
	return &Context{mapper: mapper, appender: appender, group: map[string]interface{}{}}
}
