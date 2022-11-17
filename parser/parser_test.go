package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/structql/node"
	"testing"
)

func TestParseSelector(t *testing.T) {
	var testCases = []struct {
		description string
		expr        string
		expect      interface{}
	}{
		{
			description: "basic selector",
			expr:        "/Records",
			expect:      &node.Selector{Name: "Records", Child: &node.Selector{}},
		},
		{
			description: "basic selector relative",
			expr:        "Records",
			expect:      &node.Selector{Name: "Records", Child: &node.Selector{}},
		},
		{
			description: "basic selector relative",
			expr:        "Root/Records",
			expect:      &node.Selector{Name: "Root", Child: &node.Selector{Name: "Records", Child: &node.Selector{}}},
		},
		{
			description: "node with condition",
			expr:        "Items[Active=true]/Nodes",
			expect:      &node.Selector{Name: "Items", Child: &node.Selector{Name: "Nodes", Child: &node.Selector{}}},
		},
	}

	for _, testCase := range testCases {
		actual, err := ParseSelector(testCase.expr)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assertly.AssertValues(t, testCase.expect, actual, testCase.description)
	}
}
