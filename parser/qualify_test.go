package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQualifyExpr(t *testing.T) {
	var testCases = []struct {
		description string
		expr        string
		expect      string
	}{
		{
			description: "binary expr",
			expr:        "Field1 = 'abc' AND Field2 IS NULL",
			expect:      `Field1 ==  "abc" &&  Field2 == nil`,
		},
	}

	for _, testCase := range testCases {
		qualify, err := ParseQualify("", []byte(testCase.expr), 0)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		expr := AsBinaryGoExpr("", qualify)
		assert.EqualValues(t, testCase.expect, expr, testCase.description)
	}
}
