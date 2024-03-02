package parser

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/structql/node"
	"github.com/viant/xunsafe"
	"reflect"
	"testing"
)

func TestQualifyExpr(t *testing.T) {
	var testCases = []struct {
		description string
		fieldType   reflect.Type
		values      []interface{}
		expr        string
		expect      string
	}{
		{
			description: "binary expr",
			expr:        "Field1 = 'abc' AND Field2 IS NULL",
			expect:      `Field1 ==  "abc" &&  Field2 == nil`,
		},
		{
			description: "binary expr",
			values:      []interface{}{"abc", "xyz"},
			expr:        "Field1 =  ? AND Field2 =  3 AND Field3 =  ? ",
			expect:      `Field1 ==  "abc" &&  Field2 ==  3 &&  Field3 == "xyz"`,
		},
		{
			description: "binary ptr expr",
			fieldType:   reflect.PtrTo(reflect.TypeOf("")),
			values:      []interface{}{"abc"},
			expr:        "Field1 = ?",
			expect:      `Field1 != nil AND *Field1 == "abc"`,
		},
		{
			description: "binary ptr is nil",
			fieldType:   reflect.PtrTo(reflect.TypeOf("")),
			values:      []interface{}{"abc"},
			expr:        "Field1 is null",
			expect:      `Field1 == nil`,
		},
		{
			description: "in",
			fieldType:   reflect.TypeOf(0),
			values:      []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expr:        "Field1 in(?,?,?,?,?,?,?,?,?,?)",
			expect:      `InField1(Field1,1)`,
		},
	}

	for _, testCase := range testCases {
		qualify, err := ParseQualify("", []byte(testCase.expr), 0)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		fType := testCase.fieldType
		if fType == nil {
			fType = reflect.TypeOf("")
		}

		binding := &node.Binding{}
		expr, _ := AsBinaryGoExpr("", qualify, func(name string) *xunsafe.Field {
			return &xunsafe.Field{Name: name, Type: fType}
		}, &node.Values{Bindings: binding, Values: testCase.values})
		assert.EqualValues(t, testCase.expect, expr, testCase.description)
	}
}
