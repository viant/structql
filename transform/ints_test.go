package transform

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestAsInts(t *testing.T) {

	type Record struct {
		ID   int
		Name string
		Ids  []int
	}

	var testCases = []struct {
		description string
		source      interface{}
		fieldName   string
		expect      []int
	}{
		{
			description: "[]*T  []int extract",
			fieldName:   "Ids",
			source: []*Record{
				{
					Ids: []int{1, 5, 7},
				},
			},
			expect: []int{1, 5, 7},
		},
		{
			description: "[]T []int extract",
			fieldName:   "Ids",
			source: []Record{
				{
					Ids: []int{1, 7, 7},
				},
			},
			expect: []int{1, 7, 7},
		},
		{
			description: "T []int extract",
			fieldName:   "Ids",
			source: Record{
				Ids: []int{8, 5, 7},
			},
			expect: []int{8, 5, 7},
		},
	}

	for _, testCase := range testCases {
		target := reflect.TypeOf(testCase.source)
		fn, err := AsInts(target, testCase.fieldName)
		if !assert.Nilf(t, err, testCase.description) {
			continue
		}
		actual := fn(testCase.source)
		assert.EqualValuesf(t, testCase.expect, actual, testCase.description)
	}

}
