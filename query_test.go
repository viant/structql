package structql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/structql/transform"
	"github.com/viant/toolbox"
	"reflect"
	"testing"
)

func TestSelector_Select(t *testing.T) {

	type Item struct {
		ID   int
		Name string
	}

	type Record struct {
		ID       int
		Name     string
		Active   bool
		Comments string
		Items    []*Item
	}

	type Holder struct {
		ID      int
		Records []*Record
	}

	type Transformed1 struct {
		Name   string
		Active bool
	}

	var testCases = []struct {
		description string
		query       string
		source      interface{}
		dest        interface{}
		expect      interface{}
		IntsField   string
	}{

		{
			description: "query with criteria dynamic dest nested",
			query:       "SELECT Name,Active FROM `/Records[Active = true]`",
			source: []*Holder{
				{

					Records: []*Record{
						{
							ID:       1,
							Name:     "name 100",
							Active:   true,
							Comments: "comments 1",
						},
						{
							ID:       2,
							Name:     "name 200",
							Active:   false,
							Comments: "comments 2",
						},
					},
				},
				{
					Records: []*Record{
						{
							ID:       3,
							Name:     "name 300",
							Active:   true,
							Comments: "comments 3",
						},
					},
				},
			},
			expect: `[
	{
		"Name": "name 100",
		"Active": true
	},
	{
		"Name": "name 300",
		"Active": true
	}
]`,
		},
		{
			description: "query with dest ARRAY_AGG",
			query:       "SELECT ARRAY_AGG(ID) AS IDs FROM `/` WHERE Active = true",
			source: []*Record{
				{
					ID:       1,
					Name:     "name 1",
					Active:   true,
					Comments: "comments 1",
				},
				{
					ID:       2,
					Name:     "name 2",
					Active:   false,
					Comments: "comments 2",
				},
				{
					ID:       3,
					Name:     "name 3",
					Active:   true,
					Comments: "comments 3",
				},
			},
			expect: `[{"IDs":[1, 3]}]`,
		},
		{
			description: "query with dest ARRAY_AGG all",
			query:       "SELECT ARRAY_AGG(ID) AS XInts FROM `/` ",
			source: []*Record{
				{
					ID:       1,
					Name:     "name 1",
					Active:   true,
					Comments: "comments 1",
				},
				{
					ID:       2,
					Name:     "name 2",
					Active:   false,
					Comments: "comments 2",
				},
				{
					ID:       3,
					Name:     "name 3",
					Active:   true,
					Comments: "comments 3",
				},
			},
			IntsField: "XInts",
			expect:    `[1,2, 3]`,
		},
		{
			description: "query with dest",
			query:       "SELECT Name, Active FROM `/`",
			source: []*Record{
				{
					ID:       1,
					Name:     "name 1",
					Active:   true,
					Comments: "comments 1",
				},
				{
					ID:       2,
					Name:     "name 2",
					Active:   false,
					Comments: "comments 2",
				},
				{
					ID:       3,
					Name:     "name 3",
					Active:   true,
					Comments: "comments 3",
				},
			},
			dest: &Transformed1{},
			expect: `[
	{
		"Name": "name 1",
		"Active": true
	},
	{
		"Name": "name 2",
		"Active": false
	},
	{
		"Name": "name 3",
		"Active": true
	}
]`,
		},
		{
			description: "query with dynamic dest",
			query:       "SELECT Name, Active FROM `/`",
			source: []*Record{
				{
					ID:       1,
					Name:     "name 10",
					Active:   true,
					Comments: "comments 1",
				},
				{
					ID:       2,
					Name:     "name 20",
					Active:   false,
					Comments: "comments 2",
				},
				{
					ID:       3,
					Name:     "name 30",
					Active:   true,
					Comments: "comments 3",
				},
			},
			expect: `[
	{
		"Name": "name 10",
		"Active": true
	},
	{
		"Name": "name 20",
		"Active": false
	},
	{
		"Name": "name 30",
		"Active": true
	}
]`,
		},

		{
			description: "query with dynamic dest nested",
			query:       "SELECT Name,Active FROM `/Records`",
			source: []*Holder{
				{

					Records: []*Record{
						{
							ID:       1,
							Name:     "name 100",
							Active:   true,
							Comments: "comments 1",
						},
						{
							ID:       2,
							Name:     "name 200",
							Active:   false,
							Comments: "comments 2",
						},
					},
				},
				{
					Records: []*Record{
						{
							ID:       3,
							Name:     "name 300",
							Active:   true,
							Comments: "comments 3",
						},
					},
				},
			},
			expect: `[
	{
		"Name": "name 100",
		"Active": true
	},
	{
		"Name": "name 200",
		"Active": false
	},
	{
		"Name": "name 300",
		"Active": true
	}
]`,
		},

		{
			description: "query with WHERE criteria dynamic dest nested",
			query:       "SELECT Name,Active FROM `/Records` WHERE Active = true",
			source: []*Holder{
				{

					Records: []*Record{
						{
							ID:       1,
							Name:     "name 100",
							Active:   true,
							Comments: "comments 1",
						},
						{
							ID:       2,
							Name:     "name 200",
							Active:   false,
							Comments: "comments 2",
						},
					},
				},
				{
					Records: []*Record{
						{
							ID:       3,
							Name:     "name 300",
							Active:   true,
							Comments: "comments 3",
						},
					},
				},
			},
			expect: `[
	{
		"Name": "name 100",
		"Active": true
	},
	{
		"Name": "name 300",
		"Active": true
	}
]`,
		},
		{
			description: "query *",
			query:       "SELECT * FROM `/Records`",
			source: []*Holder{
				{

					Records: []*Record{
						{
							ID:       1,
							Name:     "name 100",
							Active:   true,
							Comments: "comments 1",
						},
						{
							ID:       2,
							Name:     "name 200",
							Active:   false,
							Comments: "comments 2",
						},
					},
				},
				{
					Records: []*Record{
						{
							ID:       3,
							Name:     "name 300",
							Active:   true,
							Comments: "comments 3",
						},
					},
				},
			},
			expect: []*Record{
				{
					ID:       1,
					Name:     "name 100",
					Active:   true,
					Comments: "comments 1",
				},
				{
					ID:       2,
					Name:     "name 200",
					Active:   false,
					Comments: "comments 2",
				},
				{
					ID:       3,
					Name:     "name 300",
					Active:   true,
					Comments: "comments 3",
				},
			},
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		sel, err := NewQuery(testCase.query, reflect.TypeOf(testCase.source), reflect.TypeOf(testCase.dest))
		if !assert.Nil(t, err, testCase.description) {
			fmt.Printf("err: %v\n", err)
			continue
		}
		dest, err := sel.Select(testCase.source)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if testCase.IntsField != "" {
			asInts, err := transform.AsInts(sel.Type(), testCase.IntsField)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}
			dest = asInts(dest)
		}

		if !assertly.AssertValues(t, testCase.expect, dest, testCase.description) {
			toolbox.DumpIndent(dest, true)
		}
	}
}
