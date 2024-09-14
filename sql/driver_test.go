package sql

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ExecContext(t *testing.T) {

	type Foo struct {
		Id   int
		Name string
	}
	var testCase = []struct {
		description string
		dsn         string
		sql         string
		params      []interface{}
		expect      interface{}
	}{
		{
			description: "register inlined type",
			dsn:         "file:///testdata/",
			sql:         "REGISTER TYPE Bar AS struct{id int; name string}",
		},
		{
			description: "register named type",
			dsn:         "file:///testdata/",
			sql:         "REGISTER TYPE Foo AS ?",
			params:      []interface{}{Foo{}},
		},
		{
			description: "register inlined type",
			dsn:         "file:///testdata/",
			sql:         "REGISTER GLOBAL TYPE Bar AS struct{id int; name string}",
		},
		{
			description: "register named type",
			dsn:         "file:///testdata/",
			sql:         "REGISTER GLOBAL TYPE Foo AS ?",
			params:      []interface{}{Foo{}},
		},
	}

	for _, tc := range testCase {
		t.Run(tc.description, func(t *testing.T) {
			db, err := sql.Open("structql", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			assert.NotNil(t, db, tc.description)
			_, err = db.ExecContext(context.Background(), tc.sql, tc.params...)
			assert.Nil(t, err, tc.description)
		})
	}
}

func Test_QueryContext(t *testing.T) {

	type Single struct {
		Id    int
		Name  string
		Value int
	}
	type Foo struct {
		Id   int
		Name string
	}
	var testCase = []struct {
		description string
		dsn         string
		execSQL     string
		execParams  []interface{}
		querySQL    string
		queryParams []interface{}
		expect      interface{}
		scanner     func(r *sql.Rows) (interface{}, error)
	}{

		{
			description: "single row pseudo",
			dsn:         "mem://localhost/structql/",
			querySQL:    "SELECT ID, NAME, VALUE, TS  FROM (SELECT 1 AS ID, CAST(? AS CHAR) AS NAME, CAST(? AS int) AS VALUE, NOW() TS  FROM single LIMIT 1)",
			queryParams: []interface{}{"name1", 10},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Single{}
				ts := time.Time{}
				err := r.Scan(&foo.Id, &foo.Name, &foo.Value, &ts)
				return &foo, err
			},
			expect: []interface{}{
				&Single{Id: 1, Name: "name1", Value: 10},
			},
		},

		{
			description: "select all rows with register named type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER TYPE Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "name1"},
				&Foo{Id: 2, Name: "name2"},
			},
		},

		{
			description: "select all rows with register named type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER TYPE Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "name1"},
				&Foo{Id: 2, Name: "name2"},
			},
		},
		{
			description: "select all rows with register global named type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER GLOBAL TYPE Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "name1"},
				&Foo{Id: 2, Name: "name2"},
			},
		},
		{
			description: "select 1 row by id with register named type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER TYPE Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo WHERE id=2",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 2, Name: "name2"},
			},
		},
		{
			description: "select 2 rows by id with in operator and register named type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER TYPE Foo AS ?",
			execParams:  []interface{}{Foo{}},
			querySQL:    "SELECT * FROM Foo WHERE id IN(?, ?, ?)",
			queryParams: []interface{}{1, 2, 3},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 1, Name: "name1"},
				&Foo{Id: 2, Name: "name2"},
			},
		},
		{
			description: "select 1 row by id with register inlined type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER TYPE Foo AS struct{Id int; Name string}",
			execParams:  []interface{}{},
			querySQL:    "SELECT * FROM Foo WHERE id=2",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 2, Name: "name2"},
			},
		},
		{
			description: "select 1 row by id with global register inlined type",
			dsn:         "file:///testdata/",
			execSQL:     "REGISTER GLOBAL TYPE Foo AS struct{Id int; Name string}",
			execParams:  []interface{}{},
			querySQL:    "SELECT * FROM Foo WHERE id=2",
			queryParams: []interface{}{},
			scanner: func(r *sql.Rows) (interface{}, error) {
				foo := Foo{}
				err := r.Scan(&foo.Id, &foo.Name)
				return &foo, err
			},
			expect: []interface{}{
				&Foo{Id: 2, Name: "name2"},
			},
		},
	}

	//testCase = testCase[:1]
	for _, tc := range testCase {
		t.Run(tc.description, func(t *testing.T) {
			db, err := sql.Open("structql", tc.dsn)
			if !assert.Nil(t, err, tc.description) {
				return
			}
			assert.NotNil(t, db, tc.description)
			if tc.execSQL != "" {
				_, err = db.ExecContext(context.Background(), tc.execSQL, tc.execParams...)
				assert.Nil(t, err, tc.description)
			}

			rows, err := db.QueryContext(context.Background(), tc.querySQL, tc.queryParams...)
			assert.Nil(t, err, tc.description)
			assert.NotNil(t, rows, tc.description)
			var items []interface{}
			for rows.Next() {
				item, err := tc.scanner(rows)

				assert.Nil(t, err, tc.description)
				items = append(items, item)
			}
			assert.Equal(t, tc.expect, items, tc.description)
		})
	}
}
