# structql (SQL for GoLang data structures)

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/structql)](https://goreportcard.com/report/github.com/viant/structql)
[![GoDoc](https://godoc.org/github.com/viant/structql?status.svg)](https://godoc.org/github.com/viant/structql)

This library is compatible with Go 1.17+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
- [Contribution](#contributing-to-structql)
- [License](#license)

## Motivation

The primary aim of this library is to facilitate access to structured data through SQL queries. 
It is specifically tailored to transform Golang data structures using SQL syntax. 
The initial release of this library focuses on implementing the foundational features of SQL,
laying the groundwork for more advanced functionalities in future updates


## Introduction

#### Go struct transformation with SQL

- Basic Query

```go

SQL := "SELECT ID, Name FROM `/` WHERE Status = 2"
query, err := structql.NewQuery(SQL, reflect.TypeOf(&Vendor{}), nil)
if err != nil {
    log.Fatal(err)
}	
result, err := query.Select(vendors)
if err != nil {
    log.Fatal(err)
}
```


- Nested Query

```go
SQL := "SELECT ProductID,Revenue FROM `/Products[Active=1]/Performance` WHERE Revenue > 100.0 "
query, err := structql.NewQuery(SQL, reflect.TypeOf(&Vendor{}), nil)
if err != nil {
    log.Fatal(err)
}	
result, err := query.Select(vendors)
if err != nil {
    log.Fatal(err)
}
```

#### Querying data with database/sql


## DSN Data Source Name
The structql driver accepts the following DSN

* 'structql://[localhost|cloudProvider$bucket]/[baseURI|folderPath][{options}]'

  Where queryString can optionally configure the following option:
    - key:  access key id
    - secret: access key secret



## Usage

### database/sql

```go
package mypkg

import (
  "context"
  "database/sql"
  _ "github.com/viant/structql/sql"
  "log"
)

func Example() {
  db, err := sql.Open("structql", "structql:///opt/local/testdata/")
  if err != nil {
    log.Fatal(err)
  }
  type Foo struct {
    ID   int
    Name string
  }
  _, err = db.Exec("REGISTER TYPE Foo AS ?", &Foo{})
  if err != nil {
    log.Fatal(err)
  }

  rows, err := db.QueryContext(context.Background(), "SELECT id,name FROM Foo WHERE id IN(?, ?)", 1, 3)
  if err != nil {
    log.Fatal(err)
  }
  var foos []*Foo
  for rows.Next() {
    var foo Foo
    err = rows.Scan(&foo.ID, &foo.Name)
    if err != nil {
      log.Fatal(err)
    }
    foos = append(foos, &foo)
  }
}



```

```go

### Go struct transformation with SQL

```go

package myPkg

import (
	"github.com/viant/structql"
	"log"
	"reflect"
	"time"
)

type (
	Vendor struct {
		ID       int
		Name     string
		Revenue  float64
		Status int
		Products []*Product
	}

	Product struct {
		ID          int
		Name        string
		Status      int
		Performance []*Performance
	}

	Performance struct {
		ProductID int
		Date      time.Time
		Quantity  float64
		Revenue   float64
	}
)


func ExampleQuery_Select() {
	var vendors = []*Vendor{
		{
			ID:   1,
			Name: "Vendor 1",
			Products: []*Product{
				{
					ID:     1,
					Status: 1,
					Name:   "Product 1",
					Performance: []*Performance{
						{
							ProductID: 1,
							Revenue:   13050,
							Quantity:  124,
						},
					},
				},
			},
		},
		{
			ID:   2,
			Name: "Vendor 2",
			Products: []*Product{
				{
					ID:     2,
					Name:   "Product 2",
					Status: 1,
					Performance: []*Performance{
						{
							ProductID: 2,
							Revenue:   16050,
							Quantity:  110,
						},
					},
				},
				{
					ID:     7,
					Name:   "Product 7",
					Status: 0,
					Performance: []*Performance{
						{
							ProductID: 7,
							Revenue:   160,
							Quantity:  10,
						},
					},
				},
			},
		},
		{
			ID:   3,
			Name: "Vendor 3",
			Products: []*Product{
				{
					ID:     3,
					Name:   "Product 3",
					Status: 1,
					Performance: []*Performance{
						{
							ProductID: 3,
							Revenue:   11750,
							Quantity:  143,
						},
					},
				},
				{
					ID:     4,
					Name:   "Product 4",
					Status: 1,
					Performance: []*Performance{
						{
							ProductID: 4,
							Revenue:   11,
							Quantity:  1,
						},
					},
				},
			},
		},
	}
	SQL := "SELECT ProductID,Revenue FROM `/Products[Active=1]/Performance` WHERE Revenue > 100.0 "
	type Query1Output struct {
		ProductID int
		Revenue   float64
	}
	query, err := structql.NewQuery(SQL, reflect.TypeOf(vendors), reflect.TypeOf(Query1Output{}))
	if err != nil {
		log.Fatal(err)
	}
	result, err := query.Select(vendors)
	if err != nil {
		log.Fatal(err)
	}
	
}

```


## Contributing to structql

structql is an open source project and contributors are welcome!

See [TODO](TODO.md) list

## Credits and Acknowledgements

**Library Author:** Adrian Witas

