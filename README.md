# structql (SQL for Golang data structures)

[![GoReportCard](https://goreportcard.com/badge/github.com/viant/structql)](https://goreportcard.com/report/github.com/viant/structql)
[![GoDoc](https://godoc.org/github.com/viant/structql?status.svg)](https://godoc.org/github.com/viant/structql)

This library is compatible with Go 1.17+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Usage](#usage)
- [Contribution](#contributing-to-structql)
- [License](#license)

## Motivation

The goal of this library is to be able dynamically transform golang data structure with SQL.
Initial version implements only basic SQL functionality. 
As this library grow complex SQL transformation will be added, including indexes.


## Introduction

- Basic Query

```go

SQL := "SELECT ID, Name FROM `/` WHERE Status = 2"
query, err := structql.NewSelector(SQL, reflect.TypeOf(&Vendor{}), nil)
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
query, err := structql.NewSelector(SQL, reflect.TypeOf(&Vendor{}), nil)
if err != nil {
    log.Fatal(err)
}	
result, err := query.Select(vendors)
if err != nil {
    log.Fatal(err)
}
```



## Usage

```go

package myPkg

import (
	"github.com/viant/structql"
	"github.com/viant/toolbox"
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
	query, err := structql.NewSelector(SQL, reflect.TypeOf(vendors), reflect.TypeOf(Query1Output{}))
	if err != nil {
		log.Fatal(err)
	}
	result, err := query.Select(vendors)
	if err != nil {
		log.Fatal(err)
	}
	toolbox.Dump(result)
}

```


## Contributing to structql

structql is an open source project and contributors are welcome!

See [TODO](TODO.md) list

## Credits and Acknowledgements

**Library Author:** Adrian Witas

