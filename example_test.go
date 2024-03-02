package structql_test

import (
	"fmt"
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
	fmt.Printf("%v\n", result)
}
