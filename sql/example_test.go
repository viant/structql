package sql_test

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
