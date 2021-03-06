package main

import (
	"database/sql"

	"github.com/c4pt0r/log"
	"github.com/c4pt0r/tielect"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid"
)

type MyCandidate struct {
	id string
}

// Make sure the ID is unique
func (c *MyCandidate) ID() string {
	return c.id
}

func (c *MyCandidate) Val() string {
	return "any value"
}

func UUID() string {
	u, _ := uuid.NewV4()
	return u.String()
}

func main() {
	// Use TiDB connection here
	db, err := sql.Open("mysql", "root:@tcp(localhost:4000)/test")
	if err != nil {
		log.Fatal(err)
	}

	e := tielect.NewElection(db, "default", &MyCandidate{
		id: UUID(),
	})
	e.Init()

	// Start election, it will block until election is finished
	if ok, err := e.Campaign(); err != nil {
		panic(err)
	} else if !ok {
		log.I("I am not the leader")
	} else {
		log.I("I'm the leader!")
	}
}
