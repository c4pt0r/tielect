package tielect

import (
	"database/sql"
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrNotLeader = errors.New("not leader")
)

const (
	// DefaultLease is the default lease time for a leader.
	DefaultLease = 5 * time.Second
)

type Election struct {
	dsn    string
	c      Candidate
	leader Candidate
	db     *sql.DB
}

type Candidate interface {
	ID() string
	Profile() string
}

func NewElection(dsn string) *Election {
	return &Election{
		dsn: dsn,
	}
}

func (e *Election) Init() error {
	var err error
	e.db, err = sql.Open("mysql", e.dsn)
	if err != nil {
		return err
	}

	e.db.SetConnMaxLifetime(time.Minute * 3)
	e.db.SetMaxOpenConns(50)
	e.db.SetMaxIdleConns(50)

	stmt := `
		CREATE TABLE IF NOT EXISTS tielect (
			name VARCHAR(255) NOT NULL,
			leader_id VARCHAR(255) NOT NULL,
			value TEXT NOT NULL,
			last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			lease TIME,
			PRIMARY KEY (name),
			KEY (name, leader_id)
		)
	`
	_, err = e.db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil

}

func (e *Election) Campaign(val string) error {
	return nil
}

func (e *Election) Proclaim(val string) error {
	return nil
}

func (e *Election) Resign() error {
	return nil
}
