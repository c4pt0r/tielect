package tielect

import (
	"database/sql"
	"errors"
	"time"

	"github.com/c4pt0r/log"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/atomic"
)

var (
	ErrNotLeader = errors.New("not leader")
)

const (
	// DefaultLease is the default lease time for a leader.
	DefaultLeaseInSec = 5
)

type Election struct {
	db *sql.DB

	c    Candidate
	name string

	currentTerm     int64
	currentRevision int64

	sinceLastUpdate time.Time

	resigned *atomic.Bool
}

type Candidate interface {
	ID() string
	Val() string
}

func NewElection(db *sql.DB, name string, candidate Candidate) *Election {
	return &Election{
		db:       db,
		c:        candidate,
		resigned: atomic.NewBool(false),
	}
}

func (e *Election) Init() error {
	stmt := `
		CREATE TABLE IF NOT EXISTS tielect (
			name VARCHAR(255) NOT NULL,
			leader_id VARCHAR(255) NOT NULL,
			value TEXT NOT NULL,
			term BIGINT NOT NULL,
			revision BIGINT NOT NULL,
			PRIMARY KEY (name),
			KEY (name, leader_id),
			KEY (name, term),
			KEY (name, term, revision)
		)
	`
	_, err := e.db.Exec(stmt)
	if err != nil {
		return err
	}
	return nil
}

// Campaign will block until the election is won.
func (e *Election) Campaign() (bool, error) {
	for {
		// if old leader is timeout or resigned
		// try to be new leader
		if e.isLeaderTimeout() || e.getCurrentLeaderID() == "" {
			succ, err := e.tryToBeLeader()
			if err != nil {
				return false, err
			}
			if succ {
				break
			}
		}
		time.Sleep(time.Second)
	}

	go func() {
		log.Info("become a leader, try to update lease")
		for !e.resigned.Load() {
			// Update the lease time for the leader, every second.
			log.Info("update lease")
			err := e.Proclaim()
			if err != nil {
				if err == ErrNotLeader {
					log.Error("someone becomea leader, stop updating lease")
				}
				return
			}
			time.Sleep(time.Second)
		}
	}()
	return true, nil
}

func (e *Election) isLeaderTimeout() bool {
	stmt := `
		SELECT revision, term
		FROM tielect
		WHERE name = ?
		`
	var revision, term int64
	err := e.db.QueryRow(stmt, e.name).Scan(&revision, &term)
	if err != nil {
		log.E(err)
		return false
	}

	if e.currentTerm != term {
		e.currentTerm = term
		e.currentRevision = revision
		e.sinceLastUpdate = time.Now()
		return false
	}

	if e.currentRevision != revision {
		e.currentRevision = revision
		e.sinceLastUpdate = time.Now()
		return false
	}

	if e.currentRevision == revision &&
		e.sinceLastUpdate.Add(time.Second*DefaultLeaseInSec).Before(time.Now()) {
		return true
	}

	return false
}

func (e *Election) getCurrentLeaderID() string {
	stmt := `
		SELECT
			leader_id
		FROM 
			tielect
		WHERE
			name = ?
	`
	var leaderID string
	err := e.db.QueryRow(stmt, e.name).Scan(&leaderID)
	if err != nil {
		log.E(err)
		return ""
	}
	return leaderID
}

func (e *Election) tryToBeLeader() (bool, error) {
	// begin txn
	// 1. get current term
	// 2. current term + 1
	// 3. revision = 0
	// 4. leader_id = candidate_id
	// 5. update tielect
	// 6. commit txn
	txn, err := e.db.Begin()
	if err != nil {
		return false, err
	}
	defer txn.Rollback()

	log.I("try to be leader")
	stmt := `
		SELECT
			term, revision
		FROM
			tielect
		WHERE
			name = ? 
		FOR UPDATE
		`
	var term, revision int64
	err = txn.QueryRow(stmt, e.name).Scan(&term, &revision)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if err == sql.ErrNoRows {
		stmt = `
			INSERT INTO 
				tielect (name, leader_id, value, term, revision)
			VALUES 
				(?, ?, ?, ?, ?)
			`
		_, err = txn.Exec(stmt, e.name, e.c.ID(), e.c.Val(), 0, 0)
	} else {
		stmt = `
			UPDATE 
				tielect
			SET
				term = ?,
				revision = ?,
				leader_id = ?
			WHERE 
				name = ?
			`
		_, err = txn.Exec(stmt, term+1, 0, e.c.ID(), e.name)
	}
	if err != nil {
		return false, err
	}
	err = txn.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Proclaim will update the lease time for the leader.
// if current candidate is not leader will return ErrNotLeader.
func (e *Election) Proclaim() error {
	stmt := `
		UPDATE
			tielect
		SET 
			revision = revision + 1
		WHERE
			name = ? AND leader_id = ? AND term = ?
	`
	ret, err := e.db.Exec(stmt, e.name, e.c.ID(), e.currentTerm)
	if err != nil {
		return err
	}
	if c, err := ret.RowsAffected(); err != nil {
		return err
	} else if c == 0 {
		return ErrNotLeader
	}
	return nil
}

func (e *Election) getCurrentTerm() (int64, error) {
	stmt := `
		SELECT 
			term
		FROM
			tielect
		WHERE
			name = ?
	`
	var term int64
	err := e.db.QueryRow(stmt, e.name).Scan(&term)
	if err != nil {
		return 0, err
	}
	return term, nil
}

// Leader returns the current leader.
func (e *Election) Leader() (Candidate, error) {
	return nil, nil
}

// Resign will resign the current leader.
func (e *Election) Resign(name string) error {
	// update the leader_id to empty string
	log.Info("resign leader", e.c.ID())
	stmt := `
		UPDATE
			tielect
		SET
			leader_id = ""
		WHERE
			name = ? AND leader_id = ? AND term = ?
	`
	_, err := e.db.Exec(stmt, e.name, e.c.ID(), e.currentTerm)
	if err != nil {
		return err
	}
	// stop updating lease
	e.resigned.Store(true)
	return nil
}
