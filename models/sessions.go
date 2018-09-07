package models

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
)

var sessionStatusMap = map[flows.SessionStatus]SessionStatus{
	flows.SessionStatusActive:    SessionStatus("A"),
	flows.SessionStatusCompleted: SessionStatus("C"),
	flows.SessionStatusErrored:   SessionStatus("E"),
	flows.SessionStatusWaiting:   SessionStatus("W"),
}

type SessionID float64
type SessionStatus string

type Session struct {
	ID        SessionID     `db:"id"`
	Status    SessionStatus `db:"status"`
	Responded bool          `db:"responded"`
	Output    string        `db:"output"`
	ContactID ContactID     `db:"contact_id"`
	OrgID     OrgID         `db:"org_id"`
	CreatedOn time.Time

	flowIDs map[flows.FlowUUID]FlowID
	runs    []*FlowRun
	outbox  []*Msg
}

func (s *Session) AddFlowIDMapping(uuid flows.FlowUUID, id FlowID) {
	s.flowIDs[uuid] = id
}

func (s *Session) GetFlowID(uuid flows.FlowUUID) (FlowID, bool) {
	id, found := s.flowIDs[uuid]
	return id, found
}

func (s *Session) AddOutboxMsg(m *Msg) {
	s.outbox = append(s.outbox, m)
}

func (s *Session) GetOutbox() []*Msg {
	return s.outbox
}

const insertSessionSQL = `
INSERT INTO
flows_flowsession(status, responded, output, contact_id, org_id)
           VALUES(:status, :responded, :output, :contact_id, :org_id)
RETURNING id
`

// CreateSession writes the passed in session to our database, writes any runs that need to be created
// as well as appying any events created in the session
func CreateSession(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, s flows.Session) (*Session, error) {
	output, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	sessionStatus, found := sessionStatusMap[s.Status()]
	if !found {
		return nil, fmt.Errorf("unknown session status: %s", s.Status())
	}

	session := &Session{
		Status:    sessionStatus,
		Responded: false,
		Output:    string(output),
		ContactID: ContactID(s.Contact().ID()),
		OrgID:     org.GetOrgID(),
		CreatedOn: s.Runs()[0].CreatedOn(), // TODO: do something more sane here
	}

	rows, err := tx.NamedQuery(insertSessionSQL, session)
	if err != nil {
		return nil, err
	}
	rows.Next()
	err = rows.Scan(&session.ID)
	rows.Close()

	if err != nil {
		return nil, err
	}

	// then our runs
	for _, r := range s.Runs() {
		run, err := CreateRun(ctx, tx, org, session, r)
		if err != nil {
			return nil, err
		}

		// TODO: probably need to store flow run uuid to id mapping in db session for parent id lookups

		// save the run to our session
		session.runs = append(session.runs, run)

		// apply any events
	}

	// return our session
	return session, nil
}
