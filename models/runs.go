package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"gopkg.in/guregu/null.v3"
)

type FlowID int64

type ExitType null.String

var (
	ExitInterrupted = null.NewString("I", true)
	ExitCompleted   = null.NewString("C", true)
	ExitExpired     = null.NewString("E", true)
)

type FlowRun struct {
	ID         FlowID        `db:"id"`
	UUID       flows.RunUUID `db:"uuid"`
	IsActive   bool          `db:"is_active"`
	CreatedOn  time.Time     `db:"created_on"`
	ModifiedOn time.Time     `db:"modified_on"`
	ExitedOn   *time.Time    `db:"exited_on"`
	ExitType   null.String   `db:"exit_type"`
	ExpiresOn  *time.Time    `db:"expires_on"`
	TimeoutOn  *time.Time    `db:"timeout_on"`
	Responded  bool          `db:"responded"`

	// TODO: should this be a complex object that can read / write iself to the DB as JSON?
	Results string `db:"results"`

	// TODO: should this be a complex object that can read / write iself to the DB as JSON?
	Path string `db:"path"`

	// TODO: should this be a complex object that can read / write iself to the DB as JSON?
	Events          string         `db:"events"`
	CurrentNodeUUID flows.NodeUUID `db:"current_node_uuid"`
	ContactID       int            `db:"contact_id"`
	FlowID          FlowID         `db:"flow_id"`
	OrgID           OrgID          `db:"org_id"`
	ParentID        null.Int       `db:"parent_id"`
	SessionID       SessionID      `db:"session_id"`
	StartID         null.Int       `db:"start_id"`
}

type Step struct {
	UUID      flows.StepUUID `json:"uuid"`
	NodeUUID  flows.NodeUUID `json:"node_uuid"`
	ArrivedOn time.Time      `json:"arrived_on"`
	ExitUUID  flows.ExitUUID `json:"exit_uuid,omitempty"`
}

const insertRunSQL = `
INSERT INTO
flows_flowrun(uuid, is_active, created_on, modified_on, exited_on, exit_type, expires_on, responded, results, path, 
	          events, current_node_uuid, contact_id, flow_id, org_id, session_id)
	   VALUES(:uuid, :is_active, :created_on, NOW(), :exited_on, :exit_type, :expires_on, :responded, :results, :path,
	          :events, :current_node_uuid, :contact_id, :flow_id, :org_id, :session_id)
RETURNING id
`

func CreateRun(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, r flows.FlowRun) (*FlowRun, error) {
	flowID, err := org.GetFlowID(r.Flow().UUID())
	if err != nil {
		return nil, err
	}

	run := &FlowRun{
		UUID:      r.UUID(),
		CreatedOn: r.CreatedOn(),
		ExitedOn:  r.ExitedOn(),
		ExpiresOn: r.ExpiresOn(),
		ContactID: r.Contact().ID(),
		FlowID:    flowID,
		SessionID: session.ID,
		StartID:   null.NewInt(0, false),
		OrgID:     org.GetOrgID(),
	}

	// build our path elements
	path := make([]Step, len(r.Path()))
	for i, p := range r.Path() {
		path[i].UUID = p.UUID()
		path[i].NodeUUID = p.NodeUUID()
		path[i].ArrivedOn = p.ArrivedOn()
		path[i].ExitUUID = p.ExitUUID()
	}
	pathJSON, err := json.Marshal(path)
	if err != nil {
		return nil, err
	}
	run.Path = string(pathJSON)

	// set our current node uuid
	run.CurrentNodeUUID = path[len(path)-1].NodeUUID

	// set our exit type if we exited
	if r.Status() != flows.RunStatusActive {
		if r.Status() == flows.RunStatusErrored {
			run.ExitType = ExitInterrupted
		} else {
			run.ExitType = ExitCompleted
		}
		run.IsActive = false
	} else {
		run.IsActive = true
	}

	// only keep track of some events
	// TODO: combine this with application of events below?
	filteredEvents := make([]flows.Event, 0)
	for _, e := range r.Events() {
		if e.Type() == "msg_created" || e.Type() == "msg_received" {
			filteredEvents = append(filteredEvents, e)
		}
	}
	eventJSON, err := json.Marshal(filteredEvents)
	if err != nil {
		return nil, err
	}
	run.Events = string(eventJSON)

	resultsJSON, err := json.Marshal(r.Results())
	if err != nil {
		return nil, err
	}
	run.Results = string(resultsJSON)

	// TODO: set responded (always false for now)
	// TODO: set parent id (always null for now)

	// ok, insert our run
	rows, err := tx.NamedQuery(insertRunSQL, run)
	if err != nil {
		return nil, err
	}
	rows.Next()
	err = rows.Scan(&run.ID)
	if err != nil {
		return nil, err
	}

	rows.Close()

	// now apply our events
	for _, evt := range filteredEvents {
		err := ApplyEvent(ctx, tx, org, session, run, evt)
		if err != nil {
			return nil, err
		}
	}

	return run, nil
}
