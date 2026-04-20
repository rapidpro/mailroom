package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

// StartID is our type for flow start idst
type StartID int

// NilStartID is our constant for a nil start id
var NilStartID = StartID(0)

func (i *StartID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i StartID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *StartID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i StartID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

// StartType is the type for the type of a start
type StartType string

// start type constants
const (
	StartTypeManual     = StartType("M")
	StartTypeAPI        = StartType("A")
	StartTypeAPIZapier  = StartType("Z")
	StartTypeFlowAction = StartType("F")
	StartTypeTrigger    = StartType("T")
)

// StartStatus is the type for the status of a start
type StartStatus string

// start status constants
const (
	StartStatusPending  = StartStatus("P")
	StartStatusStarting = StartStatus("S")
	StartStatusComplete = StartStatus("C")
	StartStatusFailed   = StartStatus("F")
)

// Exclusions are preset exclusion conditions
type Exclusions struct {
	NonActive         bool `json:"non_active"`          // contacts who are blocked, stopped or archived
	InAFlow           bool `json:"in_a_flow"`           // contacts who are currently in a flow (including this one)
	StartedPreviously bool `json:"started_previously"`  // contacts who have been in this flow in the last 90 days
	NotSeenSinceDays  int  `json:"not_seen_since_days"` // contacts who have not been seen for more than this number of days
}

// NoExclusions is a constant for the empty value
var NoExclusions = Exclusions{}

// Scan supports reading exclusion values from JSON in database
func (e *Exclusions) Scan(value any) error {
	if value == nil {
		*e = Exclusions{}
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("failed type assertion to []byte")
	}
	return json.Unmarshal(b, &e)
}

func (e Exclusions) Value() (driver.Value, error) { return json.Marshal(e) }

// FlowStart represents the top level flow start in our system
type FlowStart struct {
	ID          StartID    `json:"start_id"      db:"id"`
	UUID        uuids.UUID `json:"-"             db:"uuid"`
	StartType   StartType  `json:"start_type"    db:"start_type"`
	OrgID       OrgID      `json:"org_id"        db:"org_id"`
	CreatedByID UserID     `json:"created_by_id" db:"created_by_id"`
	FlowID      FlowID     `json:"flow_id"       db:"flow_id"`

	URNs            []urns.URN  `json:"urns,omitempty"`
	ContactIDs      []ContactID `json:"contact_ids,omitempty"`
	GroupIDs        []GroupID   `json:"group_ids,omitempty"`
	ExcludeGroupIDs []GroupID   `json:"exclude_group_ids,omitempty"` // used when loading scheduled triggers as flow starts
	Query           null.String `json:"query,omitempty"        db:"query"`
	CreateContact   bool        `json:"create_contact"`
	Exclusions      Exclusions  `json:"exclusions"             db:"exclusions"`

	Params         null.JSON `json:"params,omitempty"          db:"params"`
	ParentSummary  null.JSON `json:"parent_summary,omitempty"  db:"parent_summary"`
	SessionHistory null.JSON `json:"session_history,omitempty" db:"session_history"`
}

// NewFlowStart creates a new flow start objects for the passed in parameters
func NewFlowStart(orgID OrgID, startType StartType, flowID FlowID) *FlowStart {
	return &FlowStart{UUID: uuids.New(), OrgID: orgID, StartType: startType, FlowID: flowID}
}

func (s *FlowStart) WithGroupIDs(groupIDs []GroupID) *FlowStart {
	s.GroupIDs = groupIDs
	return s
}

func (s *FlowStart) WithExcludeGroupIDs(groupIDs []GroupID) *FlowStart {
	s.ExcludeGroupIDs = groupIDs
	return s
}

func (s *FlowStart) WithContactIDs(contactIDs []ContactID) *FlowStart {
	s.ContactIDs = contactIDs
	return s
}

func (s *FlowStart) WithURNs(us []urns.URN) *FlowStart {
	s.URNs = us
	return s
}

func (s *FlowStart) WithQuery(query string) *FlowStart {
	s.Query = null.String(query)
	return s
}

func (s *FlowStart) WithExcludeStartedPreviously(exclude bool) *FlowStart {
	s.Exclusions.StartedPreviously = exclude
	return s
}

func (s *FlowStart) WithExcludeInAFlow(exclude bool) *FlowStart {
	s.Exclusions.InAFlow = exclude
	return s
}

func (s *FlowStart) WithCreateContact(create bool) *FlowStart {
	s.CreateContact = create
	return s
}

func (s *FlowStart) WithParentSummary(sum json.RawMessage) *FlowStart {
	s.ParentSummary = null.JSON(sum)
	return s
}

func (s *FlowStart) WithSessionHistory(history json.RawMessage) *FlowStart {
	s.SessionHistory = null.JSON(history)
	return s
}

func (s *FlowStart) WithParams(params json.RawMessage) *FlowStart {
	s.Params = null.JSON(params)
	return s
}

// MarkStartStarted sets the status for the passed in flow start to S and updates the contact count on it
func MarkStartStarted(ctx context.Context, db DBorTx, startID StartID, contactCount int) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'S', contact_count = $2, modified_on = NOW() WHERE id = $1", startID, contactCount)
	return errors.Wrapf(err, "error setting start as started")
}

// MarkStartComplete sets the status for the passed in flow start
func MarkStartComplete(ctx context.Context, db DBorTx, startID StartID) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'C', modified_on = NOW() WHERE id = $1", startID)
	return errors.Wrapf(err, "error marking flow start as complete")
}

// MarkStartFailed sets the status for the passed in flow start to F
func MarkStartFailed(ctx context.Context, db DBorTx, startID StartID) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'F', modified_on = NOW() WHERE id = $1", startID)
	return errors.Wrapf(err, "error setting flow start as failed")
}

// GetFlowStartAttributes gets the basic attributes for the passed in start id, this includes ONLY its id, uuid, flow_id and params
func GetFlowStartAttributes(ctx context.Context, db DBorTx, startID StartID) (*FlowStart, error) {
	start := &FlowStart{}
	err := db.GetContext(ctx, start, `SELECT id, uuid, flow_id, params, parent_summary, session_history FROM flows_flowstart WHERE id = $1`, startID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load start attributes for id: %d", startID)
	}
	return start, nil
}

type startContact struct {
	StartID   StartID   `db:"flowstart_id"`
	ContactID ContactID `db:"contact_id"`
}

type startGroup struct {
	StartID StartID `db:"flowstart_id"`
	GroupID GroupID `db:"contactgroup_id"`
}

// InsertFlowStarts inserts all the passed in starts
func InsertFlowStarts(ctx context.Context, db DBorTx, starts []*FlowStart) error {
	// insert our starts
	err := BulkQuery(ctx, "inserting flow start", db, sqlInsertStart, starts)
	if err != nil {
		return errors.Wrapf(err, "error inserting flow starts")
	}

	// build up all our contact associations
	contacts := make([]*startContact, 0, len(starts))
	for _, start := range starts {
		for _, contactID := range start.ContactIDs {
			contacts = append(contacts, &startContact{StartID: start.ID, ContactID: contactID})
		}
	}

	// insert our contacts
	err = BulkQuery(ctx, "inserting flow start contacts", db, sqlInsertStartContact, contacts)
	if err != nil {
		return errors.Wrapf(err, "error inserting flow start contacts for flow")
	}

	// build up all our group associations
	groups := make([]*startGroup, 0, len(starts))
	for _, start := range starts {
		for _, groupID := range start.GroupIDs {
			groups = append(groups, &startGroup{StartID: start.ID, GroupID: groupID})
		}
	}

	// insert our groups
	err = BulkQuery(ctx, "inserting flow start groups", db, sqlInsertStartGroup, groups)
	if err != nil {
		return errors.Wrapf(err, "error inserting flow start groups for flow")
	}

	return nil
}

const sqlInsertStart = `
INSERT INTO
	flows_flowstart(uuid,  org_id,  flow_id,  start_type,  created_on, modified_on, query,  exclusions,  status, params,  parent_summary,  session_history)
			 VALUES(:uuid, :org_id, :flow_id, :start_type, NOW(),      NOW(),       :query, :exclusions, 'P',    :params, :parent_summary, :session_history)
RETURNING
	id
`

const sqlInsertStartContact = `
INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES(:flowstart_id, :contact_id)`

const sqlInsertStartGroup = `
INSERT INTO flows_flowstart_groups(flowstart_id, contactgroup_id) VALUES(:flowstart_id, :contactgroup_id)`

// CreateBatch creates a batch for this start using the passed in contact ids
func (s *FlowStart) CreateBatch(contactIDs []ContactID, flowType FlowType, last bool, totalContacts int) *FlowStartBatch {
	return &FlowStartBatch{
		StartID:        s.ID,
		StartType:      s.StartType,
		OrgID:          s.OrgID,
		FlowID:         s.FlowID,
		FlowType:       flowType,
		ContactIDs:     contactIDs,
		ParentSummary:  s.ParentSummary,
		SessionHistory: s.SessionHistory,
		Params:         s.Params,
		CreatedByID:    s.CreatedByID,
		IsLast:         last,
		TotalContacts:  totalContacts,
	}
}

// FlowStartBatch represents a single flow batch that needs to be started
type FlowStartBatch struct {
	StartID     StartID     `json:"start_id"`
	StartType   StartType   `json:"start_type"`
	OrgID       OrgID       `json:"org_id"`
	CreatedByID UserID      `json:"created_by_id"`
	FlowID      FlowID      `json:"flow_id"`
	FlowType    FlowType    `json:"flow_type"`
	ContactIDs  []ContactID `json:"contact_ids"`

	Params         null.JSON `json:"params,omitempty"`
	ParentSummary  null.JSON `json:"parent_summary,omitempty"`
	SessionHistory null.JSON `json:"session_history,omitempty"`

	IsLast        bool `json:"is_last,omitempty"`
	TotalContacts int  `json:"total_contacts"`
}

// ReadSessionHistory reads a session history from the given JSON
func ReadSessionHistory(data []byte) (*flows.SessionHistory, error) {
	h := &flows.SessionHistory{}
	return h, jsonx.Unmarshal(data, h)
}
