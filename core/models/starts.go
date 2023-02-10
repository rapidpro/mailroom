package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/null/v2"
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

// MarkStartStarted sets the status for the passed in flow start to S and updates the contact count on it
func MarkStartStarted(ctx context.Context, db Queryer, startID StartID, contactCount int, createdContactIDs []ContactID) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'S', contact_count = $2, modified_on = NOW() WHERE id = $1", startID, contactCount)
	if err != nil {
		return errors.Wrapf(err, "error setting start as started")
	}

	// if we created contacts, add them to the start for logging
	if len(createdContactIDs) > 0 {
		type startContact struct {
			StartID   StartID   `db:"flowstart_id"`
			ContactID ContactID `db:"contact_id"`
		}

		args := make([]*startContact, len(createdContactIDs))
		for i, id := range createdContactIDs {
			args[i] = &startContact{StartID: startID, ContactID: id}
		}
		return BulkQuery(
			ctx, "adding created contacts to flow start", db,
			`INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES(:flowstart_id, :contact_id) ON CONFLICT DO NOTHING`,
			args,
		)
	}
	return nil
}

// MarkStartComplete sets the status for the passed in flow start
func MarkStartComplete(ctx context.Context, db Queryer, startID StartID) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'C', modified_on = NOW() WHERE id = $1", startID)
	return errors.Wrapf(err, "error marking flow start as complete")
}

// MarkStartFailed sets the status for the passed in flow start to F
func MarkStartFailed(ctx context.Context, db Queryer, startID StartID) error {
	_, err := db.ExecContext(ctx, "UPDATE flows_flowstart SET status = 'F', modified_on = NOW() WHERE id = $1", startID)
	return errors.Wrapf(err, "error setting flow start as failed")
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

	ParentSummary  null.JSON `json:"parent_summary,omitempty"`
	SessionHistory null.JSON `json:"session_history,omitempty"`
	Extra          null.JSON `json:"extra,omitempty"`

	RestartParticipants bool `json:"restart_participants"`
	IncludeActive       bool `json:"include_active"`

	IsLast        bool `json:"is_last,omitempty"`
	TotalContacts int  `json:"total_contacts"`
}

func (b *FlowStartBatch) ExcludeStartedPreviously() bool { return !b.RestartParticipants }
func (b *FlowStartBatch) ExcludeInAFlow() bool           { return !b.IncludeActive }

// FlowStart represents the top level flow start in our system
type FlowStart struct {
	ID          StartID    `json:"start_id"      db:"id"`
	UUID        uuids.UUID `                     db:"uuid"`
	StartType   StartType  `json:"start_type"    db:"start_type"`
	OrgID       OrgID      `json:"org_id"        db:"org_id"`
	CreatedByID UserID     `json:"created_by_id" db:"created_by_id"`
	FlowID      FlowID     `json:"flow_id"       db:"flow_id"`
	FlowType    FlowType   `json:"flow_type"`

	URNs            []urns.URN  `json:"urns,omitempty"`
	ContactIDs      []ContactID `json:"contact_ids,omitempty"`
	GroupIDs        []GroupID   `json:"group_ids,omitempty"`
	ExcludeGroupIDs []GroupID   `json:"exclude_group_ids,omitempty"` // used when loading scheduled triggers as flow starts
	Query           null.String `json:"query,omitempty"        db:"query"`
	CreateContact   bool        `json:"create_contact"`

	RestartParticipants bool `json:"restart_participants" db:"restart_participants"`
	IncludeActive       bool `json:"include_active"       db:"include_active"`

	Extra          null.JSON `json:"extra,omitempty"           db:"extra"`
	ParentSummary  null.JSON `json:"parent_summary,omitempty"  db:"parent_summary"`
	SessionHistory null.JSON `json:"session_history,omitempty" db:"session_history"`
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

func (s *FlowStart) ExcludeStartedPreviously() bool { return !s.RestartParticipants }
func (s *FlowStart) WithExcludeStartedPreviously(exclude bool) *FlowStart {
	s.RestartParticipants = !exclude
	return s
}

func (s *FlowStart) ExcludeInAFlow() bool { return !s.IncludeActive }
func (s *FlowStart) WithExcludeInAFlow(exclude bool) *FlowStart {
	s.IncludeActive = !exclude
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

func (s *FlowStart) WithExtra(extra json.RawMessage) *FlowStart {
	s.Extra = null.JSON(extra)
	return s
}

// GetFlowStartAttributes gets the basic attributes for the passed in start id, this includes ONLY its id, uuid, flow_id and extra
func GetFlowStartAttributes(ctx context.Context, db Queryer, startID StartID) (*FlowStart, error) {
	start := &FlowStart{}
	err := db.GetContext(ctx, start, `SELECT id, uuid, flow_id, extra, parent_summary, session_history FROM flows_flowstart WHERE id = $1`, startID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load start attributes for id: %d", startID)
	}
	return start, nil
}

// NewFlowStart creates a new flow start objects for the passed in parameters
func NewFlowStart(orgID OrgID, startType StartType, flowType FlowType, flowID FlowID) *FlowStart {
	return &FlowStart{
		UUID:                uuids.New(),
		OrgID:               orgID,
		StartType:           startType,
		FlowType:            flowType,
		FlowID:              flowID,
		RestartParticipants: true,
		IncludeActive:       true,
	}
}

type startContact struct {
	StartID   StartID   `db:"start_id"`
	ContactID ContactID `db:"contact_id"`
}

type startGroup struct {
	StartID StartID `db:"start_id"`
	GroupID GroupID `db:"contactgroup_id"`
}

// InsertFlowStarts inserts all the passed in starts
func InsertFlowStarts(ctx context.Context, db Queryer, starts []*FlowStart) error {
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
	flows_flowstart(uuid,  org_id,  flow_id,  start_type,  created_on,  modified_on,  restart_participants,  include_active,  query,  status, extra,  parent_summary,  session_history)
			 VALUES(:uuid, :org_id, :flow_id, :start_type, NOW(),       NOW(),        :restart_participants, :include_active, :query, 'P',    :extra, :parent_summary, :session_history)
RETURNING
	id
`

const sqlInsertStartContact = `
INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES(:start_id, :contact_id)`

const sqlInsertStartGroup = `
INSERT INTO flows_flowstart_groups(flowstart_id, contactgroup_id) VALUES(:start_id, :contactgroup_id)`

// CreateBatch creates a batch for this start using the passed in contact ids
func (s *FlowStart) CreateBatch(contactIDs []ContactID, last bool, totalContacts int) *FlowStartBatch {
	return &FlowStartBatch{
		StartID:             s.ID,
		StartType:           s.StartType,
		OrgID:               s.OrgID,
		FlowID:              s.FlowID,
		FlowType:            s.FlowType,
		ContactIDs:          contactIDs,
		RestartParticipants: s.RestartParticipants,
		IncludeActive:       s.IncludeActive,
		ParentSummary:       s.ParentSummary,
		SessionHistory:      s.SessionHistory,
		Extra:               s.Extra,
		IsLast:              last,
		TotalContacts:       totalContacts,
		CreatedByID:         s.CreatedByID,
	}
}

// ReadSessionHistory reads a session history from the given JSON
func ReadSessionHistory(data []byte) (*flows.SessionHistory, error) {
	h := &flows.SessionHistory{}
	return h, jsonx.Unmarshal(data, h)
}
