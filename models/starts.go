package models

import (
	"context"
	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	null "gopkg.in/guregu/null.v3"
)

// StartID is our type for flow start ids
type StartID struct {
	null.Int
}

// NilStartID is our constant for a nil start id
var NilStartID = StartID{null.NewInt(0, false)}

// NewStartID creates a new start id for the passed in int
func NewStartID(id int) StartID {
	return StartID{null.NewInt(int64(id), true)}
}

// MarkStartComplete sets the status for the passed in flow start
func MarkStartComplete(ctx context.Context, db *sqlx.DB, startID StartID) error {
	_, err := db.Exec("UPDATE flows_flowstart SET status = 'C' WHERE id = $1", startID)
	if err != nil {
		return errors.Annotatef(err, "error setting start as complete")
	}
	return nil
}

// FlowStartBatch represents a single flow batch that needs to be started
type FlowStartBatch struct {
	b struct {
		StartID             StartID           `json:"start_id"`
		OrgID               OrgID             `json:"org_id"`
		FlowID              FlowID            `json:"flow_id"`
		ContactIDs          []flows.ContactID `json:"contact_ids"`
		RestartParticipants bool              `json:"restart_participants"`
		IncludeActive       bool              `json:"include_active"`
		IsLast              bool              `json:"is_last,omitempty"`
	}
}

func (b *FlowStartBatch) StartID() StartID              { return b.b.StartID }
func (b *FlowStartBatch) OrgID() OrgID                  { return b.b.OrgID }
func (b *FlowStartBatch) FlowID() FlowID                { return b.b.FlowID }
func (b *FlowStartBatch) ContactIDs() []flows.ContactID { return b.b.ContactIDs }
func (b *FlowStartBatch) RestartParticipants() bool     { return b.b.RestartParticipants }
func (b *FlowStartBatch) IncludeActive() bool           { return b.b.IncludeActive }
func (b *FlowStartBatch) IsLast() bool                  { return b.b.IsLast }
func (b *FlowStartBatch) SetIsLast(last bool)           { b.b.IsLast = last }

func (b *FlowStartBatch) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *FlowStartBatch) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

// FlowStart represents the top level flow start in our system
type FlowStart struct {
	s struct {
		StartID             StartID           `json:"start_id"`
		OrgID               OrgID             `json:"org_id"`
		FlowID              FlowID            `json:"flow_id"`
		GroupIDs            []GroupID         `json:"group_ids"`
		ContactIDs          []flows.ContactID `json:"contact_ids"`
		RestartParticipants bool              `json:"restart_participants"`
		IncludeActive       bool              `json:"include_active"`
	}
}

func (s *FlowStart) StartID() StartID              { return s.s.StartID }
func (s *FlowStart) OrgID() OrgID                  { return s.s.OrgID }
func (s *FlowStart) FlowID() FlowID                { return s.s.FlowID }
func (s *FlowStart) GroupIDs() []GroupID           { return s.s.GroupIDs }
func (s *FlowStart) ContactIDs() []flows.ContactID { return s.s.ContactIDs }
func (s *FlowStart) RestartParticipants() bool     { return s.s.RestartParticipants }
func (s *FlowStart) IncludeActive() bool           { return s.s.IncludeActive }

func (s *FlowStart) MarshalJSON() ([]byte, error)    { return json.Marshal(s.s) }
func (s *FlowStart) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &s.s) }

func NewFlowStart(startID StartID, orgID OrgID, flowID FlowID, groupIDs []GroupID, contactIDs []flows.ContactID, restartParticipants bool, includeActive bool) *FlowStart {
	s := &FlowStart{}
	s.s.StartID = startID
	s.s.OrgID = orgID
	s.s.FlowID = flowID
	s.s.GroupIDs = groupIDs
	s.s.ContactIDs = contactIDs
	s.s.RestartParticipants = restartParticipants
	s.s.IncludeActive = includeActive
	return s
}

func (s *FlowStart) CreateBatch(contactIDs []flows.ContactID) *FlowStartBatch {
	b := &FlowStartBatch{}
	b.b.StartID = s.StartID()
	b.b.OrgID = s.OrgID()
	b.b.FlowID = s.FlowID()
	b.b.ContactIDs = contactIDs
	b.b.RestartParticipants = s.RestartParticipants()
	b.b.IncludeActive = s.IncludeActive()
	return b
}
