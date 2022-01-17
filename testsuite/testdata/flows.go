package testdata

import (
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
)

type Flow struct {
	ID   models.FlowID
	UUID assets.FlowUUID
}

func (f *Flow) Reference() *assets.FlowReference {
	return &assets.FlowReference{UUID: f.UUID, Name: ""}
}

// InsertFlow inserts a flow
func InsertFlow(db *sqlx.DB, org *Org, definition []byte) *Flow {
	uuid, err := jsonparser.GetString(definition, "uuid")
	if err != nil {
		panic(err)
	}

	var id models.FlowID
	must(db.Get(&id,
		`INSERT INTO flows_flow(uuid, org_id, name, flow_type, version_number, expires_after_minutes, ignore_triggers, has_issues, is_active, is_archived, is_system, created_by_id, created_on, modified_by_id, modified_on, saved_on, saved_by_id) 
		VALUES($1, $2, 'Test', 'M', 1, 10, FALSE, FALSE, TRUE, FALSE, FALSE, $3, NOW(), $3, NOW(), NOW(), $3) RETURNING id`, uuid, org.ID, Admin.ID,
	))

	db.MustExec(`INSERT INTO flows_flowrevision(flow_id, definition, spec_version, revision, is_active, created_by_id, created_on, modified_by_id, modified_on) 
	VALUES($1, $2, '13.1.0', 1, TRUE, $3, NOW(), $3, NOW())`, id, definition, Admin.ID)

	return &Flow{ID: id, UUID: assets.FlowUUID(uuid)}
}

// InsertFlowStart inserts a flow start
func InsertFlowStart(db *sqlx.DB, org *Org, flow *Flow, contacts []*Contact) models.StartID {
	var id models.StartID
	must(db.Get(&id,
		`INSERT INTO flows_flowstart(uuid, org_id, flow_id, start_type, created_on, modified_on, restart_participants, include_active, contact_count, status, created_by_id)
		 VALUES($1, $2, $3, 'M', NOW(), NOW(), TRUE, TRUE, 2, 'P', 1) RETURNING id`, uuids.New(), org.ID, flow.ID,
	))

	for _, c := range contacts {
		db.MustExec(`INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES($1, $2)`, id, c.ID)
	}

	return id
}

// InsertFlowSession inserts a flow session
func InsertFlowSession(db *sqlx.DB, org *Org, contact *Contact, status models.SessionStatus, timeoutOn *time.Time) models.SessionID {
	var id models.SessionID
	must(db.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, responded, created_on, timeout_on, session_type) 
		 VALUES($1, $2, $3, $4, TRUE, NOW(), $5, 'M') RETURNING id`, uuids.New(), org.ID, contact.ID, status, timeoutOn,
	))
	return id
}

// InsertFlowRun inserts a flow run
func InsertFlowRun(db *sqlx.DB, org *Org, sessionID models.SessionID, contact *Contact, flow *Flow, status models.RunStatus, parent flows.RunUUID, expiresOn *time.Time) models.FlowRunID {
	isActive := status == models.RunStatusActive || status == models.RunStatusWaiting

	var id models.FlowRunID
	must(db.Get(&id,
		`INSERT INTO flows_flowrun(uuid, org_id, session_id, contact_id, flow_id, status, is_active, parent_uuid, responded, created_on, modified_on, expires_on) 
		 VALUES($1, $2, $3, $4, $5, $6, $7, $8, TRUE, NOW(), NOW(), $9) RETURNING id`, uuids.New(), org.ID, null.Int(sessionID), contact.ID, flow.ID, status, isActive, null.String(parent), expiresOn,
	))
	return id
}
