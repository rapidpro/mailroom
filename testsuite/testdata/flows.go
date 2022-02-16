package testdata

import (
	"time"

	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"
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
	name, err := jsonparser.GetString(definition, "name")
	if err != nil {
		panic(err)
	}

	var id models.FlowID
	must(db.Get(&id,
		`INSERT INTO flows_flow(org_id, uuid, name, flow_type, version_number, expires_after_minutes, ignore_triggers, has_issues, is_active, is_archived, is_system, created_by_id, created_on, modified_by_id, modified_on, saved_on, saved_by_id) 
		VALUES($1, $2, $3, 'M', 1, 10, FALSE, FALSE, TRUE, FALSE, FALSE, $4, NOW(), $4, NOW(), NOW(), $4) RETURNING id`, org.ID, uuid, name, Admin.ID,
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
func InsertFlowSession(db *sqlx.DB, org *Org, contact *Contact, sessionType models.FlowType, status models.SessionStatus, currentFlow *Flow, connectionID models.ConnectionID) models.SessionID {
	now := time.Now()
	tomorrow := now.Add(time.Hour * 24)

	var waitStartedOn, waitExpiresOn *time.Time
	if status == models.SessionStatusWaiting {
		waitStartedOn = &now
		waitExpiresOn = &tomorrow
	}

	var id models.SessionID
	must(db.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, responded, created_on, session_type, current_flow_id, connection_id, wait_started_on, wait_expires_on, wait_resume_on_expire) 
		 VALUES($1, $2, $3, $4, TRUE, NOW(), $5, $6, $7, $8, $9, FALSE) RETURNING id`, uuids.New(), org.ID, contact.ID, status, sessionType, currentFlow.ID, connectionID, waitStartedOn, waitExpiresOn,
	))
	return id
}

// InsertWaitingSession inserts a waiting flow session
func InsertWaitingSession(db *sqlx.DB, org *Org, contact *Contact, sessionType models.FlowType, currentFlow *Flow, connectionID models.ConnectionID, waitStartedOn, waitExpiresOn time.Time, waitResumeOnExpire bool, waitTimeoutOn *time.Time) models.SessionID {
	var id models.SessionID
	must(db.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, responded, created_on, session_type, current_flow_id, connection_id, wait_started_on, wait_expires_on, wait_resume_on_expire, timeout_on) 
		 VALUES($1, $2, $3, 'W', TRUE, NOW(), $4, $5, $6, $7, $8, $9, $10) RETURNING id`, uuids.New(), org.ID, contact.ID, sessionType, currentFlow.ID, connectionID, waitStartedOn, waitExpiresOn, waitResumeOnExpire, waitTimeoutOn,
	))
	return id
}

// InsertFlowRun inserts a flow run
func InsertFlowRun(db *sqlx.DB, org *Org, sessionID models.SessionID, contact *Contact, flow *Flow, status models.RunStatus) models.FlowRunID {
	isActive := status == models.RunStatusActive || status == models.RunStatusWaiting

	var id models.FlowRunID
	must(db.Get(&id,
		`INSERT INTO flows_flowrun(uuid, org_id, session_id, contact_id, flow_id, status, is_active, responded, created_on, modified_on) 
		 VALUES($1, $2, $3, $4, $5, $6, $7, TRUE, NOW(), NOW()) RETURNING id`, uuids.New(), org.ID, null.Int(sessionID), contact.ID, flow.ID, status, isActive,
	))
	return id
}
