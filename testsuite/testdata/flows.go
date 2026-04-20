package testdata

import (
	"os"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type Flow struct {
	ID   models.FlowID
	UUID assets.FlowUUID
}

func (f *Flow) Load(rt *runtime.Runtime, oa *models.OrgAssets) *models.Flow {
	flow, err := oa.FlowByID(f.ID)
	if err != nil {
		panic(err)
	}
	return flow
}

func (f *Flow) Reference() *assets.FlowReference {
	return &assets.FlowReference{UUID: f.UUID, Name: ""}
}

// InsertFlow inserts a flow
func InsertFlow(rt *runtime.Runtime, org *Org, definition []byte) *Flow {
	uuid, err := jsonparser.GetString(definition, "uuid")
	if err != nil {
		panic(err)
	}
	name, err := jsonparser.GetString(definition, "name")
	if err != nil {
		panic(err)
	}

	var id models.FlowID
	must(rt.DB.Get(&id,
		`INSERT INTO flows_flow(org_id, uuid, name, flow_type, version_number, base_language, expires_after_minutes, ignore_triggers, has_issues, is_active, is_archived, is_system, created_by_id, created_on, modified_by_id, modified_on, saved_on, saved_by_id) 
		VALUES($1, $2, $3, 'M', '13.1.0', 'eng', 10, FALSE, FALSE, TRUE, FALSE, FALSE, $4, NOW(), $4, NOW(), NOW(), $4) RETURNING id`, org.ID, uuid, name, Admin.ID,
	))

	rt.DB.MustExec(`INSERT INTO flows_flowrevision(flow_id, definition, spec_version, revision, created_by_id, created_on) 
	VALUES($1, $2, '13.1.0', 1, $3, NOW())`, id, definition, Admin.ID)

	return &Flow{ID: id, UUID: assets.FlowUUID(uuid)}
}

func ImportFlows(rt *runtime.Runtime, org *Org, path string) []*Flow {
	assetsJSON, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	flowsJSON, _, _, err := jsonparser.Get(assetsJSON, "flows")
	if err != nil {
		panic(err)
	}

	flows := []*Flow{}

	_, err = jsonparser.ArrayEach(flowsJSON, func(flowJSON []byte, dataType jsonparser.ValueType, offset int, err error) {
		flow := InsertFlow(rt, org, flowJSON)
		flows = append(flows, flow)
	})
	if err != nil {
		panic(err)
	}

	return flows
}

// InsertFlowStart inserts a flow start
func InsertFlowStart(rt *runtime.Runtime, org *Org, flow *Flow, contacts []*Contact) models.StartID {
	var id models.StartID
	must(rt.DB.Get(&id,
		`INSERT INTO flows_flowstart(uuid, org_id, flow_id, start_type, exclusions, created_on, modified_on, contact_count, status, created_by_id)
		 VALUES($1, $2, $3, 'M', '{}', NOW(), NOW(), 2, 'P', 1) RETURNING id`, uuids.New(), org.ID, flow.ID,
	))

	for _, c := range contacts {
		rt.DB.MustExec(`INSERT INTO flows_flowstart_contacts(flowstart_id, contact_id) VALUES($1, $2)`, id, c.ID)
	}

	return id
}

// InsertFlowSession inserts a flow session
func InsertFlowSession(rt *runtime.Runtime, org *Org, contact *Contact, sessionType models.FlowType, status models.SessionStatus, currentFlow *Flow, callID models.CallID) models.SessionID {
	now := time.Now()
	tomorrow := now.Add(time.Hour * 24)

	var waitStartedOn, waitExpiresOn, endedOn *time.Time
	if status == models.SessionStatusWaiting {
		waitStartedOn = &now
		waitExpiresOn = &tomorrow
	} else {
		endedOn = &now
	}

	var id models.SessionID
	must(rt.DB.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, output, responded, created_on, session_type, current_flow_id, call_id, wait_started_on, wait_expires_on, wait_resume_on_expire, ended_on) 
		 VALUES($1, $2, $3, $4, '{}', TRUE, NOW(), $5, $6, $7, $8, $9, FALSE, $10) RETURNING id`, uuids.New(), org.ID, contact.ID, status, sessionType, currentFlow.ID, callID, waitStartedOn, waitExpiresOn, endedOn,
	))
	return id
}

// InsertWaitingSession inserts a waiting flow session
func InsertWaitingSession(rt *runtime.Runtime, org *Org, contact *Contact, sessionType models.FlowType, currentFlow *Flow, callID models.CallID, waitStartedOn, waitExpiresOn time.Time, waitResumeOnExpire bool, waitTimeoutOn *time.Time) models.SessionID {
	var id models.SessionID
	must(rt.DB.Get(&id,
		`INSERT INTO flows_flowsession(uuid, org_id, contact_id, status, output, responded, created_on, session_type, current_flow_id, call_id, wait_started_on, wait_expires_on, wait_resume_on_expire, timeout_on) 
		 VALUES($1, $2, $3, 'W', '{"status":"waiting"}', TRUE, NOW(), $4, $5, $6, $7, $8, $9, $10) RETURNING id`, uuids.New(), org.ID, contact.ID, sessionType, currentFlow.ID, callID, waitStartedOn, waitExpiresOn, waitResumeOnExpire, waitTimeoutOn,
	))
	return id
}

// InsertFlowRun inserts a flow run
func InsertFlowRun(rt *runtime.Runtime, org *Org, sessionID models.SessionID, contact *Contact, flow *Flow, status models.RunStatus) models.FlowRunID {
	now := time.Now()

	var exitedOn *time.Time
	if status != models.RunStatusActive && status != models.RunStatusWaiting {
		exitedOn = &now
	}

	var id models.FlowRunID
	must(rt.DB.Get(&id,
		`INSERT INTO flows_flowrun(uuid, org_id, session_id, contact_id, flow_id, status, responded, created_on, modified_on, exited_on) 
		 VALUES($1, $2, $3, $4, $5, $6, TRUE, NOW(), NOW(), $7) RETURNING id`, uuids.New(), org.ID, sessionID, contact.ID, flow.ID, status, exitedOn,
	))
	return id
}
