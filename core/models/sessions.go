package models

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/storage"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

type SessionID int64
type SessionStatus string

const (
	SessionStatusWaiting     SessionStatus = "W"
	SessionStatusCompleted   SessionStatus = "C"
	SessionStatusExpired     SessionStatus = "X"
	SessionStatusInterrupted SessionStatus = "I"
	SessionStatusFailed      SessionStatus = "F"

	storageTSFormat = "20060102T150405.999Z"
)

var sessionStatusMap = map[flows.SessionStatus]SessionStatus{
	flows.SessionStatusWaiting:   SessionStatusWaiting,
	flows.SessionStatusCompleted: SessionStatusCompleted,
	flows.SessionStatusFailed:    SessionStatusFailed,
}

type SessionCommitHook func(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, []*Session) error

// Session is the mailroom type for a FlowSession
type Session struct {
	s struct {
		ID                 SessionID         `db:"id"`
		UUID               flows.SessionUUID `db:"uuid"`
		SessionType        FlowType          `db:"session_type"`
		Status             SessionStatus     `db:"status"`
		Responded          bool              `db:"responded"`
		Output             null.String       `db:"output"`
		OutputURL          null.String       `db:"output_url"`
		ContactID          ContactID         `db:"contact_id"`
		OrgID              OrgID             `db:"org_id"`
		CreatedOn          time.Time         `db:"created_on"`
		EndedOn            *time.Time        `db:"ended_on"`
		WaitStartedOn      *time.Time        `db:"wait_started_on"`
		WaitTimeoutOn      *time.Time        `db:"timeout_on"`
		WaitExpiresOn      *time.Time        `db:"wait_expires_on"`
		WaitResumeOnExpire bool              `db:"wait_resume_on_expire"`
		CurrentFlowID      FlowID            `db:"current_flow_id"`
		CallID             *CallID           `db:"call_id"`
	}

	incomingMsgID      MsgID
	incomingExternalID null.String

	// any call associated with this flow session
	call *Call

	// time after our last message is sent that we should timeout
	timeout *time.Duration

	contact *flows.Contact
	runs    []*FlowRun

	seenRuns map[flows.RunUUID]time.Time

	// we keep around a reference to the sprint associated with this session
	sprint flows.Sprint

	// the scene for our event hooks
	scene *Scene

	findStep func(flows.StepUUID) (flows.Run, flows.Step)
}

func (s *Session) ID() SessionID                      { return s.s.ID }
func (s *Session) UUID() flows.SessionUUID            { return flows.SessionUUID(s.s.UUID) }
func (s *Session) SessionType() FlowType              { return s.s.SessionType }
func (s *Session) Status() SessionStatus              { return s.s.Status }
func (s *Session) Responded() bool                    { return s.s.Responded }
func (s *Session) Output() string                     { return string(s.s.Output) }
func (s *Session) OutputURL() string                  { return string(s.s.OutputURL) }
func (s *Session) ContactID() ContactID               { return s.s.ContactID }
func (s *Session) OrgID() OrgID                       { return s.s.OrgID }
func (s *Session) CreatedOn() time.Time               { return s.s.CreatedOn }
func (s *Session) EndedOn() *time.Time                { return s.s.EndedOn }
func (s *Session) WaitStartedOn() *time.Time          { return s.s.WaitStartedOn }
func (s *Session) WaitTimeoutOn() *time.Time          { return s.s.WaitTimeoutOn }
func (s *Session) WaitExpiresOn() *time.Time          { return s.s.WaitExpiresOn }
func (s *Session) WaitResumeOnExpire() bool           { return s.s.WaitResumeOnExpire }
func (s *Session) CurrentFlowID() FlowID              { return s.s.CurrentFlowID }
func (s *Session) CallID() *CallID                    { return s.s.CallID }
func (s *Session) IncomingMsgID() MsgID               { return s.incomingMsgID }
func (s *Session) IncomingMsgExternalID() null.String { return s.incomingExternalID }
func (s *Session) Scene() *Scene                      { return s.scene }

// StoragePath returns the path for the session
func (s *Session) StoragePath() string {
	ts := s.CreatedOn().UTC().Format(storageTSFormat)

	// example output: orgs/1/c/20a5/20a5534c-b2ad-4f18-973a-f1aa3b4e6c74/20060102T150405.123Z_session_8a7fc501-177b-4567-a0aa-81c48e6de1c5_51df83ac21d3cf136d8341f0b11cb1a7.json"
	return path.Join(
		"orgs",
		fmt.Sprintf("%d", s.OrgID()),
		"c",
		string(s.ContactUUID()[:4]),
		string(s.ContactUUID()),
		fmt.Sprintf("%s_session_%s_%s.json", ts, s.UUID(), s.OutputMD5()),
	)
}

// ContactUUID returns the UUID of our contact
func (s *Session) ContactUUID() flows.ContactUUID {
	return s.contact.UUID()
}

// Contact returns the contact for this session
func (s *Session) Contact() *flows.Contact {
	return s.contact
}

// Runs returns our flow run
func (s *Session) Runs() []*FlowRun {
	return s.runs
}

// Sprint returns the sprint associated with this session
func (s *Session) Sprint() flows.Sprint {
	return s.sprint
}

// FindStep finds the run and step with the given UUID
func (s *Session) FindStep(uuid flows.StepUUID) (flows.Run, flows.Step) {
	return s.findStep(uuid)
}

// Timeout returns the amount of time after our last message sends that we should timeout
func (s *Session) Timeout() *time.Duration {
	return s.timeout
}

// OutputMD5 returns the md5 of the passed in session
func (s *Session) OutputMD5() string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s.s.Output)))
}

// SetIncomingMsg set the incoming message that this session should be associated with in this sprint
func (s *Session) SetIncomingMsg(id MsgID, externalID null.String) {
	s.incomingMsgID = id
	s.incomingExternalID = externalID
}

// SetCall sets the channel connection associated with this sprint
func (s *Session) SetCall(c *Call) {
	connID := c.ID()
	s.s.CallID = &connID
	s.call = c
}

func (s *Session) Call() *Call {
	return s.call
}

// FlowSession creates a flow session for the passed in session object. It also populates the runs we know about
func (s *Session) FlowSession(cfg *runtime.Config, sa flows.SessionAssets, env envs.Environment) (flows.Session, error) {
	session, err := goflow.Engine(cfg).ReadSession(sa, json.RawMessage(s.s.Output), assets.IgnoreMissing)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal session")
	}

	// walk through our session, populate seen runs
	s.seenRuns = make(map[flows.RunUUID]time.Time, len(session.Runs()))
	for _, r := range session.Runs() {
		s.seenRuns[r.UUID()] = r.ModifiedOn()
	}

	return session, nil
}

// looks for a wait event and updates wait fields if one exists
func (s *Session) updateWait(evts []flows.Event) {
	canResume := func(r flows.Run) bool {
		// a session can be resumed on a wait expiration if there's a parent and it's a messaging flow
		return r.ParentInSession() != nil && r.Flow().Type() == flows.FlowTypeMessaging
	}

	s.s.WaitStartedOn = nil
	s.s.WaitTimeoutOn = nil
	s.s.WaitExpiresOn = nil
	s.s.WaitResumeOnExpire = false
	s.timeout = nil

	now := time.Now()

	for _, e := range evts {
		switch typed := e.(type) {
		case *events.MsgWaitEvent:
			run, _ := s.findStep(e.StepUUID())

			s.s.WaitStartedOn = &now
			s.s.WaitExpiresOn = typed.ExpiresOn
			s.s.WaitResumeOnExpire = canResume(run)

			if typed.TimeoutSeconds != nil {
				seconds := time.Duration(*typed.TimeoutSeconds) * time.Second
				timeoutOn := now.Add(seconds)

				s.s.WaitTimeoutOn = &timeoutOn
				s.timeout = &seconds
			}
		case *events.DialWaitEvent:
			run, _ := s.findStep(e.StepUUID())

			s.s.WaitStartedOn = &now
			s.s.WaitExpiresOn = typed.ExpiresOn
			s.s.WaitResumeOnExpire = canResume(run)
		}
	}
}

const sqlUpdateSession = `
UPDATE 
	flows_flowsession
SET 
	output = :output, 
	output_url = :output_url,
	status = :status, 
	ended_on = :ended_on,
	responded = :responded,
	current_flow_id = :current_flow_id,
	wait_started_on = :wait_started_on,
	wait_expires_on = :wait_expires_on,
	wait_resume_on_expire = :wait_resume_on_expire,
	timeout_on = :timeout_on
WHERE 
	id = :id
`

const sqlUpdateSessionNoOutput = `
UPDATE 
	flows_flowsession
SET 
	output_url = :output_url,
	status = :status, 
	ended_on = :ended_on,
	responded = :responded,
	current_flow_id = :current_flow_id,
	wait_started_on = :wait_started_on,
	wait_expires_on = :wait_expires_on,
	wait_resume_on_expire = :wait_resume_on_expire,
	timeout_on = :timeout_on
WHERE 
	id = :id
`

const sqlUpdateRun = `
UPDATE
	flows_flowrun fr
SET
	status = r.status,
	exited_on = r.exited_on::timestamp with time zone,
	responded = r.responded::bool,
	results = r.results,
	path = r.path::jsonb,
	current_node_uuid = r.current_node_uuid::uuid,
	modified_on = NOW()
FROM (
	VALUES(:uuid, :status, :exited_on, :responded, :results, :path, :current_node_uuid)
) AS
	r(uuid, status, exited_on, responded, results, path, current_node_uuid)
WHERE
	fr.uuid = r.uuid::uuid
`

// Update updates the session based on the state passed in from our engine session, this also takes care of applying any event hooks
func (s *Session) Update(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *OrgAssets, fs flows.Session, sprint flows.Sprint, contact *Contact, hook SessionCommitHook) error {
	// make sure we have our seen runs
	if s.seenRuns == nil {
		return errors.Errorf("missing seen runs, cannot update session")
	}

	output, err := json.Marshal(fs)
	if err != nil {
		return errors.Wrapf(err, "error marshalling flow session")
	}
	s.s.Output = null.String(output)

	// map our status over
	status, found := sessionStatusMap[fs.Status()]
	if !found {
		return errors.Errorf("unknown session status: %s", fs.Status())
	}
	s.s.Status = status

	if s.s.Status != SessionStatusWaiting {
		now := time.Now()
		s.s.EndedOn = &now
	}

	// now build up our runs
	for _, r := range fs.Runs() {
		run, err := newRun(ctx, tx, oa, s, r)
		if err != nil {
			return errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// set the run on our session
		s.runs = append(s.runs, run)
	}

	// set our sprint, wait and step finder
	s.sprint = sprint
	s.findStep = fs.FindStep
	s.s.CurrentFlowID = NilFlowID

	// update wait related fields
	s.updateWait(sprint.Events())

	// run through our runs to figure out our current flow
	for _, r := range fs.Runs() {
		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flowID, err := FlowIDForUUID(ctx, tx, oa, r.FlowReference().UUID)
			if err != nil {
				return errors.Wrapf(err, "error loading flow: %s", r.FlowReference().UUID)
			}
			s.s.CurrentFlowID = flowID
		}

		// if we haven't already been marked as responded, walk our runs looking for an input
		if !s.s.Responded {
			// run through events, see if any are received events
			for _, e := range r.Events() {
				if e.Type() == events.TypeMsgReceived {
					s.s.Responded = true
					break
				}
			}
		}
	}

	// apply all our pre write events
	for _, e := range sprint.Events() {
		err := ApplyPreWriteEvent(ctx, rt, tx, oa, s.scene, e)
		if err != nil {
			return errors.Wrapf(err, "error applying event: %v", e)
		}
	}

	// the SQL statement we'll use to update this session
	updateSQL := sqlUpdateSession

	// if writing to S3, do so
	if rt.Config.SessionStorage == "s3" {
		err := WriteSessionOutputsToStorage(ctx, rt, []*Session{s})
		if err != nil {
			slog.Error("error writing session to s3", "error", err)
		}

		// don't write output in our SQL
		updateSQL = sqlUpdateSessionNoOutput
	}

	// write our new session state to the db
	_, err = tx.NamedExecContext(ctx, updateSQL, s.s)
	if err != nil {
		return errors.Wrapf(err, "error updating session")
	}

	// if this session is complete, so is any associated connection
	if s.call != nil {
		if s.Status() == SessionStatusCompleted || s.Status() == SessionStatusFailed {
			err := s.call.UpdateStatus(ctx, tx, CallStatusCompleted, 0, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error update channel connection")
			}
		}
	}

	// figure out which runs are new and which are updated
	updatedRuns := make([]any, 0, 1)
	newRuns := make([]any, 0)
	for _, r := range s.Runs() {
		modified, found := s.seenRuns[r.UUID()]
		if !found {
			newRuns = append(newRuns, &r.r)
			continue
		}

		if r.ModifiedOn().After(modified) {
			updatedRuns = append(updatedRuns, &r.r)
			continue
		}
	}

	// call our global pre commit hook if present
	if hook != nil {
		err := hook(ctx, tx, rt.RP, oa, []*Session{s})
		if err != nil {
			return errors.Wrapf(err, "error calling commit hook: %v", hook)
		}
	}

	// update all modified runs at once
	err = BulkQuery(ctx, "update runs", tx, sqlUpdateRun, updatedRuns)
	if err != nil {
		slog.Error("error while updating runs for session", "error", err, "session", string(output))
		return errors.Wrapf(err, "error updating runs")
	}

	// insert all new runs at once
	err = BulkQuery(ctx, "insert runs", tx, sqlInsertRun, newRuns)
	if err != nil {
		return errors.Wrapf(err, "error writing runs")
	}

	if err := RecordFlowStatistics(ctx, rt, tx, []flows.Session{fs}, []flows.Sprint{sprint}); err != nil {
		return errors.Wrapf(err, "error saving flow statistics")
	}

	var eventsToHandle []flows.Event

	// if session didn't fail, we need to handle this sprint's events
	if s.Status() != SessionStatusFailed {
		eventsToHandle = append(eventsToHandle, sprint.Events()...)
	}

	eventsToHandle = append(eventsToHandle, NewSprintEndedEvent(contact, true))

	// apply all our events to generate hooks
	err = HandleEvents(ctx, rt, tx, oa, s.scene, eventsToHandle)
	if err != nil {
		return errors.Wrapf(err, "error applying events: %d", s.ID())
	}

	// gather all our pre commit events, group them by hook and apply them
	err = ApplyEventPreCommitHooks(ctx, rt, tx, oa, []*Scene{s.scene})
	if err != nil {
		return errors.Wrapf(err, "error applying pre commit hook: %T", hook)
	}

	return nil
}

// ClearWaitTimeout clears the timeout on the wait on this session and is used if the engine tells us
// that the flow no longer has a timeout on that wait. It can be called without updating the session
// in the database which is used when handling msg_created events before session is updated anyway.
func (s *Session) ClearWaitTimeout(ctx context.Context, db *sqlx.DB) error {
	s.s.WaitTimeoutOn = nil

	if db != nil {
		_, err := db.ExecContext(ctx, `UPDATE flows_flowsession SET timeout_on = NULL WHERE id = $1`, s.ID())
		return errors.Wrap(err, "error clearing wait timeout")
	}
	return nil
}

// MarshalJSON is our custom marshaller so that our inner struct get output
func (s *Session) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.s)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (s *Session) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &s.s)
}

// NewSession a session objects from the passed in flow session. It does NOT
// commit said session to the database.
func NewSession(ctx context.Context, tx *sqlx.Tx, oa *OrgAssets, fs flows.Session, sprint flows.Sprint) (*Session, error) {
	output, err := json.Marshal(fs)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling flow session")
	}

	// map our status over
	sessionStatus, found := sessionStatusMap[fs.Status()]
	if !found {
		return nil, errors.Errorf("unknown session status: %s", fs.Status())
	}

	// session must have at least one run
	if len(fs.Runs()) < 1 {
		return nil, errors.Errorf("cannot write session that has no runs")
	}

	// figure out our type
	sessionType, found := flowTypeMapping[fs.Type()]
	if !found {
		return nil, errors.Errorf("unknown flow type: %s", fs.Type())
	}

	uuid := fs.UUID()
	if uuid == "" {
		uuid = flows.SessionUUID(uuids.New())
	}

	// create our session object
	session := &Session{}
	s := &session.s
	s.UUID = uuid
	s.Status = sessionStatus
	s.SessionType = sessionType
	s.Responded = false
	s.Output = null.String(output)
	s.ContactID = ContactID(fs.Contact().ID())
	s.OrgID = oa.OrgID()
	s.CreatedOn = fs.Runs()[0].CreatedOn()

	if s.Status != SessionStatusWaiting {
		now := time.Now()
		s.EndedOn = &now
	}

	session.contact = fs.Contact()
	session.scene = NewSceneForSession(session)

	session.sprint = sprint
	session.findStep = fs.FindStep

	// now build up our runs
	for _, r := range fs.Runs() {
		run, err := newRun(ctx, tx, oa, session, r)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// save the run to our session
		session.runs = append(session.runs, run)

		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flowID, err := FlowIDForUUID(ctx, tx, oa, r.FlowReference().UUID)
			if err != nil {
				return nil, errors.Wrapf(err, "error loading current flow for UUID: %s", r.FlowReference().UUID)
			}
			s.CurrentFlowID = flowID
		}
	}

	// calculate our timeout if any
	session.updateWait(sprint.Events())

	return session, nil
}

const sqlInsertWaitingSession = `
INSERT INTO
	flows_flowsession( uuid,  session_type,  status,  responded,  output,  output_url,  contact_id,  org_id, created_on, current_flow_id,  timeout_on,  wait_started_on,  wait_expires_on,  wait_resume_on_expire,  call_id)
               VALUES(:uuid, :session_type, :status, :responded, :output, :output_url, :contact_id, :org_id, NOW(),     :current_flow_id, :timeout_on, :wait_started_on, :wait_expires_on, :wait_resume_on_expire, :call_id)
RETURNING id`

const sqlInsertWaitingSessionNoOutput = `
INSERT INTO
	flows_flowsession( uuid,  session_type,  status,  responded,           output_url,  contact_id,  org_id, created_on, current_flow_id,  timeout_on,  wait_started_on,  wait_expires_on,  wait_resume_on_expire,  call_id)
               VALUES(:uuid, :session_type, :status, :responded,          :output_url, :contact_id, :org_id, NOW(),     :current_flow_id, :timeout_on, :wait_started_on, :wait_expires_on, :wait_resume_on_expire, :call_id)
RETURNING id`

const sqlInsertEndedSession = `
INSERT INTO
	flows_flowsession( uuid,  session_type,  status,  responded,  output,  output_url,  contact_id,  org_id, created_on, ended_on, wait_resume_on_expire, call_id)
               VALUES(:uuid, :session_type, :status, :responded, :output, :output_url, :contact_id, :org_id, NOW(),      NOW(),    FALSE,                :call_id)
RETURNING id`

const sqlInsertEndedSessionNoOutput = `
INSERT INTO
	flows_flowsession( uuid,  session_type,  status,  responded,           output_url,  contact_id,  org_id, created_on, ended_on, wait_resume_on_expire, call_id)
               VALUES(:uuid, :session_type, :status, :responded,          :output_url, :contact_id, :org_id, NOW(),      NOW(),    FALSE,                :call_id)
RETURNING id`

// InsertSessions writes the passed in session to our database, writes any runs that need to be created
// as well as appying any events created in the session
func InsertSessions(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *OrgAssets, ss []flows.Session, sprints []flows.Sprint, contacts []*Contact, hook SessionCommitHook) ([]*Session, error) {
	if len(ss) == 0 {
		return nil, nil
	}

	// create all our session objects
	sessions := make([]*Session, 0, len(ss))
	waitingSessionsI := make([]any, 0, len(ss))
	endedSessionsI := make([]any, 0, len(ss))
	completedCallIDs := make([]CallID, 0, 1)

	for i, s := range ss {
		session, err := NewSession(ctx, tx, oa, s, sprints[i])
		if err != nil {
			return nil, errors.Wrapf(err, "error creating session objects")
		}
		sessions = append(sessions, session)

		if session.Status() == SessionStatusWaiting {
			waitingSessionsI = append(waitingSessionsI, &session.s)
		} else {
			endedSessionsI = append(endedSessionsI, &session.s)
			if session.call != nil {
				completedCallIDs = append(completedCallIDs, session.call.ID())
			}
		}
	}

	// apply all our pre write events
	for i := range ss {
		for _, e := range sprints[i].Events() {
			err := ApplyPreWriteEvent(ctx, rt, tx, oa, sessions[i].scene, e)
			if err != nil {
				return nil, errors.Wrapf(err, "error applying event: %v", e)
			}
		}
	}

	// call our global pre commit hook if present
	if hook != nil {
		err := hook(ctx, tx, rt.RP, oa, sessions)
		if err != nil {
			return nil, errors.Wrapf(err, "error calling commit hook: %v", hook)
		}
	}

	// the SQL we'll use to do our insert of sessions
	insertEndedSQL := sqlInsertEndedSession
	insertWaitingSQL := sqlInsertWaitingSession

	// if writing our sessions to S3, do so
	if rt.Config.SessionStorage == "s3" {
		err := WriteSessionOutputsToStorage(ctx, rt, sessions)
		if err != nil {
			return nil, errors.Wrapf(err, "error writing sessions to storage")
		}

		insertEndedSQL = sqlInsertEndedSessionNoOutput
		insertWaitingSQL = sqlInsertWaitingSessionNoOutput
	}

	// insert our ended sessions first
	err := BulkQuery(ctx, "insert ended sessions", tx, insertEndedSQL, endedSessionsI)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting ended sessions")
	}

	// mark any connections that are done as complete as well
	err = BulkUpdateCallStatuses(ctx, tx, completedCallIDs, CallStatusCompleted)
	if err != nil {
		return nil, errors.Wrapf(err, "error updating channel connections to complete")
	}

	// insert waiting sessions
	err = BulkQuery(ctx, "insert waiting sessions", tx, insertWaitingSQL, waitingSessionsI)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting waiting sessions")
	}

	// for each session associate our run with each
	runs := make([]any, 0, len(sessions))
	for _, s := range sessions {
		for _, r := range s.runs {
			runs = append(runs, &r.r)

			// set our session id now that it is written
			r.SetSessionID(s.ID())
		}
	}

	// insert all runs
	err = BulkQuery(ctx, "insert runs", tx, sqlInsertRun, runs)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing runs")
	}

	if err := RecordFlowStatistics(ctx, rt, tx, ss, sprints); err != nil {
		return nil, errors.Wrapf(err, "error saving flow statistics")
	}

	// apply our all events for the session
	scenes := make([]*Scene, 0, len(ss))
	for i, s := range sessions {
		var eventsToHandle []flows.Event

		// if session didn't fail, we need to handle this sprint's events
		if s.Status() != SessionStatusFailed {
			eventsToHandle = append(eventsToHandle, sprints[i].Events()...)
		}

		eventsToHandle = append(eventsToHandle, NewSprintEndedEvent(contacts[i], false))

		err = HandleEvents(ctx, rt, tx, oa, s.Scene(), eventsToHandle)
		if err != nil {
			return nil, errors.Wrapf(err, "error applying events for session: %d", s.ID())
		}

		scenes = append(scenes, s.Scene())
	}

	// gather all our pre commit events, group them by hook
	err = ApplyEventPreCommitHooks(ctx, rt, tx, oa, scenes)
	if err != nil {
		return nil, errors.Wrapf(err, "error applying pre commit hook: %T", hook)
	}

	// return our session
	return sessions, nil
}

const sqlSelectWaitingSessionForContact = `
SELECT 
	id,
	uuid,
	session_type,
	status,
	responded,
	output,
	output_url,
	contact_id,
	org_id,
	created_on,
	ended_on,
	timeout_on,
	wait_started_on,
	wait_expires_on,
	wait_resume_on_expire,
	current_flow_id,
	call_id
FROM 
	flows_flowsession fs
WHERE
    session_type = $1 AND
	contact_id = $2 AND
	status = 'W'
ORDER BY
	created_on DESC
LIMIT 1
`

// FindWaitingSessionForContact returns the waiting session for the passed in contact, if any
func FindWaitingSessionForContact(ctx context.Context, db *sqlx.DB, st storage.Storage, oa *OrgAssets, sessionType FlowType, contact *flows.Contact) (*Session, error) {
	rows, err := db.QueryxContext(ctx, sqlSelectWaitingSessionForContact, sessionType, contact.ID())
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting waiting session")
	}
	defer rows.Close()

	// no rows? no sessions!
	if !rows.Next() {
		return nil, nil
	}

	// scan in our session
	session := &Session{
		contact: contact,
	}
	session.scene = NewSceneForSession(session)

	if err := rows.StructScan(&session.s); err != nil {
		return nil, errors.Wrapf(err, "error scanning session")
	}

	// load our output from storage if necessary
	if session.OutputURL() != "" {
		// strip just the path out of our output URL
		u, err := url.Parse(session.OutputURL())
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing output URL: %s", session.OutputURL())
		}

		start := time.Now()

		_, output, err := st.Get(ctx, u.Path)
		if err != nil {
			return nil, errors.Wrapf(err, "error reading session from storage: %s", session.OutputURL())
		}

		slog.Debug("loaded session from storage", "elapsed", time.Since(start), "output_url", session.OutputURL())
		session.s.Output = null.String(output)
	}

	return session, nil
}

// WriteSessionsToStorage writes the outputs of the passed in sessions to our storage (S3), updating the
// output_url for each on success. Failure of any will cause all to fail.
func WriteSessionOutputsToStorage(ctx context.Context, rt *runtime.Runtime, sessions []*Session) error {
	start := time.Now()

	uploads := make([]*storage.Upload, len(sessions))
	for i, s := range sessions {
		uploads[i] = &storage.Upload{
			Path:        s.StoragePath(),
			Body:        []byte(s.Output()),
			ContentType: "application/json",
		}
	}

	err := rt.SessionStorage.BatchPut(ctx, uploads)
	if err != nil {
		return errors.Wrapf(err, "error writing sessions to storage")
	}

	for i, s := range sessions {
		s.s.OutputURL = null.String(uploads[i].URL)
	}

	slog.Debug("wrote sessions to s3", "elapsed", time.Since(start), "count", len(sessions))
	return nil
}

// FilterByWaitingSession takes contact ids and returns those who have waiting sessions
func FilterByWaitingSession(ctx context.Context, db *sqlx.DB, contacts []ContactID) ([]ContactID, error) {
	var overlap []ContactID
	err := db.SelectContext(ctx, &overlap, `SELECT DISTINCT(contact_id) FROM flows_flowsession WHERE status = 'W' AND contact_id = ANY($1)`, pq.Array(contacts))
	return overlap, err
}

// GetSessionWaitExpiresOn looks up the wait expiration for the passed in session and will return nil if the
// session is no longer waiting
func GetSessionWaitExpiresOn(ctx context.Context, db *sqlx.DB, sessionID SessionID) (*time.Time, error) {
	var expiresOn time.Time
	err := db.Get(&expiresOn, `SELECT wait_expires_on FROM flows_flowsession WHERE id = $1 AND status = 'W'`, sessionID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting wait_expires_on for session #%d", sessionID)
	}
	return &expiresOn, nil
}

// ExitSessions exits sessions and their runs. It batches the given session ids and exits each batch in a transaction.
func ExitSessions(ctx context.Context, db *sqlx.DB, sessionIDs []SessionID, status SessionStatus) error {
	if len(sessionIDs) == 0 {
		return nil
	}

	// split into batches and exit each batch in a transaction
	for _, idBatch := range ChunkSlice(sessionIDs, 100) {
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return errors.Wrapf(err, "error starting transaction to exit sessions")
		}

		if err := exitSessionBatch(ctx, tx, idBatch, status); err != nil {
			return errors.Wrapf(err, "error exiting batch of sessions")
		}

		if err := tx.Commit(); err != nil {
			return errors.Wrapf(err, "error committing session exits")
		}
	}

	return nil
}

const sqlExitSessions = `
   UPDATE flows_flowsession
      SET status = $3, ended_on = $2, wait_started_on = NULL, wait_expires_on = NULL, timeout_on = NULL, current_flow_id = NULL
    WHERE id = ANY ($1) AND status = 'W'
RETURNING contact_id`

const sqlExitSessionRuns = `
UPDATE flows_flowrun
   SET exited_on = $2, status = $3, modified_on = NOW()
 WHERE session_id = ANY($1) AND status IN ('A', 'W')`

const sqlExitSessionContacts = `
 UPDATE contacts_contact 
    SET current_flow_id = NULL, modified_on = NOW() 
  WHERE id = ANY($1)`

// exits sessions and their runs inside the given transaction
func exitSessionBatch(ctx context.Context, tx *sqlx.Tx, sessionIDs []SessionID, status SessionStatus) error {
	runStatus := RunStatus(status) // session status codes are subset of run status codes
	contactIDs := make([]SessionID, 0, len(sessionIDs))

	// first update the sessions themselves and get the contact ids
	start := time.Now()

	err := tx.SelectContext(ctx, &contactIDs, sqlExitSessions, pq.Array(sessionIDs), time.Now(), status)
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}

	slog.Debug("exited session batch", "count", len(contactIDs), "elapsed", time.Since(start))

	// then the runs that belong to these sessions
	start = time.Now()

	res, err := tx.ExecContext(ctx, sqlExitSessionRuns, pq.Array(sessionIDs), time.Now(), runStatus)
	if err != nil {
		return errors.Wrapf(err, "error exiting session runs")
	}

	rows, _ := res.RowsAffected()
	slog.Debug("exited session batch runs", "count", rows, "elapsed", time.Since(start))

	// and finally the contacts from each session
	start = time.Now()

	res, err = tx.ExecContext(ctx, sqlExitSessionContacts, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}

	rows, _ = res.RowsAffected()
	slog.Debug("exited session batch contacts", "count", rows, "elapsed", time.Since(start))

	return nil
}

func getWaitingSessionsForContacts(ctx context.Context, db DBorTx, contactIDs []ContactID) ([]SessionID, error) {
	sessionIDs := make([]SessionID, 0, len(contactIDs))

	err := db.SelectContext(ctx, &sessionIDs, `SELECT id FROM flows_flowsession WHERE status = 'W' AND contact_id = ANY($1)`, pq.Array(contactIDs))
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting waiting sessions for contacts")
	}

	return sessionIDs, nil
}

// InterruptSessionsForContacts interrupts any waiting sessions for the given contacts
func InterruptSessionsForContacts(ctx context.Context, db *sqlx.DB, contactIDs []ContactID) (int, error) {
	sessionIDs, err := getWaitingSessionsForContacts(ctx, db, contactIDs)
	if err != nil {
		return 0, err
	}

	return len(sessionIDs), errors.Wrapf(ExitSessions(ctx, db, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}

// InterruptSessionsForContactsTx interrupts any waiting sessions for the given contacts inside the given transaction.
// This version is used for interrupting during flow starts where contacts are already batched and we have an open transaction.
func InterruptSessionsForContactsTx(ctx context.Context, tx *sqlx.Tx, contactIDs []ContactID) error {
	sessionIDs, err := getWaitingSessionsForContacts(ctx, tx, contactIDs)
	if err != nil {
		return err
	}

	return errors.Wrapf(exitSessionBatch(ctx, tx, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}

const sqlWaitingSessionIDsForChannel = `
SELECT fs.id
  FROM flows_flowsession fs
  JOIN ivr_call cc ON fs.call_id = cc.id
 WHERE fs.status = 'W' AND cc.channel_id = $1;`

// InterruptSessionsForChannel interrupts any waiting sessions with calls on the given channel
func InterruptSessionsForChannel(ctx context.Context, db *sqlx.DB, channelID ChannelID) error {
	sessionIDs := make([]SessionID, 0, 10)

	err := db.SelectContext(ctx, &sessionIDs, sqlWaitingSessionIDsForChannel, channelID)
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for channel %d", channelID)
	}

	return errors.Wrapf(ExitSessions(ctx, db, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}

const sqlWaitingSessionIDsForFlows = `
SELECT id
  FROM flows_flowsession
 WHERE status = 'W' AND current_flow_id = ANY($1);`

// InterruptSessionsForFlows interrupts any waiting sessions currently in the given flows
func InterruptSessionsForFlows(ctx context.Context, db *sqlx.DB, flowIDs []FlowID) error {
	if len(flowIDs) == 0 {
		return nil
	}

	sessionIDs := make([]SessionID, 0, len(flowIDs))

	err := db.SelectContext(ctx, &sessionIDs, sqlWaitingSessionIDsForFlows, pq.Array(flowIDs))
	if err != nil {
		return errors.Wrapf(err, "error selecting waiting sessions for flows")
	}

	return errors.Wrapf(ExitSessions(ctx, db, sessionIDs, SessionStatusInterrupted), "error exiting sessions")
}
