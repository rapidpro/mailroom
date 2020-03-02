package models

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/null"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type SessionCommitHook func(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, []*Session) error

type SessionID int64
type SessionStatus string

type FlowRunID int64

const NilFlowRunID = FlowRunID(0)

const (
	SessionStatusWaiting     = "W"
	SessionStatusCompleted   = "C"
	SessionStatusExpired     = "X"
	SessionStatusInterrupted = "I"
	SessionStatusFailed      = "F"
)

var sessionStatusMap = map[flows.SessionStatus]SessionStatus{
	flows.SessionStatusWaiting:   SessionStatusWaiting,
	flows.SessionStatusCompleted: SessionStatusCompleted,
	flows.SessionStatusFailed:    SessionStatusFailed,
}

type RunStatus string

const (
	RunStatusActive      = "A"
	RunStatusWaiting     = "W"
	RunStatusCompleted   = "C"
	RunStatusExpired     = "X"
	RunStatusInterrupted = "I"
	RunStatusFailed      = "F"
)

var runStatusMap = map[flows.RunStatus]RunStatus{
	flows.RunStatusActive:    RunStatusActive,
	flows.RunStatusWaiting:   RunStatusWaiting,
	flows.RunStatusCompleted: RunStatusCompleted,
	flows.RunStatusExpired:   RunStatusExpired,
	flows.RunStatusFailed:    RunStatusFailed,
}

type ExitType = null.String

var (
	ExitInterrupted = ExitType("I")
	ExitCompleted   = ExitType("C")
	ExitExpired     = ExitType("E")
	ExitFailed      = ExitType("F")
)

var exitToSessionStatusMap = map[ExitType]SessionStatus{
	ExitInterrupted: SessionStatusInterrupted,
	ExitCompleted:   SessionStatusCompleted,
	ExitExpired:     SessionStatusExpired,
}

var exitToRunStatusMap = map[ExitType]RunStatus{
	ExitInterrupted: RunStatusInterrupted,
	ExitCompleted:   RunStatusCompleted,
	ExitExpired:     RunStatusExpired,
}

var keptEvents = map[string]bool{
	events.TypeMsgCreated:  true,
	events.TypeMsgReceived: true,
}

// Session is the mailroom type for a FlowSession
type Session struct {
	s struct {
		ID            SessionID         `db:"id"`
		UUID          flows.SessionUUID `db:"uuid"`
		SessionType   FlowType          `db:"session_type"`
		Status        SessionStatus     `db:"status"`
		Responded     bool              `db:"responded"`
		Output        string            `db:"output"`
		ContactID     ContactID         `db:"contact_id"`
		OrgID         OrgID             `db:"org_id"`
		CreatedOn     time.Time         `db:"created_on"`
		EndedOn       *time.Time        `db:"ended_on"`
		TimeoutOn     *time.Time        `db:"timeout_on"`
		WaitStartedOn *time.Time        `db:"wait_started_on"`
		CurrentFlowID FlowID            `db:"current_flow_id"`
		ConnectionID  *ConnectionID     `db:"connection_id"`
	}

	incomingMsgID      MsgID
	incomingExternalID null.String

	// any channel connection associated with this flow session
	channelConnection *ChannelConnection

	// time after our last message is sent that we should timeout
	timeout *time.Duration

	contact *flows.Contact
	runs    []*FlowRun

	seenRuns map[flows.RunUUID]time.Time

	// we keep around a reference to the sprint associated with this session
	sprint flows.Sprint

	// the scene for our event hooks
	scene *Scene

	// we also keep around a reference to the wait (if any)
	wait flows.ActivatedWait
}

func (s *Session) ID() SessionID                      { return s.s.ID }
func (s *Session) UUID() flows.SessionUUID            { return flows.SessionUUID(s.s.UUID) }
func (s *Session) SessionType() FlowType              { return s.s.SessionType }
func (s *Session) Status() SessionStatus              { return s.s.Status }
func (s *Session) Responded() bool                    { return s.s.Responded }
func (s *Session) Output() string                     { return s.s.Output }
func (s *Session) ContactID() ContactID               { return s.s.ContactID }
func (s *Session) OrgID() OrgID                       { return s.s.OrgID }
func (s *Session) CreatedOn() time.Time               { return s.s.CreatedOn }
func (s *Session) EndedOn() *time.Time                { return s.s.EndedOn }
func (s *Session) TimeoutOn() *time.Time              { return s.s.TimeoutOn }
func (s *Session) ClearTimeoutOn()                    { s.s.TimeoutOn = nil }
func (s *Session) WaitStartedOn() *time.Time          { return s.s.WaitStartedOn }
func (s *Session) CurrentFlowID() FlowID              { return s.s.CurrentFlowID }
func (s *Session) ConnectionID() *ConnectionID        { return s.s.ConnectionID }
func (s *Session) IncomingMsgID() MsgID               { return s.incomingMsgID }
func (s *Session) IncomingMsgExternalID() null.String { return s.incomingExternalID }
func (s *Session) Scene() *Scene                      { return s.scene }

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

// Wait returns the wait associated with this session (if any)
func (s *Session) Wait() flows.ActivatedWait {
	return s.wait
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
func (s *Session) SetIncomingMsg(id flows.MsgID, externalID null.String) {
	s.incomingMsgID = MsgID(id)
	s.incomingExternalID = externalID
}

// SetChannelConnection sets the channel connection associated with this sprint
func (s *Session) SetChannelConnection(cc *ChannelConnection) {
	connID := cc.ID()
	s.s.ConnectionID = &connID
	s.channelConnection = cc

	// also set it on all our runs
	for _, r := range s.runs {
		r.SetConnectionID(&connID)
	}
}

func (s *Session) ChannelConnection() *ChannelConnection {
	return s.channelConnection
}

// MarshalJSON is our custom marshaller so that our inner struct get output
func (s *Session) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.s)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (s *Session) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &s.s)
}

// FlowRun is the mailroom type for a FlowRun
type FlowRun struct {
	r struct {
		ID         FlowRunID     `db:"id"`
		UUID       flows.RunUUID `db:"uuid"`
		Status     RunStatus     `db:"status"`
		IsActive   bool          `db:"is_active"`
		CreatedOn  time.Time     `db:"created_on"`
		ModifiedOn time.Time     `db:"modified_on"`
		ExitedOn   *time.Time    `db:"exited_on"`
		ExitType   ExitType      `db:"exit_type"`
		ExpiresOn  *time.Time    `db:"expires_on"`
		Responded  bool          `db:"responded"`

		// TODO: should this be a complex object that can read / write iself to the DB as JSON?
		Results string `db:"results"`

		// TODO: should this be a complex object that can read / write iself to the DB as JSON?
		Path string `db:"path"`

		// TODO: should this be a complex object that can read / write iself to the DB as JSON?
		Events string `db:"events"`

		CurrentNodeUUID null.String     `db:"current_node_uuid"`
		ContactID       flows.ContactID `db:"contact_id"`
		FlowID          FlowID          `db:"flow_id"`
		OrgID           OrgID           `db:"org_id"`
		ParentUUID      *flows.RunUUID  `db:"parent_uuid"`
		SessionID       SessionID       `db:"session_id"`
		StartID         StartID         `db:"start_id"`
		ConnectionID    *ConnectionID   `db:"connection_id"`
	}

	// we keep a reference to model run as well
	run flows.FlowRun
}

func (r *FlowRun) SetSessionID(sessionID SessionID)     { r.r.SessionID = sessionID }
func (r *FlowRun) SetConnectionID(connID *ConnectionID) { r.r.ConnectionID = connID }
func (r *FlowRun) SetStartID(startID StartID)           { r.r.StartID = startID }
func (r *FlowRun) UUID() flows.RunUUID                  { return r.r.UUID }
func (r *FlowRun) ModifiedOn() time.Time                { return r.r.ModifiedOn }

// MarshalJSON is our custom marshaller so that our inner struct get output
func (r *FlowRun) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.r)
}

// UnmarshalJSON is our custom marshaller so that our inner struct get output
func (r *FlowRun) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &r.r)
}

// Step represents a single step in a run, this struct is used for serialization to the steps
type Step struct {
	UUID      flows.StepUUID `json:"uuid"`
	NodeUUID  flows.NodeUUID `json:"node_uuid"`
	ArrivedOn time.Time      `json:"arrived_on"`
	ExitUUID  flows.ExitUUID `json:"exit_uuid,omitempty"`
}

// NewSession a session objects from the passed in flow session. It does NOT
// commit said session to the database.
func NewSession(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, fs flows.Session, sprint flows.Sprint) (*Session, error) {
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
	sessionType, found := FlowTypeMapping[fs.Type()]
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
	s.Output = string(output)
	s.ContactID = ContactID(fs.Contact().ID())
	s.OrgID = org.OrgID()
	s.CreatedOn = fs.Runs()[0].CreatedOn()

	session.contact = fs.Contact()
	session.scene = NewSceneForSession(session)

	session.sprint = sprint
	session.wait = fs.Wait()

	// now build up our runs
	for _, r := range fs.Runs() {
		run, err := newRun(ctx, tx, org, session, r)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// save the run to our session
		session.runs = append(session.runs, run)

		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flowID, err := flowIDForUUID(ctx, tx, org, r.FlowReference().UUID)
			if err != nil {
				return nil, errors.Wrapf(err, "error loading current flow for UUID: %s", r.FlowReference().UUID)
			}
			s.CurrentFlowID = flowID
		}
	}

	// calculate our timeout if any
	session.calculateTimeout(fs, sprint)

	return session, nil
}

// ActiveSessionForContact returns the active session for the passed in contact, if any
func ActiveSessionForContact(ctx context.Context, db *sqlx.DB, org *OrgAssets, sessionType FlowType, contact *flows.Contact) (*Session, error) {
	rows, err := db.QueryxContext(ctx, selectLastSessionSQL, sessionType, contact.ID())
	if err != nil {
		return nil, errors.Wrapf(err, "error selecting active session")
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
	err = rows.StructScan(&session.s)
	if err != nil {
		return nil, errors.Wrapf(err, "error scanning session")
	}

	return session, nil
}

const selectLastSessionSQL = `
SELECT 
	id,
	uuid,
	session_type,
	status,
	responded,
	output,
	contact_id,
	org_id,
	created_on,
	ended_on,
	timeout_on,
	wait_started_on,
	current_flow_id,
	connection_id
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

const insertCompleteSessionSQL = `
INSERT INTO
	flows_flowsession( uuid, session_type, status, responded, output, contact_id, org_id, created_on, ended_on, wait_started_on, connection_id)
               VALUES(:uuid,:session_type,:status,:responded,:output,:contact_id,:org_id, NOW(),      NOW(),    NULL,           :connection_id)
RETURNING id
`

const insertIncompleteSessionSQL = `
INSERT INTO
	flows_flowsession( uuid, session_type, status, responded, output, contact_id, org_id, created_on, current_flow_id, timeout_on, wait_started_on, connection_id)
               VALUES(:uuid,:session_type,:status,:responded,:output,:contact_id,:org_id, NOW(),     :current_flow_id,:timeout_on,:wait_started_on,:connection_id)
RETURNING id
`

// FlowSession creates a flow session for the passed in session object. It also populates the runs we know about
func (s *Session) FlowSession(sa flows.SessionAssets, env envs.Environment) (flows.Session, error) {
	session, err := goflow.Engine().ReadSession(sa, json.RawMessage(s.s.Output), assets.IgnoreMissing)
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

// calculates the timeout value for this session based on our waits
func (s *Session) calculateTimeout(fs flows.Session, sprint flows.Sprint) {
	// if we are on a wait and it has a timeout
	if fs.Wait() != nil && fs.Wait().TimeoutSeconds() != nil {
		now := time.Now()
		s.s.WaitStartedOn = &now

		seconds := time.Duration(*fs.Wait().TimeoutSeconds()) * time.Second
		s.timeout = &seconds

		timeoutOn := now.Add(seconds)
		s.s.TimeoutOn = &timeoutOn
	} else {
		s.s.WaitStartedOn = nil
		s.s.TimeoutOn = nil
		s.timeout = nil
	}
}

// WriteUpdatedSession updates the session based on the state passed in from our engine session, this also takes care of applying any event hooks
func (s *Session) WriteUpdatedSession(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, fs flows.Session, sprint flows.Sprint, hook SessionCommitHook) error {
	// make sure we have our seen runs
	if s.seenRuns == nil {
		return errors.Errorf("missing seen runs, cannot update session")
	}

	output, err := json.Marshal(fs)
	if err != nil {
		return errors.Wrapf(err, "error marshalling flow session")
	}
	s.s.Output = string(output)

	// map our status over
	status, found := sessionStatusMap[fs.Status()]
	if !found {
		return errors.Errorf("unknown session status: %s", fs.Status())
	}
	s.s.Status = status

	// now build up our runs
	for _, r := range fs.Runs() {
		run, err := newRun(ctx, tx, org, s, r)
		if err != nil {
			return errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// set the run on our session
		s.runs = append(s.runs, run)
	}

	// calculate our new timeout
	s.calculateTimeout(fs, sprint)

	// set our sprint and wait
	s.sprint = sprint
	s.wait = fs.Wait()

	// run through our runs to figure out our current flow
	for _, r := range fs.Runs() {
		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flowID, err := flowIDForUUID(ctx, tx, org, r.FlowReference().UUID)
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
		err := ApplyPreWriteEvent(ctx, tx, rp, org, s.scene, e)
		if err != nil {
			return errors.Wrapf(err, "error applying event: %v", e)
		}
	}

	// write our new session state to the db
	_, err = tx.NamedExecContext(ctx, updateSessionSQL, s.s)
	if err != nil {
		return errors.Wrapf(err, "error updating session")
	}

	// if this session is complete, so is any associated connection
	if s.channelConnection != nil {
		if s.Status() == SessionStatusCompleted || s.Status() == SessionStatusFailed {
			err := s.channelConnection.UpdateStatus(ctx, tx, ConnectionStatusCompleted, 0, time.Now())
			if err != nil {
				return errors.Wrapf(err, "error update channel connection")
			}
		}
	}

	// figure out which runs are new and which are updated
	updatedRuns := make([]interface{}, 0, 1)
	newRuns := make([]interface{}, 0)
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
		err := hook(ctx, tx, rp, org, []*Session{s})
		if err != nil {
			return errors.Wrapf(err, "error calling commit hook: %v", hook)
		}
	}

	// update all modified runs at once
	err = BulkSQL(ctx, "update runs", tx, updateRunSQL, updatedRuns)
	if err != nil {
		logrus.WithError(err).WithField("session", string(output)).Error("error while updating runs for session")
		return errors.Wrapf(err, "error updating runs")
	}

	// insert all new runs at once
	err = BulkSQL(ctx, "insert runs", tx, insertRunSQL, newRuns)
	if err != nil {
		return errors.Wrapf(err, "error writing runs")
	}

	// apply all our events
	if s.Status() != SessionStatusFailed {
		err = HandleEvents(ctx, tx, rp, org, s.scene, sprint.Events())
		if err != nil {
			return errors.Wrapf(err, "error applying events: %d", s.ID())
		}
	}

	// gather all our pre commit events, group them by hook and apply them
	err = ApplyEventPreCommitHooks(ctx, tx, rp, org, []*Scene{s.scene})
	if err != nil {
		return errors.Wrapf(err, "error applying pre commit hook: %T", hook)
	}

	return nil
}

const updateSessionSQL = `
UPDATE 
	flows_flowsession
SET 
	output = :output, 
	status = :status, 
	ended_on = CASE WHEN :status = 'W' THEN NULL ELSE NOW() END,
	responded = :responded,
	current_flow_id = :current_flow_id,
	timeout_on = :timeout_on,
	wait_started_on = :wait_started_on
WHERE 
	id = :id
`

const updateRunSQL = `
UPDATE
	flows_flowrun fr
SET
	is_active = r.is_active::bool,
	exit_type = r.exit_type,
	status = r.status,
	exited_on = r.exited_on::timestamp with time zone,
	expires_on = r.expires_on::timestamp with time zone,
	responded = r.responded::bool,
	results = r.results,
	path = r.path::jsonb,
	events = r.events::jsonb,
	current_node_uuid = r.current_node_uuid::uuid,
	modified_on = NOW()
FROM (
	VALUES(:uuid, :is_active, :exit_type, :status, :exited_on, :expires_on, :responded, :results, :path, :events, :current_node_uuid)
) AS
	r(uuid, is_active, exit_type, status, exited_on, expires_on, responded, results, path, events, current_node_uuid)
WHERE
	fr.uuid = r.uuid::uuid
`

// WriteSessions writes the passed in session to our database, writes any runs that need to be created
// as well as appying any events created in the session
func WriteSessions(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, ss []flows.Session, sprints []flows.Sprint, hook SessionCommitHook) ([]*Session, error) {
	if len(ss) == 0 {
		return nil, nil
	}

	// create all our session objects
	sessions := make([]*Session, 0, len(ss))
	completeSessionsI := make([]interface{}, 0, len(ss))
	incompleteSessionsI := make([]interface{}, 0, len(ss))
	completedConnectionIDs := make([]ConnectionID, 0, 1)
	for i, s := range ss {
		session, err := NewSession(ctx, tx, org, s, sprints[i])
		if err != nil {
			return nil, errors.Wrapf(err, "error creating session objects")
		}
		sessions = append(sessions, session)

		if session.Status() == SessionStatusCompleted {
			completeSessionsI = append(completeSessionsI, &session.s)
			if session.channelConnection != nil {
				completedConnectionIDs = append(completedConnectionIDs, session.channelConnection.ID())
			}
		} else {
			incompleteSessionsI = append(incompleteSessionsI, &session.s)
		}
	}

	// apply all our pre write events
	for i := range ss {
		for _, e := range sprints[i].Events() {
			err := ApplyPreWriteEvent(ctx, tx, rp, org, sessions[i].scene, e)
			if err != nil {
				return nil, errors.Wrapf(err, "error applying event: %v", e)
			}
		}
	}

	// call our global pre commit hook if present
	if hook != nil {
		err := hook(ctx, tx, rp, org, sessions)
		if err != nil {
			return nil, errors.Wrapf(err, "error calling commit hook: %v", hook)
		}
	}

	// insert our complete sessions first
	err := BulkSQL(ctx, "insert completed sessions", tx, insertCompleteSessionSQL, completeSessionsI)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting completed sessions")
	}

	// mark any connections that are done as complete as well
	err = UpdateChannelConnectionStatuses(ctx, tx, completedConnectionIDs, ConnectionStatusCompleted)
	if err != nil {
		return nil, errors.Wrapf(err, "error updating channel connections to complete")
	}

	// insert incomplete sessions
	err = BulkSQL(ctx, "insert incomplete sessions", tx, insertIncompleteSessionSQL, incompleteSessionsI)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting incomplete sessions")
	}

	// for each session associate our run with each
	runs := make([]interface{}, 0, len(sessions))
	for _, s := range sessions {
		for _, r := range s.runs {
			runs = append(runs, &r.r)

			// set our session id now that it is written
			r.SetSessionID(s.ID())
		}
	}

	// insert all runs
	err = BulkSQL(ctx, "insert runs", tx, insertRunSQL, runs)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing runs")
	}

	// apply our all events for the session
	scenes := make([]*Scene, 0, len(ss))
	for i := range sessions {
		if ss[i].Status() == SessionStatusFailed {
			continue
		}

		err = HandleEvents(ctx, tx, rp, org, sessions[i].Scene(), sprints[i].Events())
		if err != nil {
			return nil, errors.Wrapf(err, "error applying events for session: %d", sessions[i].ID())
		}

		scene := sessions[i].Scene()
		scenes = append(scenes, scene)
	}

	// gather all our pre commit events, group them by hook
	err = ApplyEventPreCommitHooks(ctx, tx, rp, org, scenes)
	if err != nil {
		return nil, errors.Wrapf(err, "error applying pre commit hook: %T", hook)
	}

	// return our session
	return sessions, nil
}

const insertRunSQL = `
INSERT INTO
flows_flowrun(uuid, is_active, created_on, modified_on, exited_on, exit_type, status, expires_on, responded, results, path, 
	          events, current_node_uuid, contact_id, flow_id, org_id, session_id, start_id, parent_uuid, connection_id)
	   VALUES(:uuid, :is_active, :created_on, NOW(), :exited_on, :exit_type, :status, :expires_on, :responded, :results, :path,
	          :events, :current_node_uuid, :contact_id, :flow_id, :org_id, :session_id, :start_id, :parent_uuid, :connection_id)
RETURNING id
`

// newRun writes the passed in flow run to our database, also applying any events in those runs as
// appropriate. (IE, writing db messages etc..)
func newRun(ctx context.Context, tx *sqlx.Tx, org *OrgAssets, session *Session, fr flows.FlowRun) (*FlowRun, error) {
	// build our path elements
	path := make([]Step, len(fr.Path()))
	for i, p := range fr.Path() {
		path[i].UUID = p.UUID()
		path[i].NodeUUID = p.NodeUUID()
		path[i].ArrivedOn = p.ArrivedOn()
		path[i].ExitUUID = p.ExitUUID()
	}
	pathJSON, err := json.Marshal(path)
	if err != nil {
		return nil, err
	}

	flowID, err := flowIDForUUID(ctx, tx, org, fr.FlowReference().UUID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load flow with uuid: %s", fr.FlowReference().UUID)
	}

	// create our run
	run := &FlowRun{}
	r := &run.r
	r.UUID = fr.UUID()
	r.Status = runStatusMap[fr.Status()]
	r.CreatedOn = fr.CreatedOn()
	r.ExitedOn = fr.ExitedOn()
	r.ExpiresOn = fr.ExpiresOn()
	r.ModifiedOn = fr.ModifiedOn()
	r.ContactID = fr.Contact().ID()
	r.FlowID = flowID
	r.SessionID = session.ID()
	r.StartID = NilStartID
	r.OrgID = org.OrgID()
	r.Path = string(pathJSON)
	if len(path) > 0 {
		r.CurrentNodeUUID = null.String(path[len(path)-1].NodeUUID)
	}
	run.run = fr

	// set our exit type if we exited
	// TODO: audit exit types
	if fr.Status() != flows.RunStatusActive && fr.Status() != flows.RunStatusWaiting {
		if fr.Status() == flows.RunStatusFailed {
			r.ExitType = ExitInterrupted
		} else {
			r.ExitType = ExitCompleted
		}
		r.IsActive = false
	} else {
		r.IsActive = true
	}

	// we filter which events we write to our events json right now
	filteredEvents := make([]flows.Event, 0)
	for _, e := range fr.Events() {
		if keptEvents[e.Type()] {
			filteredEvents = append(filteredEvents, e)
		}

		// mark ourselves as responded if we received a message
		if e.Type() == events.TypeMsgReceived {
			r.Responded = true
		}
	}
	eventJSON, err := json.Marshal(filteredEvents)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling events for run: %s", run.UUID())
	}
	r.Events = string(eventJSON)

	// write our results out
	resultsJSON, err := json.Marshal(fr.Results())
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling results for run: %s", run.UUID())
	}
	r.Results = string(resultsJSON)

	// set our parent UUID if we have a parent
	if fr.Parent() != nil {
		uuid := fr.Parent().UUID()
		r.ParentUUID = &uuid
	}

	return run, nil
}

// FindFlowStartedOverlap returns the list of contact ids which overlap with those passed in and which
// have been in the flow passed in.
func FindFlowStartedOverlap(ctx context.Context, db *sqlx.DB, flowID FlowID, contacts []ContactID) ([]ContactID, error) {
	var overlap []ContactID
	err := db.SelectContext(ctx, &overlap, flowStartedOverlapSQL, pq.Array(contacts), flowID)
	return overlap, err
}

// TODO: no perfect index, will probably use contact index flows_flowrun_contact_id_985792a9
// could be slow in the cases of contacts having many distinct runs
const flowStartedOverlapSQL = `
SELECT
	DISTINCT(contact_id)
FROM
	flows_flowrun
WHERE
	contact_id = ANY($1) AND
	flow_id = $2
`

// FindActiveSessionOverlap returns the list of contact ids which overlap with those passed in which are active in any other flows
func FindActiveSessionOverlap(ctx context.Context, db *sqlx.DB, flowType FlowType, contacts []ContactID) ([]ContactID, error) {
	var overlap []ContactID
	err := db.SelectContext(ctx, &overlap, activeSessionOverlapSQL, flowType, pq.Array(contacts))
	return overlap, err
}

const activeSessionOverlapSQL = `
SELECT
	DISTINCT(contact_id)
FROM
	flows_flowsession fs JOIN
	flows_flow ff ON fs.current_flow_id = ff.id
WHERE
	fs.status = 'W' AND
	ff.is_active = TRUE AND
	ff.is_archived = FALSE AND
	ff.flow_type = $1 AND
	fs.contact_id = ANY($2)
`

// RunExpiration looks up the run expiration for the passed in run, can return nil if the run is no longer active
func RunExpiration(ctx context.Context, db *sqlx.DB, runID FlowRunID) (*time.Time, error) {
	var expiration time.Time
	err := db.Get(&expiration, `SELECT expires_on FROM flows_flowrun WHERE id = $1 AND is_active = TRUE`, runID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "unable to select expiration for run: %d", runID)
	}
	return &expiration, nil
}

// ExitSessions marks the passed in sessions as completed, also doing so for all associated runs
func ExitSessions(ctx context.Context, tx Queryer, sessionIDs []SessionID, exitType ExitType, now time.Time) error {
	if len(sessionIDs) == 0 {
		return nil
	}

	// map exit type to statuses for sessions and runs
	sessionStatus := exitToSessionStatusMap[exitType]
	runStatus, found := exitToRunStatusMap[exitType]
	if !found {
		return errors.Errorf("unknown exit type: %s", exitType)
	}

	// first interrupt our runs
	start := time.Now()
	res, err := tx.ExecContext(ctx, exitSessionRunsSQL, pq.Array(sessionIDs), exitType, now, runStatus)
	if err != nil {
		return errors.Wrapf(err, "error exiting session runs")
	}
	rows, _ := res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("exited session runs")

	// then our sessions
	start = time.Now()

	res, err = tx.ExecContext(ctx, exitSessionsSQL, pq.Array(sessionIDs), now, sessionStatus)
	if err != nil {
		return errors.Wrapf(err, "error exiting sessions")
	}
	rows, _ = res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("exited sessions")

	return nil
}

const exitSessionRunsSQL = `
UPDATE
	flows_flowrun
SET
	is_active = FALSE,
	exit_type = $2,
	exited_on = $3,
	status = $4,
	timeout_on = NULL,
	modified_on = NOW()
WHERE
	id = ANY (SELECT id FROM flows_flowrun WHERE session_id = ANY($1) AND is_active = TRUE)
`

const exitSessionsSQL = `
UPDATE
	flows_flowsession
SET
	ended_on = $2,
	status = $3
WHERE
	id = ANY ($1)
`

// InterruptContactRuns interrupts all runs and sesions that exist for the passed in list of contacts
func InterruptContactRuns(ctx context.Context, tx Queryer, sessionType FlowType, contactIDs []flows.ContactID, now time.Time) error {
	if len(contactIDs) == 0 {
		return nil
	}

	// first interrupt our runs
	err := Exec(ctx, "interrupting contact runs", tx, interruptContactRunsSQL, sessionType, pq.Array(contactIDs), now)
	if err != nil {
		return err
	}

	err = Exec(ctx, "interrupting contact sessions", tx, interruptContactSessionsSQL, sessionType, pq.Array(contactIDs), now)
	if err != nil {
		return err
	}

	return nil
}

const interruptContactRunsSQL = `
UPDATE
	flows_flowrun
SET
	is_active = FALSE,
	exited_on = $3,
	exit_type = 'I',
	status = 'I',
	modified_on = NOW()
WHERE
	id = ANY (
		SELECT 
		  fr.id 
		FROM 
		  flows_flowrun fr
		  JOIN flows_flow ff ON fr.flow_id = ff.id
		WHERE 
		  fr.contact_id = ANY($2) AND 
		  fr.is_active = TRUE AND
		  ff.flow_type = $1
		)
`

const interruptContactSessionsSQL = `
UPDATE
	flows_flowsession
SET
	status = 'I',
	ended_on = $3
WHERE
	id = ANY (SELECT id FROM flows_flowsession WHERE session_type = $1 AND contact_id = ANY($2) AND status = 'W')
`

// ExpireRunsAndSessions expires all the passed in runs and sessions. Note this should only be called
// for runs that have no parents or no way of continuing
func ExpireRunsAndSessions(ctx context.Context, db *sqlx.DB, runIDs []FlowRunID, sessionIDs []SessionID) error {
	if len(runIDs) == 0 {
		return nil
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "error starting transaction to expire sessions")
	}

	err = Exec(ctx, "expiring runs", tx, expireRunsSQL, pq.Array(runIDs))
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error expiring runs")
	}

	err = Exec(ctx, "expiring sessions", tx, expireSessionsSQL, pq.Array(sessionIDs))
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error expiring sessions")
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrapf(err, "error committing expiration of runs and sessions")
	}
	return nil
}

const expireSessionsSQL = `
	UPDATE
		flows_flowsession s
	SET
		timeout_on = NULL,
		ended_on = NOW(),
		status = 'X'
	WHERE
		id = ANY($1)
`

const expireRunsSQL = `
	UPDATE
		flows_flowrun fr
	SET
		is_active = FALSE,
		exited_on = NOW(),
		exit_type = 'E',
		status = 'E',
		modified_on = NOW()
	WHERE
		id = ANY($1)
`
