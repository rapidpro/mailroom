package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/guregu/null.v3"
)

type SessionCommitHook func(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, []*Session) error

type SessionID int64
type SessionStatus string

type FlowRunID int64

const (
	SessionStatusActive    = "A"
	SessionStatusCompleted = "C"
	SessionStatusErrored   = "E"
	SessionStatusWaiting   = "W"
	SessionStatusExpired   = "X"
)

var sessionStatusMap = map[flows.SessionStatus]SessionStatus{
	flows.SessionStatusActive:    SessionStatusActive,
	flows.SessionStatusCompleted: SessionStatusCompleted,
	flows.SessionStatusErrored:   SessionStatusErrored,
	flows.SessionStatusWaiting:   SessionStatusWaiting,
}

type ExitType null.String

var (
	ExitInterrupted = null.NewString("I", true)
	ExitCompleted   = null.NewString("C", true)
	ExitExpired     = null.NewString("E", true)
)

var keptEvents = map[string]bool{
	events.TypeMsgCreated:  true,
	events.TypeMsgReceived: true,
}

// Session is the mailroom type for a FlowSession
type Session struct {
	ID            SessionID       `db:"id"`
	Status        SessionStatus   `db:"status"`
	Responded     bool            `db:"responded"`
	Output        string          `db:"output"`
	ContactID     flows.ContactID `db:"contact_id"`
	OrgID         OrgID           `db:"org_id"`
	CreatedOn     time.Time       `db:"created_on"`
	EndedOn       *time.Time      `db:"ended_on"`
	TimeoutOn     *time.Time      `db:"timeout_on"`
	CurrentFlowID *FlowID         `db:"current_flow_id"`

	IncomingMsgID      null.Int
	IncomingExternalID string

	contact *flows.Contact
	runs    []*FlowRun

	seenRuns    map[flows.RunUUID]time.Time
	preCommits  map[EventCommitHook][]interface{}
	postCommits map[EventCommitHook][]interface{}
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

// AddPreCommitEvent adds a new event to be handled by a pre commit hook
func (s *Session) AddPreCommitEvent(hook EventCommitHook, event interface{}) {
	s.preCommits[hook] = append(s.preCommits[hook], event)
}

// AddPostCommitEvent adds a new event to be handled by a post commit hook
func (s *Session) AddPostCommitEvent(hook EventCommitHook, event interface{}) {
	s.postCommits[hook] = append(s.postCommits[hook], event)
}

// SetIncomingMsg set the incoming message that this session should be associated with in this sprint
func (s *Session) SetIncomingMsg(id flows.MsgID, externalID string) {
	s.IncomingMsgID = null.NewInt(int64(id), true)
	s.IncomingExternalID = externalID
}

// FlowRun is the mailroom type for a FlowRun
type FlowRun struct {
	ID         FlowRunID     `db:"id"`
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
	Events string `db:"events"`

	CurrentNodeUUID flows.NodeUUID  `db:"current_node_uuid"`
	ContactID       flows.ContactID `db:"contact_id"`
	FlowID          FlowID          `db:"flow_id"`
	OrgID           OrgID           `db:"org_id"`
	ParentUUID      *flows.RunUUID  `db:"parent_uuid"`
	SessionID       SessionID       `db:"session_id"`
	StartID         StartID         `db:"start_id"`

	// we keep a reference to model run as well
	run flows.FlowRun
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
func NewSession(org *OrgAssets, s flows.Session) (*Session, error) {
	output, err := json.Marshal(s)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling flow session")
	}

	// map our status over
	sessionStatus, found := sessionStatusMap[s.Status()]
	if !found {
		return nil, errors.Errorf("unknown session status: %s", s.Status())
	}

	// session must have at least one run
	if len(s.Runs()) < 1 {
		return nil, errors.Errorf("cannot write session that has no runs")
	}

	// create our session object
	session := &Session{
		Status:    sessionStatus,
		Responded: false, // TODO: populate once we are running real flows
		Output:    string(output),
		ContactID: s.Contact().ID(),
		OrgID:     org.OrgID(),
		CreatedOn: s.Runs()[0].CreatedOn(),

		contact:     s.Contact(),
		preCommits:  make(map[EventCommitHook][]interface{}),
		postCommits: make(map[EventCommitHook][]interface{}),
	}

	// now build up our runs
	for _, r := range s.Runs() {
		run, err := newRun(org, session, r)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// save the run to our session
		session.runs = append(session.runs, run)

		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flow, err := org.Flow(r.Flow().UUID())
			if err != nil {
				return nil, errors.Wrapf(err, "error loading current flow")
			}
			flowID := flow.(*Flow).ID()
			session.CurrentFlowID = &flowID
		}
	}

	// set our timeout if we have a wait
	if s.Wait() != nil {
		session.TimeoutOn = s.Wait().TimeoutOn()
	}

	return session, nil
}

// ActiveSessionForContact returns the active session for the passed in contact, if any
func ActiveSessionForContact(ctx context.Context, db *sqlx.DB, org *OrgAssets, contact *flows.Contact) (*Session, error) {
	rows, err := db.QueryxContext(ctx, selectLastSessionSQL, contact.ID())
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
		contact:     contact,
		preCommits:  make(map[EventCommitHook][]interface{}),
		postCommits: make(map[EventCommitHook][]interface{}),
	}
	err = rows.StructScan(session)
	if err != nil {
		return nil, errors.Wrapf(err, "error scanning session")
	}

	return session, nil
}

const selectLastSessionSQL = `
SELECT 
	id, status, responded, output, contact_id, org_id, created_on, ended_on, current_flow_id 
FROM 
	flows_flowsession
WHERE
	contact_id = $1 AND
	status = 'W'
ORDER BY
	created_on DESC
LIMIT 1
`

const insertCompleteSessionSQL = `
INSERT INTO
	flows_flowsession(status, responded, output, contact_id, org_id, created_on, ended_on)
               VALUES(:status, :responded, :output, :contact_id, :org_id, NOW(), NOW())
RETURNING id
`

const insertIncompleteSessionSQL = `
INSERT INTO
	flows_flowsession(status, responded, output, contact_id, org_id, created_on, current_flow_id, timeout_on)
               VALUES(:status, :responded, :output, :contact_id, :org_id, NOW(), :current_flow_id, :timeout_on)
RETURNING id
`

// FlowSession creates a flow session for the passed in session object. It also populates the runs we know about
func (s *Session) FlowSession(sa flows.SessionAssets, env utils.Environment, client *utils.HTTPClient) (flows.Session, error) {
	session, err := engine.ReadSession(sa, engine.NewDefaultConfig(), client, json.RawMessage(s.Output))
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

// WriteUpdatedSession updates the session based on the state passed in from our engine session, this also takes care of applying any event hooks
func (s *Session) WriteUpdatedSession(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, fs flows.Session) error {
	// make sure we have our seen runs
	if s.seenRuns == nil {
		return errors.Errorf("missing seen runs, cannot update session")
	}

	output, err := json.Marshal(fs)
	if err != nil {
		return errors.Wrapf(err, "error marshalling flow session")
	}
	s.Output = string(output)

	// map our status over
	status, found := sessionStatusMap[fs.Status()]
	if !found {
		return errors.Errorf("unknown session status: %s", fs.Status())
	}
	s.Status = status

	// now build up our runs
	for _, r := range fs.Runs() {
		run, err := newRun(org, s, r)
		if err != nil {
			return errors.Wrapf(err, "error creating run: %s", r.UUID())
		}

		// set the run on our session
		s.runs = append(s.runs, run)
	}

	// set our timeout if there is one
	if fs.Wait() == nil {
		s.TimeoutOn = nil
	} else {
		s.TimeoutOn = fs.Wait().TimeoutOn()
	}

	// run through our runs to figure out our current flow
	for _, r := range fs.Runs() {
		// if this run is waiting, save it as the current flow
		if r.Status() == flows.RunStatusWaiting {
			flow, err := org.Flow(r.Flow().UUID())
			if err != nil {
				return errors.Wrapf(err, "error loading current flow")
			}
			flowID := flow.(*Flow).ID()
			s.CurrentFlowID = &flowID
		}

		// if we haven't already been marked as responded, walk our runs looking for an input
		if !s.Responded {
			// run through events, see if any are received events
			for _, e := range r.Events() {
				if e.Type() == events.TypeMsgReceived {
					s.Responded = true
					break
				}
			}
		}
	}

	// write our new session state to the db
	_, err = tx.NamedExecContext(ctx, updateSessionSQL, s)
	if err != nil {
		return errors.Wrapf(err, "error updating session")
	}

	// figure out which runs are new and which are updated
	updatedRuns := make([]interface{}, 0, 1)
	newRuns := make([]interface{}, 0)
	for _, r := range s.Runs() {
		modified, found := s.seenRuns[r.UUID]
		if !found {
			newRuns = append(newRuns, r)
			continue
		}

		if r.ModifiedOn.After(modified) {
			updatedRuns = append(updatedRuns, r)
			continue
		}
	}

	// update all modified runs at once
	err = BulkSQL(ctx, "update runs", tx, updateRunSQL, updatedRuns)
	if err != nil {
		return errors.Wrapf(err, "error updating runs")
	}

	// insert all new runs at once
	err = BulkSQL(ctx, "insert runs", tx, insertRunSQL, newRuns)
	if err != nil {
		return errors.Wrapf(err, "error writing runs")
	}

	// apply all our events
	for _, e := range fs.Events() {
		err := ApplyEvent(ctx, tx, rp, org, s, e)
		if err != nil {
			return errors.Wrapf(err, "error applying event: %v", e)
		}
	}

	// gather all our pre commit events, group them by hook and apply them
	err = ApplyPreEventHooks(ctx, tx, rp, org, []*Session{s})
	if err != nil {
		return errors.Wrapf(err, "error applying pre commit hooks")
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
	timeout_on = :timeout_on
WHERE 
	id = :id
`

const updateRunSQL = `
UPDATE
	flows_flowrun fr
SET
	is_active = r.is_active::bool,
	exit_type = r.exit_type,
	exited_on = r.exited_on::timestamp with time zone,
	expires_on = r.expires_on::timestamp with time zone,
	responded = r.responded::bool,
	results = r.results,
	path = r.path::jsonb,
	events = r.events::jsonb,
	current_node_uuid = r.current_node_uuid::uuid,
	modified_on = NOW()
FROM (
	VALUES(:uuid, :is_active, :exit_type, :exited_on, :expires_on, :responded, :results, :path, :events, :current_node_uuid)
) AS
	r(uuid, is_active, exit_type, exited_on, expires_on, responded, results, path, events, current_node_uuid)
WHERE
	fr.uuid = r.uuid::uuid
`

// WriteSessions writes the passed in session to our database, writes any runs that need to be created
// as well as appying any events created in the session
func WriteSessions(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, ss []flows.Session, hook SessionCommitHook) ([]*Session, error) {
	if len(ss) == 0 {
		return nil, nil
	}

	// create all our session objects
	sessions := make([]*Session, 0, len(ss))
	completeSessionsI := make([]interface{}, 0, len(ss))
	incompleteSessionsI := make([]interface{}, 0, len(ss))
	for _, s := range ss {
		session, err := NewSession(org, s)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating session objects")
		}
		sessions = append(sessions, session)

		if session.Status == SessionStatusCompleted {
			completeSessionsI = append(completeSessionsI, session)
		} else {
			incompleteSessionsI = append(incompleteSessionsI, session)
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

	// insert them all
	err = BulkSQL(ctx, "insert complete sessions", tx, insertIncompleteSessionSQL, incompleteSessionsI)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting incomplete sessions")
	}

	// for each session associate our run with each
	runs := make([]interface{}, 0, len(sessions))
	for _, s := range sessions {
		for _, r := range s.runs {
			runs = append(runs, r)

			// set our session id now that it is written
			r.SessionID = s.ID
		}
	}

	// insert all runs
	err = BulkSQL(ctx, "insert runs", tx, insertRunSQL, runs)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing runs")
	}

	// apply our all events for the session
	for i := range ss {
		for _, e := range ss[i].Events() {
			err := ApplyEvent(ctx, tx, rp, org, sessions[i], e)
			if err != nil {
				return nil, errors.Wrapf(err, "error applying event: %v", e)
			}
		}
	}

	// gather all our pre commit events, group them by hook
	err = ApplyPreEventHooks(ctx, tx, rp, org, sessions)
	if err != nil {
		return nil, errors.Wrapf(err, "error applying pre commit hooks")
	}

	// return our session
	return sessions, nil
}

const insertRunSQL = `
INSERT INTO
flows_flowrun(uuid, is_active, created_on, modified_on, exited_on, exit_type, expires_on, responded, results, path, 
	          events, current_node_uuid, contact_id, flow_id, org_id, session_id, start_id, parent_uuid)
	   VALUES(:uuid, :is_active, :created_on, NOW(), :exited_on, :exit_type, :expires_on, :responded, :results, :path,
	          :events, :current_node_uuid, :contact_id, :flow_id, :org_id, :session_id, :start_id, :parent_uuid)
RETURNING id
`

// newRun writes the passed in flow run to our database, also applying any events in those runs as
// appropriate. (IE, writing db messages etc..)
func newRun(org *OrgAssets, session *Session, r flows.FlowRun) (*FlowRun, error) {
	// no path is invalid
	if len(r.Path()) < 1 {
		return nil, errors.Errorf("run must have at least one path segment")
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

	flow, err := org.Flow(r.Flow().UUID())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load flow with uuid: %s", r.Flow().UUID())
	}

	// create our run
	run := &FlowRun{
		UUID:            r.UUID(),
		CreatedOn:       r.CreatedOn(),
		ExitedOn:        r.ExitedOn(),
		ExpiresOn:       r.ExpiresOn(),
		ModifiedOn:      r.ModifiedOn(),
		ContactID:       r.Contact().ID(),
		FlowID:          flow.(*Flow).ID(),
		SessionID:       session.ID,
		StartID:         NilStartID,
		OrgID:           org.OrgID(),
		Path:            string(pathJSON),
		CurrentNodeUUID: path[len(path)-1].NodeUUID,
		run:             r,
	}

	// set our exit type if we exited
	// TODO: audit exit types
	if r.Status() != flows.RunStatusActive && r.Status() != flows.RunStatusWaiting {
		if r.Status() == flows.RunStatusErrored {
			run.ExitType = ExitInterrupted
		} else {
			run.ExitType = ExitCompleted
		}
		run.IsActive = false
	} else {
		run.IsActive = true
	}

	// we filter which events we write to our events json right now
	filteredEvents := make([]flows.Event, 0)
	for _, e := range r.Events() {
		if keptEvents[e.Type()] {
			filteredEvents = append(filteredEvents, e)
		}

		// mark ourselves as responded if we received a message
		if e.Type() == events.TypeMsgReceived {
			run.Responded = true
		}
	}
	eventJSON, err := json.Marshal(filteredEvents)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling events for run: %s", run.UUID)
	}
	run.Events = string(eventJSON)

	// write our results out
	resultsJSON, err := json.Marshal(r.Results())
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling results for run: %s", run.UUID)
	}
	run.Results = string(resultsJSON)

	// set our parent UUID if we have a parent
	if r.Parent() != nil {
		uuid := r.Parent().UUID()
		run.ParentUUID = &uuid
	}

	return run, nil
}

// FindFlowStartedOverlap returns the list of contact ids which overlap with those passed in and which
// have been in the flow passed in.
func FindFlowStartedOverlap(ctx context.Context, db *sqlx.DB, flowID FlowID, contacts []flows.ContactID) ([]flows.ContactID, error) {
	var overlap []flows.ContactID
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

// FindActiveRunOverlap returns the list of contact ids which overlap with those passed in which are
// active in any other flows.
func FindActiveRunOverlap(ctx context.Context, db *sqlx.DB, contacts []flows.ContactID) ([]flows.ContactID, error) {
	var overlap []flows.ContactID
	err := db.SelectContext(ctx, &overlap, activeRunOverlapSQL, pq.Array(contacts))
	return overlap, err
}

// should hit perfect index flows_flowrun_contact_flow_created_on_id_idx
const activeRunOverlapSQL = `
SELECT
	DISTINCT(contact_id)
FROM
	flows_flowrun
WHERE
	contact_id = ANY($1) AND
	is_active = TRUE
`

// InterruptContactRuns interrupts all runs and sesions that exist for the passed in list of contacts
func InterruptContactRuns(ctx context.Context, tx *sqlx.Tx, contactIDs []flows.ContactID) error {
	if len(contactIDs) == 0 {
		return nil
	}

	// TODO: hangup calls here?

	// first interrupt our runs
	start := time.Now()
	res, err := tx.ExecContext(ctx, interruptContactRunsSQL, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error interrupting contact runs")
	}
	rows, _ := res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("interrupted runs")

	// then our sessions
	start = time.Now()
	res, err = tx.ExecContext(ctx, interruptContactSessionsSQL, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error interrupting contact sessions")
	}
	rows, _ = res.RowsAffected()
	logrus.WithField("count", rows).WithField("elapsed", time.Since(start)).Debug("interrupted sessions")

	return nil
}

const interruptContactRunsSQL = `
UPDATE
	flows_flowrun
SET
	is_active = FALSE,
	exited_on = NOW(),
	exit_type = 'I',
	modified_on = NOW(),
	child_context = NULL,
	parent_context = NULL
WHERE
	id = ANY (SELECT id FROM flows_flowrun WHERE contact_id = ANY($1) AND is_active = TRUE)
`

const interruptContactSessionsSQL = `
UPDATE
	flows_flowsession
SET
	status = 'I'
WHERE
	id = ANY (SELECT id FROM flows_flowsession WHERE contact_id = ANY($1) AND status = 'W')
`
