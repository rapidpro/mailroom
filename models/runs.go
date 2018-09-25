package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"gopkg.in/guregu/null.v3"
)

type SessionID int64
type SessionStatus string

const (
	SessionStatusActive    = "A"
	SessionStatusCompleted = "C"
	SessionStatusErrored   = "E"
	SessionStatusWaiting   = "W"
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
	ID        SessionID       `db:"id"`
	Status    SessionStatus   `db:"status"`
	Responded bool            `db:"responded"`
	Output    string          `db:"output"`
	ContactID flows.ContactID `db:"contact_id"`
	OrgID     OrgID           `db:"org_id"`
	CreatedOn time.Time

	org       *OrgAssets
	contactID ContactID
	contact   *flows.Contact
	runs      []*FlowRun

	preCommits  map[CommitHook][]interface{}
	postCommits map[CommitHook][]interface{}
}

// Org returns the org assets for this session
func (s *Session) Org() *OrgAssets {
	return s.org
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
func (s *Session) AddPreCommitEvent(hook CommitHook, event interface{}) {
	s.preCommits[hook] = append(s.preCommits[hook], event)
}

// AddPostCommitEvent adds a new event to be handled by a post commit hook
func (s *Session) AddPostCommitEvent(hook CommitHook, event interface{}) {
	s.postCommits[hook] = append(s.postCommits[hook], event)
}

// FlowRun is the mailroom type for a FlowRun
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
	Events string `db:"events"`

	CurrentNodeUUID flows.NodeUUID  `db:"current_node_uuid"`
	ContactID       flows.ContactID `db:"contact_id"`
	FlowID          FlowID          `db:"flow_id"`
	OrgID           OrgID           `db:"org_id"`
	ParentID        null.Int        `db:"parent_id"`
	SessionID       SessionID       `db:"session_id"`
	StartID         null.Int        `db:"start_id"`

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
		return nil, errors.Annotatef(err, "error marshalling flow session")
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
		org:         org,
		preCommits:  make(map[CommitHook][]interface{}),
		postCommits: make(map[CommitHook][]interface{}),
	}

	// now build up our runs
	for _, r := range s.Runs() {
		run, err := newRun(session, r)
		if err != nil {
			return nil, errors.Annotatef(err, "error creating run: %s", r.UUID())
		}

		// save the run to our session
		session.runs = append(session.runs, run)
	}

	return session, nil
}

const insertSessionSQL = `
INSERT INTO
flows_flowsession(status, responded, output, contact_id, org_id)
           VALUES(:status, :responded, :output, :contact_id, :org_id)
RETURNING id
`

// WriteSessions writes the passed in session to our database, writes any runs that need to be created
// as well as appying any events created in the session
func WriteSessions(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, ss []flows.Session) ([]*Session, error) {
	if len(ss) == 0 {
		return nil, nil
	}

	// create all our session objects
	sessions := make([]*Session, 0, len(ss))
	sessionsI := make([]interface{}, 0, len(ss))
	for _, s := range ss {
		session, err := NewSession(org, s)
		if err != nil {
			return nil, errors.Annotatef(err, "error creating session objects")
		}
		sessions = append(sessions, session)
		sessionsI = append(sessionsI, session)
	}

	// insert them all
	err := BulkInsert(ctx, tx, insertSessionSQL, sessionsI)
	if err != nil {
		return nil, errors.Annotatef(err, "error inserting sessions")
	}

	// for each session, apply the events to each run, gathering our list of runs in the process
	runs := make([]interface{}, len(sessions))
	for _, s := range sessions {
		for _, r := range s.runs {
			runs = append(runs, r)

			// set our session id now that it is written
			r.SessionID = s.ID
		}
	}

	// insert all runs
	err = BulkInsert(ctx, tx, insertRunSQL, runs)
	if err != nil {
		return nil, errors.Annotatef(err, "error writing runs")
	}

	// apply our all events for the session
	for i := range ss {
		for _, e := range ss[i].Events() {
			err := ApplyEvent(ctx, tx, rp, sessions[i], e)
			if err != nil {
				return nil, errors.Annotatef(err, "error applying event: %v", e)
			}
		}
	}

	// gather all our pre commit events, group them by hook
	err = ApplyPreEventHooks(ctx, tx, rp, org.OrgID(), sessions)
	if err != nil {
		return nil, errors.Annotatef(err, "error applying pre commit hooks")
	}

	// return our session
	return sessions, nil
}

const insertRunSQL = `
INSERT INTO
flows_flowrun(uuid, is_active, created_on, modified_on, exited_on, exit_type, expires_on, responded, results, path, 
	          events, current_node_uuid, contact_id, flow_id, org_id, session_id)
	   VALUES(:uuid, :is_active, :created_on, NOW(), :exited_on, :exit_type, :expires_on, :responded, :results, :path,
	          :events, :current_node_uuid, :contact_id, :flow_id, :org_id, :session_id)
RETURNING id
`

// newRun writes the passed in flow run to our database, also applying any events in those runs as
// appropriate. (IE, writing db messages etc..)
func newRun(session *Session, r flows.FlowRun) (*FlowRun, error) {
	org := session.Org()

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
		return nil, errors.Annotatef(err, "unable to load flow with uuid: %s", r.Flow().UUID())
	}

	// create our run
	run := &FlowRun{
		UUID:            r.UUID(),
		CreatedOn:       r.CreatedOn(),
		ExitedOn:        r.ExitedOn(),
		ExpiresOn:       r.ExpiresOn(),
		ContactID:       r.Contact().ID(),
		FlowID:          flow.(*Flow).ID(),
		SessionID:       session.ID,
		StartID:         null.NewInt(0, false),
		OrgID:           org.OrgID(),
		Path:            string(pathJSON),
		CurrentNodeUUID: path[len(path)-1].NodeUUID,
		run:             r,
	}

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

	// we filter which events we write to our events json right now
	filteredEvents := make([]flows.Event, 0)
	for _, e := range r.Events() {
		if keptEvents[e.Type()] {
			filteredEvents = append(filteredEvents, e)
		}
	}
	eventJSON, err := json.Marshal(filteredEvents)
	if err != nil {
		return nil, errors.Annotatef(err, "error marshalling events for run: %s", run.UUID)
	}
	run.Events = string(eventJSON)

	// write our results out
	resultsJSON, err := json.Marshal(r.Results())
	if err != nil {
		return nil, errors.Annotatef(err, "error marshalling results for run: %s", run.UUID)
	}
	run.Results = string(resultsJSON)

	// TODO: set responded (always false for now)
	// TODO: set parent id (always null for now)
	return run, nil
}
