package runner

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner/clocks"
	"github.com/nyaruka/mailroom/runtime"
)

const (
	commitTimeout = time.Minute
)

// Scene represents the context that events are occurring in
type Scene struct {
	// required state set on creation
	DBContact *models.Contact
	Contact   *flows.Contact

	// optional state set on creation
	DBCall      *models.Call
	Call        *flows.Call
	StartID     models.StartID
	IncomingMsg *models.MsgInRef
	Broadcast   *models.Broadcast

	// optional state set during processing
	DBSession           *models.Session
	Session             flows.Session
	Sprint              flows.Sprint
	WaitTimeout         time.Duration
	PriorRunModifiedOns map[flows.RunUUID]time.Time
	OutgoingMsgs        []*models.MsgOut

	preCommits    map[PreCommitHook][]any
	postCommits   map[PostCommitHook][]any
	rawEvents     []flows.Event
	persistEvents []*models.Event

	// can be overridden by tests
	Engine func(*runtime.Runtime) flows.Engine
}

// NewScene creates a new scene for the passed in contact
func NewScene(dbContact *models.Contact, contact *flows.Contact) *Scene {
	return &Scene{
		DBContact: dbContact,
		Contact:   contact,

		preCommits:  make(map[PreCommitHook][]any),
		postCommits: make(map[PostCommitHook][]any),
		rawEvents:   make([]flows.Event, 0, 5),

		Engine: goflow.Engine,
	}
}

func (s *Scene) ContactID() models.ContactID    { return models.ContactID(s.Contact.ID()) }
func (s *Scene) ContactUUID() flows.ContactUUID { return s.Contact.UUID() }

// SessionUUID is a convenience utility to get the session UUID for this scene if any
func (s *Scene) SessionUUID() flows.SessionUUID {
	if s.Session == nil {
		return ""
	}
	return s.Session.UUID()
}

// SprintUUID is a convenience utility to get the sprint UUID for this scene if any
func (s *Scene) SprintUUID() flows.SprintUUID {
	if s.Sprint == nil {
		return ""
	}
	return s.Sprint.UUID()
}

// Events returns all events added to this scene (includes non-persisted events)
func (s *Scene) Events() []flows.Event { return s.rawEvents }

func (s *Scene) AddEvent(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, e flows.Event, userID models.UserID, via models.Via) error {
	handler := eventHandlers[e.Type()]
	if handler == nil {
		panic(fmt.Sprintf("no handler for event type: %s", e.Type()))
	}

	if err := handler(ctx, rt, oa, s, e, userID); err != nil {
		return err
	}

	// turn our userID into a reference
	var user *models.User
	if userID != models.NilUserID {
		user = oa.UserByID(userID)
	}

	e.SetUser(user.Reference(), string(via))

	switch e.(type) {
	case *ContactInterruptedEvent, *SprintEndedEvent: // our pseudo events aren't real...
	default:
		s.rawEvents = append(s.rawEvents, e)

		if models.PersistEvent(e) {
			s.persistEvents = append(s.persistEvents, &models.Event{
				Event:       e,
				OrgID:       oa.OrgID(),
				ContactUUID: s.ContactUUID(),
			})
		}
	}

	return nil
}

func (s *Scene) addSprint(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ss flows.Session, sp flows.Sprint, resumed bool) error {
	s.Session = ss
	s.Sprint = sp

	evts := make([]flows.Event, 0, len(sp.Events())+1)

	for _, e := range sp.Events() {
		// if session failed, only include failure events, otherwise all events
		if ss.Status() != flows.SessionStatusFailed || e.Type() == events.TypeFailure {
			evts = append(evts, e)
		}
	}

	evts = append(evts, newSprintEndedEvent(s.DBContact, resumed))

	for _, e := range evts {
		if err := s.AddEvent(ctx, rt, oa, e, models.NilUserID, ""); err != nil {
			return fmt.Errorf("error adding event to scene: %w", err)
		}
	}

	return nil
}

// ReevaluateGroups re-evaluates query based group membership for this scene's contact and adds any resulting events.
func (s *Scene) ReevaluateGroups(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	evts := make([]flows.Event, 0, 1)
	log := func(e flows.Event) { evts = append(evts, e) }

	modifiers.ReevaluateGroups(oa.Env(), s.Contact, log)

	for _, e := range evts {
		if err := s.AddEvent(ctx, rt, oa, e, models.NilUserID, ""); err != nil {
			return fmt.Errorf("error adding group event to scene: %w", err)
		}
	}
	return nil
}

func (s *Scene) InterruptWaiting(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, status flows.SessionStatus) error {
	return addInterruptEvents(ctx, rt, oa, []*Scene{s}, nil, status)
}

// StartSession starts a new session.
func (s *Scene) StartSession(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, trigger flows.Trigger, interrupt bool) error {
	// interrupting supported from here as a convenience
	if interrupt {
		if err := addInterruptEvents(ctx, rt, oa, []*Scene{s}, nil, flows.SessionStatusInterrupted); err != nil {
			return fmt.Errorf("error interrupting existing session: %w", err)
		}
	}

	session, sprint, err := s.Engine(rt).NewSession(ctx, oa.SessionAssets(), oa.Env(), s.Contact, trigger, s.Call)
	if err != nil {
		return fmt.Errorf("error starting contact %s in flow %s: %w", s.ContactUUID(), trigger.Flow().UUID, err)
	}

	if err := s.addSprint(ctx, rt, oa, session, sprint, false); err != nil {
		return fmt.Errorf("error adding events for session %s: %w", session.UUID(), err)
	}

	return nil
}

// ResumeSession resumes the passed in session
func (s *Scene) ResumeSession(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, session *models.Session, resume flows.Resume) error {
	if s.Sprint != nil {
		panic("scene already has a sprint")
	}

	// does the flow this session is part of still exist?
	_, err := oa.FlowByUUID(session.CurrentFlowUUID)
	if err != nil {
		if err == models.ErrNotFound {
			// if flow doesn't exist, we can't resume, so fail the session
			slog.Debug("unable to find flow for resume", "contact", s.ContactUUID(), "session", session.UUID, "flow", session.CurrentFlowUUID)

			if err := s.InterruptWaiting(ctx, rt, oa, flows.SessionStatusFailed); err != nil {
				return fmt.Errorf("error adding interrupt events for unresumable session %s: %w", session.UUID, err)
			}

			return nil
		}
		return fmt.Errorf("error loading session flow %s: %w", session.CurrentFlowUUID, err)
	}

	// build our flow session
	fs, err := session.EngineSession(ctx, rt, oa.SessionAssets(), oa.Env(), s.Contact, s.Call)
	if err != nil {
		return fmt.Errorf("unable to read session %s: %w", session.UUID, err)
	}

	// record run modified times prior to resuming so we can figure out which runs are new or updated
	s.DBSession = session
	s.PriorRunModifiedOns = make(map[flows.RunUUID]time.Time, len(fs.Runs()))
	for _, r := range fs.Runs() {
		s.PriorRunModifiedOns[r.UUID()] = r.ModifiedOn()
	}

	sprint, err := fs.Resume(ctx, resume)
	if err != nil {
		return fmt.Errorf("error resuming flow: %w", err)
	}

	if err := s.addSprint(ctx, rt, oa, fs, sprint, true); err != nil {
		return fmt.Errorf("error processing events for session %s: %w", session.UUID, err)
	}

	return nil
}

func (s *Scene) ApplyModifier(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, mod flows.Modifier, userID models.UserID, via models.Via) error {
	env := flows.NewAssetsEnvironment(oa.Env(), oa.SessionAssets())
	eng := goflow.Engine(rt)

	evts := make([]flows.Event, 0)
	evtLog := func(e flows.Event) { evts = append(evts, e) }

	if _, err := modifiers.Apply(ctx, eng, env, oa.SessionAssets(), s.Contact, mod, evtLog); err != nil {
		return fmt.Errorf("error applying %s modifier to contact %s: %w", mod.Type(), s.Contact.UUID(), err)
	}

	for _, e := range evts {
		creditUserID := userID

		// don't credit group changes to the user if they didn't initiate them, and never credit warnings or errors
		if (e.Type() == events.TypeContactGroupsChanged && mod.Type() != modifiers.TypeGroups) || e.Type() == events.TypeWarning || e.Type() == events.TypeError {
			creditUserID = models.NilUserID
		}

		if err := s.AddEvent(ctx, rt, oa, e, creditUserID, via); err != nil {
			return fmt.Errorf("error adding modifier events for contact %s: %w", s.Contact.UUID(), err)
		}
	}

	return nil
}

// AttachPreCommitHook adds an item to be handled by the given pre commit hook
func (s *Scene) AttachPreCommitHook(hook PreCommitHook, item any) {
	s.preCommits[hook] = append(s.preCommits[hook], item)
}

// AttachPostCommitHook adds an item to be handled by the given post commit hook
func (s *Scene) AttachPostCommitHook(hook PostCommitHook, item any) {
	s.postCommits[hook] = append(s.postCommits[hook], item)
}

// Commit commits this scene's events
func (s *Scene) Commit(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	return BulkCommit(ctx, rt, oa, []*Scene{s})
}

// CreateScenes creates scenes for the given contact ids
func CreateScenes(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactIDs []models.ContactID, extraTickets map[models.ContactID][]*models.Ticket) ([]*Scene, error) {
	mcs, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, contactIDs)
	if err != nil {
		return nil, fmt.Errorf("error loading contacts for new scenes: %w", err)
	}

	scenes := make([]*Scene, len(mcs))
	for i, mc := range mcs {
		if extra, found := extraTickets[mc.ID()]; found {
			mc.IncludeTickets(extra)
		}

		c, err := mc.EngineContact(oa)
		if err != nil {
			return nil, fmt.Errorf("error creating engine contact for %s: %w", mc.UUID(), err)
		}

		scenes[i] = NewScene(mc, c)
	}

	return scenes, nil
}

// LockAndLoad tries to lock and load scenes for the given contact ids, returning any ids that could not be locked.
// The caller is responsible for unlocking the scenes by calling the returned unlock function.
func LockAndLoad(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, ids []models.ContactID, includeTickets map[models.ContactID][]*models.Ticket, timeout time.Duration) ([]*Scene, []models.ContactID, func(), error) {
	allScenes := make([]*Scene, 0, len(ids))
	allLocks := make(map[models.ContactID]string, len(ids))
	allGood := false

	unlockAll := func() {
		if err := clocks.Unlock(context.Background(), rt, oa, allLocks); err != nil {
			slog.Error("error unlocking contacts", "error", err, "contacts", maps.Keys(allLocks))
		}
	}

	defer func() {
		if !allGood {
			unlockAll()
		}
	}()

	remaining := ids
	start := time.Now()

	for len(remaining) > 0 && time.Since(start) < timeout {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}

		// try to get locks for these contacts, waiting for up to a second for each contact
		locks, skipped, err := clocks.TryToLock(ctx, rt, oa, remaining, time.Second)
		if err != nil {
			return nil, nil, nil, err
		}
		maps.Copy(allLocks, locks)

		locked := slices.Collect(maps.Keys(locks))

		// create scenes for the locked contacts
		scenes, err := CreateScenes(ctx, rt, oa, locked, includeTickets)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error creating scenes after locking: %w", err)
		}

		allScenes = append(allScenes, scenes...)

		remaining = skipped // skipped are now our remaining
	}

	// for test determinism
	slices.SortFunc(allScenes, func(a, b *Scene) int { return cmp.Compare(a.Contact.ID(), b.Contact.ID()) })

	allGood = true // to prevent unlock in defer

	return allScenes, remaining, unlockAll, nil
}

// BulkCommit commits the passed in scenes in a single transaction. If that fails, it retries committing each scene one at a time.
func BulkCommit(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scenes []*Scene) error {
	if len(scenes) == 0 {
		return nil // nothing to do
	}

	txCTX, cancel := context.WithTimeout(ctx, commitTimeout*time.Duration(len(scenes)))
	defer cancel()

	tx, err := rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction for bulk scene commit: %w", err)
	}

	if err := ExecutePreCommitHooks(ctx, rt, tx, oa, scenes); err != nil {
		tx.Rollback()
		return fmt.Errorf("error executing scene pre commit hooks: %w", err)
	}

	if err := tx.Commit(); err != nil {
		// retry committing our scenes one at a time
		slog.Debug("failed committing scenes in bulk, retrying one at a time", "error", err)

		tx.Rollback()

		// we failed committing the scenes in one go, try one at a time
		for _, scene := range scenes {
			txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				return fmt.Errorf("error starting transaction for retry: %w", err)
			}

			if err := ExecutePreCommitHooks(ctx, rt, tx, oa, []*Scene{scene}); err != nil {
				return fmt.Errorf("error applying scene pre commit hooks: %w", err)
			}

			if err := tx.Commit(); err != nil {
				tx.Rollback()
				slog.Error("error committing scene", "error", err, "contact", scene.ContactUUID())
				continue
			}
		}
	}

	// send events to be persisted to the history table writer
	eventsWritten := 0
	for _, scene := range scenes {
		for _, evt := range scene.persistEvents {
			if _, err := rt.Dynamo.History.Queue(evt); err != nil {
				return fmt.Errorf("error queuing scene event to writer: %w", err)
			}
		}

		eventsWritten += len(scene.persistEvents)
	}

	slog.Debug("events queued to history writer", "count", eventsWritten)

	if err := ExecutePostCommitHooks(ctx, rt, oa, scenes); err != nil {
		return fmt.Errorf("error processing post commit hooks: %w", err)
	}

	return nil
}
