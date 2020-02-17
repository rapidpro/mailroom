package models

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/pkg/errors"
)

// Scene represents the context that events are occurring in
type Scene struct {
	contact *flows.Contact
	session *Session

	preCommits  map[EventCommitHook][]interface{}
	postCommits map[EventCommitHook][]interface{}
}

func NewSceneForSession(session *Session) *Scene {
	s := &Scene{
		contact: session.Contact(),
		session: session,

		preCommits:  make(map[EventCommitHook][]interface{}),
		postCommits: make(map[EventCommitHook][]interface{}),
	}
	return s
}

func NewSceneForContact(contact *flows.Contact) *Scene {
	s := &Scene{
		contact: contact,

		preCommits:  make(map[EventCommitHook][]interface{}),
		postCommits: make(map[EventCommitHook][]interface{}),
	}
	return s
}

func (s *Scene) ID() SessionID {
	if s.session == nil {
		return SessionID(0)
	}
	return s.session.ID()
}

func (s *Scene) Contact() *flows.Contact        { return s.contact }
func (s *Scene) ContactID() ContactID           { return ContactID(s.contact.ID()) }
func (s *Scene) ContactUUID() flows.ContactUUID { return s.contact.UUID() }

func (s *Scene) Session() *Session { return s.session }

// AddPreCommitEvent adds a new event to be handled by a pre commit hook
func (s *Scene) AddPreCommitEvent(hook EventCommitHook, event interface{}) {
	s.preCommits[hook] = append(s.preCommits[hook], event)
}

// AddPostCommitEvent adds a new event to be handled by a post commit hook
func (s *Scene) AddPostCommitEvent(hook EventCommitHook, event interface{}) {
	s.postCommits[hook] = append(s.postCommits[hook], event)
}

// EventCommitHook defines a callback that will accept a certain type of events across session, either before or after committing
type EventCommitHook interface {
	Apply(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, map[*Scene][]interface{}) error
}

// ApplyPreEventHooks runs through all the pre event hooks for the passed in sessions and applies their events
func ApplyPreEventHooks(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, scenes []*Scene) error {
	// gather all our hook events together across our sessions
	preHooks := make(map[EventCommitHook]map[*Scene][]interface{})
	for _, s := range scenes {
		for hook, args := range s.preCommits {
			sessionMap, found := preHooks[hook]
			if !found {
				sessionMap = make(map[*Scene][]interface{}, len(scenes))
				preHooks[hook] = sessionMap
			}
			sessionMap[s] = args
		}
	}

	// now fire each of our hooks
	for hook, args := range preHooks {
		err := hook.Apply(ctx, tx, rp, org, args)
		if err != nil {
			return errors.Wrapf(err, "error applying pre commit hook: %T", hook)
		}
	}

	return nil
}

// ApplyPostEventHooks runs through all the post event hooks for the passed in sessions and applies their events
func ApplyPostEventHooks(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, scenes []*Scene) error {
	// gather all our hook events together across our sessions
	postHooks := make(map[EventCommitHook]map[*Scene][]interface{})
	for _, s := range scenes {
		for hook, args := range s.postCommits {
			sprintMap, found := postHooks[hook]
			if !found {
				sprintMap = make(map[*Scene][]interface{}, len(scenes))
				postHooks[hook] = sprintMap
			}
			sprintMap[s] = args
		}
	}

	// now fire each of our hooks
	for hook, args := range postHooks {
		err := hook.Apply(ctx, tx, rp, org, args)
		if err != nil {
			return errors.Wrapf(err, "error applying post commit hook: %v", hook)
		}
	}

	return nil
}

// EventHandler defines a call for handling events that occur in a flow
type EventHandler func(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, *Scene, flows.Event) error

// RegisterEventHook registers the passed in handler as being interested in the passed in type
func RegisterEventHook(eventType string, handler EventHandler) {
	// it's a bug if we try to register more than one handler for a type
	_, found := eventHandlers[eventType]
	if found {
		panic(errors.Errorf("duplicate handler being registered for type: %s", eventType))
	}
	eventHandlers[eventType] = handler
}

// RegisterPreWriteHook registers the passed in handler as being interested in the passed in type before session and run insertion
func RegisterPreWriteHook(eventType string, handler EventHandler) {
	// it's a bug if we try to register more than one handler for a type
	_, found := preHandlers[eventType]
	if found {
		panic(errors.Errorf("duplicate handler being registered for type: %s", eventType))
	}
	preHandlers[eventType] = handler
}

// ApplyEvents applies the passed in event, IE, creates the db objects required etc..
func ApplyEvents(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, scene *Scene, events []flows.Event) error {
	for _, e := range events {

		handler, found := eventHandlers[e.Type()]
		if !found {
			return errors.Errorf("unable to find handler for event type: %s", e.Type())
		}

		err := handler(ctx, tx, rp, org, scene, e)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyPreWriteEvent applies the passed in event before insertion or update, unlike normal event handlers it is not a requirement
// that all types have a handler.
func ApplyPreWriteEvent(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, scene *Scene, e flows.Event) error {
	handler, found := preHandlers[e.Type()]
	if !found {
		return nil
	}

	return handler(ctx, tx, rp, org, scene, e)
}

// our map of event type to internal handlers
var eventHandlers = make(map[string]EventHandler)

// our map of event type to pre insert handlers
var preHandlers = make(map[string]EventHandler)
