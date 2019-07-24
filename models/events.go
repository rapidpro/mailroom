package models

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/pkg/errors"
)

// EventCommitHook defines a callback that will accept a certain type of events across session, either before or after committing
type EventCommitHook interface {
	Apply(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, map[*Session][]interface{}) error
}

// ApplyPreEventHooks runs through all the pre event hooks for the passed in sessions and applies their events
func ApplyPreEventHooks(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, sessions []*Session) error {
	// gather all our hook events together across our sessions
	preHooks := make(map[EventCommitHook]map[*Session][]interface{})
	for _, s := range sessions {
		for hook, args := range s.preCommits {
			sessionMap, found := preHooks[hook]
			if !found {
				sessionMap = make(map[*Session][]interface{}, len(sessions))
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
func ApplyPostEventHooks(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, sessions []*Session) error {
	// gather all our hook events together across our sessions
	postHooks := make(map[EventCommitHook]map[*Session][]interface{})
	for _, s := range sessions {
		for hook, args := range s.postCommits {
			sessionMap, found := postHooks[hook]
			if !found {
				sessionMap = make(map[*Session][]interface{}, len(sessions))
				postHooks[hook] = sessionMap
			}
			sessionMap[s] = args
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
type EventHandler func(context.Context, *sqlx.Tx, *redis.Pool, *OrgAssets, *Session, flows.Event) error

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

// ApplyEvent applies the passed in event, IE, creates the db objects required etc..
func ApplyEvent(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, session *Session, e flows.Event) error {
	// if this session is errored, don't apply any hooks
	if session.Status() == SessionStatusErrored {
		return nil
	}

	handler, found := eventHandlers[e.Type()]
	if !found {
		return errors.Errorf("unable to find handler for event type: %s", e.Type())
	}

	return handler(ctx, tx, rp, org, session, e)
}

// ApplyPreWriteEvent applies the passed in event before insertion or update, unlike normal event handlers it is not a requirement
// that all types have a handler.
func ApplyPreWriteEvent(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *OrgAssets, session *Session, e flows.Event) error {
	// if this session is errored, don't apply any hooks
	if session.Status() == SessionStatusErrored {
		return nil
	}

	handler, found := preHandlers[e.Type()]
	if !found {
		return nil
	}

	return handler(ctx, tx, rp, org, session, e)
}

// our map of event type to internal handlers
var eventHandlers = make(map[string]EventHandler)

// our map of event type to pre insert handlers
var preHandlers = make(map[string]EventHandler)
