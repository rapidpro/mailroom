package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

// ModifyWithLock bulk modifies contacts by locking and loading them, applying modifiers and processing the resultant events.
func ModifyWithLock(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, userID models.UserID, contactIDs []models.ContactID, mods map[models.ContactID][]flows.Modifier, includeTickets map[models.ContactID][]*models.Ticket, via models.Via) (map[*flows.Contact][]flows.Event, []models.ContactID, error) {
	scenes, skipped, unlock, err := LockAndLoad(ctx, rt, oa, contactIDs, includeTickets, 10*time.Second)
	if err != nil {
		return nil, nil, err
	}

	defer unlock() // contacts are unlocked whatever happens

	evts, err := applyModifiers(ctx, rt, oa, userID, scenes, mods, via)
	if err != nil {
		return nil, nil, err
	}

	if err := BulkCommit(ctx, rt, oa, scenes); err != nil {
		return nil, nil, fmt.Errorf("error committing scenes from modifiers: %w", err)
	}

	return evts, skipped, nil
}

// ModifyWithoutLock bulk modifies contacts without locking - used during contact creation and imports.
func ModifyWithoutLock(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, userID models.UserID, mcs []*models.Contact, contacts []*flows.Contact, mods map[models.ContactID][]flows.Modifier, via models.Via) (map[*flows.Contact][]flows.Event, error) {
	scenes := make([]*Scene, len(mcs))
	for i, mc := range mcs {
		scenes[i] = NewScene(mc, contacts[i])
	}

	evts, err := applyModifiers(ctx, rt, oa, userID, scenes, mods, via)
	if err != nil {
		return nil, err
	}

	if err := BulkCommit(ctx, rt, oa, scenes); err != nil {
		return nil, fmt.Errorf("error committing scenes from modifiers: %w", err)
	}

	return evts, nil
}

func applyModifiers(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, userID models.UserID, scenes []*Scene, mods map[models.ContactID][]flows.Modifier, via models.Via) (map[*flows.Contact][]flows.Event, error) {
	evts := make(map[*flows.Contact][]flows.Event, len(scenes))

	for _, scene := range scenes {
		for _, mod := range mods[scene.ContactID()] {
			if err := scene.ApplyModifier(ctx, rt, oa, mod, userID, via); err != nil {
				return nil, fmt.Errorf("error applying modifier %T to contact %s: %w", mod, scene.ContactUUID(), err)
			}
		}

		evts[scene.Contact] = scene.Events()
	}

	return evts, nil
}

// ReevaluateGroupsWithLock re-evaluates query-based group membership for the given contacts
func ReevaluateGroupsWithLock(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contactIDs []models.ContactID) ([]models.ContactID, error) {
	scenes, skipped, unlock, err := LockAndLoad(ctx, rt, oa, contactIDs, nil, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("error locking contacts: %w", err)
	}
	defer unlock()

	for _, scene := range scenes {
		if err := scene.ReevaluateGroups(ctx, rt, oa); err != nil {
			return nil, fmt.Errorf("error re-evaluating groups: %w", err)
		}
	}

	if err := BulkCommit(ctx, rt, oa, scenes); err != nil {
		return nil, fmt.Errorf("error committing group population: %w", err)
	}

	return skipped, nil
}
