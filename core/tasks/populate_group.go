package tasks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/vkutil/locks"
)

// TypePopulateGroup is the type of the populate group task
const TypePopulateGroup = "populate_dynamic_group"

const populateGroupLockKey string = "lock:pop_dyn_group_%d"
const populateBatchSize = 100

func init() {
	RegisterType(TypePopulateGroup, func() Task { return &PopulateGroup{} })
	RegisterType("populate_group", func() Task { return &PopulateGroup{} }) // support new name, still queue with old
}

// PopulateGroup is our task to populate the contacts for a dynamic group
type PopulateGroup struct {
	GroupID models.GroupID `json:"group_id"`
	Query   string         `json:"query"`
}

func (t *PopulateGroup) Type() string {
	return TypePopulateGroup
}

// Timeout is the maximum amount of time the task can run for
func (t *PopulateGroup) Timeout() time.Duration {
	return time.Hour
}

func (t *PopulateGroup) WithAssets() models.Refresh {
	return models.RefreshGroups
}

// Perform figures out the membership for a query based group then repopulates it
func (t *PopulateGroup) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	locker := locks.NewLocker(fmt.Sprintf(populateGroupLockKey, t.GroupID), time.Hour)
	lock, err := locker.Grab(ctx, rt.VK, time.Minute*5)
	if err != nil {
		return fmt.Errorf("error grabbing lock to repopulate query group: %d: %w", t.GroupID, err)
	}
	if lock == "" {
		return fmt.Errorf("timeout waiting for lock to repopulate query group: %d", t.GroupID)
	}
	defer locker.Release(ctx, rt.VK, lock)

	start := time.Now()

	slog.Info("starting population of query group", "group_id", t.GroupID, "org_id", oa.OrgID(), "query", t.Query)

	if err := models.UpdateGroupStatus(ctx, rt.DB, t.GroupID, models.GroupStatusEvaluating); err != nil {
		return fmt.Errorf("error marking query group as evaluating: %w", err)
	}

	// reload org assets so group is included (groups with status 'X' are excluded from assets)
	oa, err = models.GetOrgAssetsWithRefresh(ctx, rt, oa.OrgID(), models.RefreshGroups)
	if err != nil {
		return fmt.Errorf("error reloading org assets: %w", err)
	}

	// get current members of the group
	currentIDs, err := models.GetGroupContactIDs(ctx, rt.DB, t.GroupID)
	if err != nil {
		return fmt.Errorf("unable to look up contact ids for group: %d: %w", t.GroupID, err)
	}

	// get contacts that match the query from search
	matchedIDs, err := search.GetContactIDsForQuery(ctx, rt, oa, nil, models.ContactStatusActive, t.Query, -1)
	endStatus := models.GroupStatusReady

	if err != nil {
		var qerr *contactql.QueryError
		if errors.As(err, &qerr) {
			matchedIDs = nil
			endStatus = models.GroupStatusInvalid

			// remove current members from the group since the query is invalid and can't be
			// in session assets for re-evaluation to handle
			if len(currentIDs) > 0 {
				removals := make([]*models.GroupRemove, len(currentIDs))
				for i, id := range currentIDs {
					removals[i] = &models.GroupRemove{ContactID: id, GroupID: t.GroupID}
				}
				if err := models.RemoveContactsFromGroups(ctx, rt.DB, removals); err != nil {
					return fmt.Errorf("error removing contacts from invalid group: %w", err)
				}
			}
		} else {
			return fmt.Errorf("error performing query: %s for group: %d: %w", t.Query, t.GroupID, err)
		}
	}

	// build the union of current members and matched contacts as the set to re-check
	recheckSet := make(map[models.ContactID]struct{}, len(currentIDs)+len(matchedIDs))
	for _, id := range currentIDs {
		recheckSet[id] = struct{}{}
	}
	for _, id := range matchedIDs {
		recheckSet[id] = struct{}{}
	}
	recheckIDs := slices.Collect(maps.Keys(recheckSet))

	// lock contacts in batches, re-evaluate group membership, and handle events
	for batch := range slices.Chunk(recheckIDs, populateBatchSize) {
		skipped, err := runner.ReevaluateGroupsWithLock(ctx, rt, oa, batch)
		if err != nil {
			return fmt.Errorf("error populating group membership: %w", err)
		}

		if len(skipped) > 0 {
			slog.Warn("failed to acquire locks for contacts during group population", "skipped", len(skipped))
		}
	}

	// mark our group as either ready or invalid
	if err := models.UpdateGroupStatus(ctx, rt.DB, t.GroupID, endStatus); err != nil {
		return fmt.Errorf("error updating query group status: %w", err)
	}

	slog.Info("completed populating query group", "elapsed", time.Since(start), "count", len(matchedIDs))

	return nil
}
