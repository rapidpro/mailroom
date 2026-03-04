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
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/locks"
)

// TypePopulateGroup is the type of the populate group task
const TypePopulateGroup = "populate_group"

const populateGroupLockKey = "lock:pop_dyn_group_%d"
const populateGroupBatchesRemainingKey = "populate_group_batches_remaining:%s"
const populateBatchSize = 100

func init() {
	RegisterType(TypePopulateGroup, func() Task { return &PopulateGroup{} })
	RegisterType("populate_dynamic_group", func() Task { return &PopulateGroup{} }) // support old name
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
	return time.Minute * 10
}

func (t *PopulateGroup) WithAssets() models.Refresh {
	return models.RefreshGroups
}

// Perform figures out the membership for a query based group then queues batch tasks to repopulate it
func (t *PopulateGroup) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	locker := locks.NewLocker(fmt.Sprintf(populateGroupLockKey, t.GroupID), time.Hour)
	lock, err := locker.Grab(ctx, rt.VK, time.Minute*5)
	if err != nil {
		return fmt.Errorf("error grabbing lock to repopulate query group: %d: %w", t.GroupID, err)
	}
	if lock == "" {
		return fmt.Errorf("timeout waiting for lock to repopulate query group: %d", t.GroupID)
	}

	// by default release the lock when we're done, unless we've queued batch tasks which will release it
	releaseLock := true
	defer func() {
		if releaseLock {
			locker.Release(ctx, rt.VK, lock)
		}
	}()

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

	if err != nil {
		var qerr *contactql.QueryError
		if errors.As(err, &qerr) {
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

			if err := models.UpdateGroupStatus(ctx, rt.DB, t.GroupID, models.GroupStatusInvalid); err != nil {
				return fmt.Errorf("error updating query group status: %w", err)
			}
			return nil
		}
		return fmt.Errorf("error performing query: %s for group: %d: %w", t.Query, t.GroupID, err)
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

	// if there are no contacts to recheck, mark the group as ready immediately
	if len(recheckIDs) == 0 {
		if err := models.UpdateGroupStatus(ctx, rt.DB, t.GroupID, models.GroupStatusReady); err != nil {
			return fmt.Errorf("error updating query group status: %w", err)
		}
		return nil
	}

	// chunk contacts into batches and queue a task for each
	batches := slices.Collect(slices.Chunk(recheckIDs, populateBatchSize))

	// generate a random ID for this population run so batch tasks can track completion
	populationID := vkutil.RandomBase64(10)

	// set valkey counter which batch tasks can decrement to know when population has completed
	counter := NewCounter(fmt.Sprintf(populateGroupBatchesRemainingKey, populationID), time.Hour)
	if err := counter.Init(ctx, rt.VK, len(batches)); err != nil {
		return fmt.Errorf("error setting populate group batch counter key: %w", err)
	}

	for _, batch := range batches {
		task := &PopulateGroupBatch{
			GroupID:      t.GroupID,
			ContactIDs:   batch,
			LockValue:    lock,
			PopulationID: populationID,
		}
		if err := Queue(ctx, rt, rt.Queues.Batch, oa.OrgID(), task, false); err != nil {
			return fmt.Errorf("error queuing populate group batch task: %w", err)
		}
	}

	// batch tasks will release the lock when the last one completes
	releaseLock = false

	slog.Info("queued populate group batch tasks", "group_id", t.GroupID, "batches", len(batches), "contacts", len(recheckIDs))

	return nil
}
