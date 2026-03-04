package tasks

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/vkutil/locks"
)

// TypePopulateGroupBatch is the type of the populate group batch task
const TypePopulateGroupBatch = "populate_group_batch"

func init() {
	RegisterType(TypePopulateGroupBatch, func() Task { return &PopulateGroupBatch{} })
}

// PopulateGroupBatch is our task to re-evaluate group membership for a batch of contacts
type PopulateGroupBatch struct {
	GroupID      models.GroupID     `json:"group_id"`
	ContactIDs   []models.ContactID `json:"contact_ids"`
	LockValue    string             `json:"lock_value"`
	PopulationID string             `json:"population_id"`
}

func (t *PopulateGroupBatch) Type() string {
	return TypePopulateGroupBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *PopulateGroupBatch) Timeout() time.Duration {
	return time.Minute
}

func (t *PopulateGroupBatch) WithAssets() models.Refresh {
	return models.RefreshGroups
}

// Perform re-evaluates group membership for a batch of contacts
func (t *PopulateGroupBatch) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets) error {
	skipped, err := runner.ReevaluateGroupsWithLock(ctx, rt, oa, t.ContactIDs)
	if err != nil {
		return fmt.Errorf("error populating group membership: %w", err)
	}

	if len(skipped) > 0 {
		slog.Warn("failed to acquire locks for contacts during group population", "group_id", t.GroupID, "skipped", len(skipped))
	}

	// decrement the counter to see if the overall population is now finished
	counter := NewCounter(fmt.Sprintf(populateGroupBatchesRemainingKey, t.PopulationID), time.Hour)
	done, err := counter.Done(ctx, rt.VK)
	if err != nil {
		return fmt.Errorf("error decrementing populate group batch counter: %w", err)
	}
	if done {
		if err := models.UpdateGroupStatus(ctx, rt.DB, t.GroupID, models.GroupStatusReady); err != nil {
			return fmt.Errorf("error updating query group status: %w", err)
		}

		// release the distributed lock now that all batches are complete
		locker := locks.NewLocker(fmt.Sprintf(populateGroupLockKey, t.GroupID), time.Minute)
		if err := locker.Release(ctx, rt.VK, t.LockValue); err != nil {
			return fmt.Errorf("error releasing populate group lock: %w", err)
		}

		slog.Info("completed populating query group", "group_id", t.GroupID)
	}

	return nil
}
