package groups

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/tasks"
	"github.com/nyaruka/mailroom/utils/locker"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// TypePopulateDynamicGroup is the type of the populate group task
const TypePopulateDynamicGroup = "populate_dynamic_group"

const populateLockKey string = "pop_dyn_group_%d"

func init() {
	tasks.RegisterType(TypePopulateDynamicGroup, func() tasks.Task { return &PopulateTask{} })
}

// PopulateTask is our task to populate the contacts for a dynamic group
type PopulateTask struct {
	OrgID   models.OrgID   `json:"org_id"`
	GroupID models.GroupID `json:"group_id"`
	Query   string         `json:"query"`
}

// Perform figures out the membership for a query based group then repopulates it
func (t *PopulateTask) Perform(ctx context.Context, mr *mailroom.Mailroom) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	lockKey := fmt.Sprintf(populateLockKey, t.GroupID)
	lock, err := locker.GrabLock(mr.RP, lockKey, time.Hour, time.Minute*5)
	if err != nil {
		return errors.Wrapf(err, "error grabbing lock to repopulate dynamic group: %d", t.GroupID)
	}
	defer locker.ReleaseLock(mr.RP, lockKey, lock)

	start := time.Now()
	log := logrus.WithFields(logrus.Fields{
		"group_id": t.GroupID,
		"org_id":   t.OrgID,
		"query":    t.Query,
	})

	log.Info("starting population of dynamic group")

	oa, err := models.GetOrgAssets(ctx, mr.DB, t.OrgID)
	if err != nil {
		return errors.Wrapf(err, "unable to load org when populating group: %d", t.GroupID)
	}

	count, err := models.PopulateDynamicGroup(ctx, mr.DB, mr.ElasticClient, oa, t.GroupID, t.Query)
	if err != nil {
		return errors.Wrapf(err, "error populating dynamic group: %d", t.GroupID)
	}
	logrus.WithField("elapsed", time.Since(start)).WithField("count", count).Info("completed populating dynamic group")

	return nil
}
