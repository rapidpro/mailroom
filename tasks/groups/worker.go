package groups

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/locker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(queue.PopulateDynamicGroup, handlePopulateDynamicGroup)
}

// PopulateTask is our definition of our group population
type PopulateTask struct {
	OrgID   models.OrgID   `json:"org_id"`
	GroupID models.GroupID `json:"group_id"`
	Query   string         `json:"query"`
}

const populateLockKey string = "pop_dyn_group_%d"

// handlePopulateDynamicGroup figures out the membership for a dynamic group then repopulates it
func handlePopulateDynamicGroup(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	// decode our task body
	if task.Type != queue.PopulateDynamicGroup {
		return errors.Errorf("unknown event type passed to populate dynamic group worker: %s", task.Type)
	}
	t := &PopulateTask{}
	err := json.Unmarshal(task.Task, t)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling task: %s", string(task.Task))
	}

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

	org, err := models.GetOrgAssets(ctx, mr.DB, t.OrgID)
	if err != nil {
		return errors.Wrapf(err, "unable to load org when populating group: %d", t.GroupID)
	}

	count, err := models.PopulateDynamicGroup(ctx, mr.DB, mr.ElasticClient, org, t.GroupID, t.Query)
	if err != nil {
		return errors.Wrapf(err, "error populating dynamic group: %d", t.GroupID)
	}
	logrus.WithField("elapsed", time.Since(start)).WithField("count", count).Info("completed populating dynamic group")

	return nil
}
