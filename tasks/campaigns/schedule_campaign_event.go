package campaigns

import (
	"context"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/tasks"
	"github.com/nyaruka/mailroom/utils/locker"

	"github.com/pkg/errors"
)

// TypeScheduleCampaignEvent is the type of the schedule event task
const TypeScheduleCampaignEvent = "schedule_campaign_event"

const scheduleLockKey string = "schedule_campaign_event_%d"

func init() {
	tasks.RegisterType(TypeScheduleCampaignEvent, func() tasks.Task { return &ScheduleCampaignEventTask{} })
}

// ScheduleCampaignEventTask is our definition of our event recalculation task
type ScheduleCampaignEventTask struct {
	OrgID           models.OrgID           `json:"org_id"`
	CampaignEventID models.CampaignEventID `json:"campaign_event_id"`
}

// Timeout is the maximum amount of time the task can run for
func (t *ScheduleCampaignEventTask) Timeout() time.Duration {
	return time.Hour
}

// Perform creates the actual event fires to schedule the given campaign event
func (t *ScheduleCampaignEventTask) Perform(ctx context.Context, mr *mailroom.Mailroom) error {
	db := mr.DB
	rp := mr.RP
	lockKey := fmt.Sprintf(scheduleLockKey, t.CampaignEventID)

	lock, err := locker.GrabLock(rp, lockKey, time.Hour, time.Minute*5)
	if err != nil {
		return errors.Wrapf(err, "error grabbing lock to schedule campaign event %d", t.CampaignEventID)
	}
	defer locker.ReleaseLock(rp, lockKey, lock)

	err = models.ScheduleCampaignEvent(ctx, db, t.OrgID, t.CampaignEventID)
	if err != nil {
		return errors.Wrapf(err, "error scheduling campaign event %d", t.CampaignEventID)
	}

	return nil
}
