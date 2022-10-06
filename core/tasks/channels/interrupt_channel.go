package channels

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
)

// TypeInterruptChannel is the type of the interruption of a channel
const TypeInterruptChannel = "interrupt_channel"

func init() {
	tasks.RegisterType(TypeInterruptChannel, func() tasks.Task { return &InterruptChannelTask{} })
}

// InterruptChannelTask is our task to interrupt a channel
type InterruptChannelTask struct {
	ChannelID models.ChannelID `json:"channel_id"`
}

// Perform implements tasks.Task
func (t *InterruptChannelTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	db := rt.DB
	rc := rt.RP.Get()
	defer rc.Close()

	channelIDs := []models.ChannelID{t.ChannelID}

	channels, err := models.GetChannelsByID(ctx, db, channelIDs)
	if err != nil {
		return err
	}

	channel := channels[0]

	if err := models.InterruptSessionsForChannels(ctx, db, channelIDs); err != nil {
		return err
	}

	err = msgio.ClearChannelCourierQueue(rc, channel)
	if err != nil {
		return err
	}

	err = models.FailChannelMessages(ctx, db, orgID, t.ChannelID)

	return err

}

// Timeout is the maximum amount of time the task can run for
func (*InterruptChannelTask) Timeout() time.Duration {
	return time.Hour
}
