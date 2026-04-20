package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	tasks.RegisterCron("retry_errored_messages", false, &RetryMessagesCron{})
}

type RetryMessagesCron struct{}

func (c *RetryMessagesCron) Next(last time.Time) time.Time {
	return tasks.CronNext(last, time.Minute)
}

func (c *RetryMessagesCron) Run(ctx context.Context, rt *runtime.Runtime) (map[string]any, error) {
	rc := rt.RP.Get()
	defer rc.Close()

	msgs, err := models.GetMessagesForRetry(ctx, rt.DB)
	if err != nil {
		return nil, errors.Wrap(err, "error fetching errored messages to retry")
	}
	if len(msgs) == 0 {
		return nil, nil // nothing to retry
	}

	err = models.MarkMessagesQueued(ctx, rt.DB, msgs)
	if err != nil {
		return nil, errors.Wrap(err, "error marking messages as queued")
	}

	msgio.QueueMessages(ctx, rt, rt.DB, nil, msgs)

	return map[string]any{"retried": len(msgs)}, nil
}
