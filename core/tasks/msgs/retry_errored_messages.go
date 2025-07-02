package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.RegisterCron("retry_errored_messages", time.Second*60, false, RetryErroredMessages)
}

func RetryErroredMessages(ctx context.Context, rt *runtime.Runtime) error {
	rc := rt.RP.Get()
	defer rc.Close()

	start := time.Now()

	msgs, err := models.GetMessagesForRetry(ctx, rt.DB)
	if err != nil {
		return errors.Wrap(err, "error fetching errored messages to retry")
	}
	if len(msgs) == 0 {
		return nil // nothing to retry
	}

	err = models.MarkMessagesQueued(ctx, rt.DB, msgs)
	if err != nil {
		return errors.Wrap(err, "error marking messages as queued")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, msgs)

	logrus.WithField("count", len(msgs)).WithField("elapsed", time.Since(start)).Info("retried errored messages")

	return nil
}
