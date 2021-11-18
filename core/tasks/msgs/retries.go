package msgs

import (
	"context"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/cron"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	retryMessagesLock = "retry_errored_messages"
	celeryLock        = "celery-task-lock:retry_errored_messages"
)

func init() {
	mailroom.AddInitFunction(startCrons)
}

func startCrons(rt *runtime.Runtime, wg *sync.WaitGroup, quit chan bool) error {
	cron.StartCron(quit, rt.RP, retryMessagesLock, time.Second*60,
		func(lockName string, lockValue string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			return RetryErroredMessages(ctx, rt)
		},
	)

	return nil
}

func RetryErroredMessages(ctx context.Context, rt *runtime.Runtime) error {
	rc := rt.RP.Get()
	defer rc.Close()

	start := time.Now()

	// RapidPro has a retry task and until it is removed we need to ensure we're not running on top of it
	// otherwise errored messages could be retried twice
	result, err := rc.Do("SET", celeryLock, uuids.New(), "NX")
	if err != nil {
		return errors.Wrap(err, "error trying to aquire celery task lock")
	}
	if result == nil { // nil result means SET NX failed because celery is currently running that task
		return nil
	}
	defer rc.Do("DEL", celeryLock)

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
