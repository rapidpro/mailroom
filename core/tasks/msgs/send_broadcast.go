package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// TypeSendBroadcast is the task type for sending a broadcast
	TypeSendBroadcast = "send_broadcast"

	startBatchSize = 100
)

func init() {
	tasks.RegisterType(TypeSendBroadcast, func() tasks.Task { return &SendBroadcastTask{} })
}

// SendBroadcastTask is the task send broadcasts
type SendBroadcastTask struct {
	*models.Broadcast
}

func (t *SendBroadcastTask) Type() string {
	return TypeSendBroadcast
}

// Timeout is the maximum amount of time the task can run for
func (t *SendBroadcastTask) Timeout() time.Duration {
	return time.Minute * 60
}

// Perform handles sending the broadcast by creating batches of broadcast sends for all the unique contacts
func (t *SendBroadcastTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	oa, err := models.GetOrgAssets(ctx, rt, t.Broadcast.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error getting org assets")
	}

	contactIDs, _, err := search.ResolveRecipients(ctx, rt, oa, &search.Recipients{
		ContactIDs:      t.Broadcast.ContactIDs,
		GroupIDs:        t.Broadcast.GroupIDs,
		URNs:            t.Broadcast.URNs,
		Query:           string(t.Broadcast.Query),
		QueryLimit:      -1,
		ExcludeGroupIDs: nil,
	})
	if err != nil {
		return errors.Wrap(err, "error resolving broadcast recipients")
	}

	// if there are no contacts to send to, mark our broadcast as sent, we are done
	if len(contactIDs) == 0 {
		err = models.MarkBroadcastSent(ctx, rt.DB, t.Broadcast.ID)
		if err != nil {
			return errors.Wrapf(err, "error marking broadcast as sent")
		}
		return nil
	}

	// two or fewer contacts? queue to our handler queue for sending
	q := queue.BatchQueue
	if len(contactIDs) <= 2 {
		q = queue.HandlerQueue
	}

	rc := rt.RP.Get()
	defer rc.Close()

	// create tasks for batches of contacts
	idBatches := models.ChunkSlice(contactIDs, startBatchSize)
	for i, idBatch := range idBatches {
		isLast := (i == len(idBatches)-1)

		batch := t.Broadcast.CreateBatch(idBatch, isLast)
		err = tasks.Queue(rc, q, t.Broadcast.OrgID, &SendBroadcastBatchTask{BroadcastBatch: batch}, queue.DefaultPriority)
		if err != nil {
			if i == 0 {
				return errors.Wrap(err, "error queuing broadcast batch")
			}
			// if we've already queued other batches.. we don't want to error and have the task be retried
			logrus.WithError(err).Error("error queuing broadcast batch")
		}
	}

	return nil
}
