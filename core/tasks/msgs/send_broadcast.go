package msgs

import (
	"context"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
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

// Timeout is the maximum amount of time the task can run for
func (t *SendBroadcastTask) Timeout() time.Duration {
	return time.Minute * 60
}

// Perform handles sending the broadcast by creating batches of broadcast sends for all the unique contacts
func (t *SendBroadcastTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	// we are building a set of contact ids, start with the explicit ones
	contactIDs := make(map[models.ContactID]bool)
	for _, id := range t.Broadcast.ContactIDs {
		contactIDs[id] = true
	}

	groupContactIDs, err := models.ContactIDsForGroupIDs(ctx, rt.DB, t.Broadcast.GroupIDs)
	for _, id := range groupContactIDs {
		contactIDs[id] = true
	}

	oa, err := models.GetOrgAssets(ctx, rt, t.Broadcast.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error getting org assets")
	}

	// get the contact ids for our URNs
	urnMap, err := models.GetOrCreateContactIDsFromURNs(ctx, rt.DB, oa, t.Broadcast.URNs)
	if err != nil {
		return errors.Wrapf(err, "error getting contact ids for urns")
	}

	urnContacts := make(map[models.ContactID]urns.URN)
	repeatedContacts := make(map[models.ContactID]urns.URN)

	q := queue.BatchQueue

	// two or fewer contacts? queue to our handler queue for sending
	if len(contactIDs) <= 2 {
		q = queue.HandlerQueue
	}

	// we want to remove contacts that are also present in URN sends, these will be a special case in our last batch
	for u, id := range urnMap {
		if contactIDs[id] {
			repeatedContacts[id] = u
			delete(contactIDs, id)
		}
		urnContacts[id] = u
	}

	rc := rt.RP.Get()
	defer rc.Close()

	contacts := make([]models.ContactID, 0, 100)

	// utility functions for queueing the current set of contacts
	queueBatch := func(isLast bool) {
		// if this is our last batch include those contacts that overlap with our urns
		if isLast {
			for id := range repeatedContacts {
				contacts = append(contacts, id)
			}
		}

		batch := t.Broadcast.CreateBatch(contacts)

		// also set our URNs
		if isLast {
			batch.IsLast = true
			batch.URNs = urnContacts
		}

		err = queue.AddTask(rc, q, TypeSendBroadcastBatch, int(t.Broadcast.OrgID), batch, queue.DefaultPriority)
		if err != nil {
			logrus.WithError(err).Error("error while queuing broadcast batch")
		}
		contacts = make([]models.ContactID, 0, 100)
	}

	// build up batches of contacts to start
	for c := range contactIDs {
		if len(contacts) == startBatchSize {
			queueBatch(false)
		}
		contacts = append(contacts, c)
	}

	// queue our last batch
	queueBatch(true)

	return nil
}
