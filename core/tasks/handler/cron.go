package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var retriedMsgs = redisx.NewIntervalSet("retried_msgs", time.Hour*24, 2)

func init() {
	mailroom.RegisterCron("retry_msgs", time.Minute*5, false, RetryPendingMsgs)
}

// RetryPendingMsgs looks for any pending msgs older than five minutes and queues them to be handled again
func RetryPendingMsgs(ctx context.Context, rt *runtime.Runtime) error {
	if !rt.Config.RetryPendingMessages {
		return nil
	}

	log := logrus.WithField("comp", "handler_retrier")
	start := time.Now()

	rc := rt.RP.Get()
	defer rc.Close()

	// check the size of our handle queue
	handlerSize, err := queue.Size(rc, queue.HandlerQueue)
	if err != nil {
		return errors.Wrapf(err, "error finding size of handler queue")
	}

	// if our queue has items in it, don't queue anything else in there, wait for it to be empty
	if handlerSize > 0 {
		log.Info("not retrying any messages, have messages in handler queue")
		return nil
	}

	// get all incoming messages that are still empty
	rows, err := rt.DB.Queryx(unhandledMsgsQuery)
	if err != nil {
		return errors.Wrapf(err, "error querying for unhandled messages")
	}
	defer rows.Close()

	retried := 0
	for rows.Next() {
		var orgID models.OrgID
		var contactID models.ContactID
		var eventJSON string
		var msgID models.MsgID

		err = rows.Scan(&orgID, &contactID, &msgID, &eventJSON)
		if err != nil {
			return errors.Wrapf(err, "error scanning msg row")
		}

		// our key is built such that we will only retry once an hour
		key := fmt.Sprintf("%d_%d", msgID, time.Now().Hour())

		dupe, err := retriedMsgs.Contains(rc, key)
		if err != nil {
			return errors.Wrapf(err, "error checking for dupe retry")
		}

		// we already retried this, skip
		if dupe {
			continue
		}

		task := &queue.Task{
			Type:     MsgEventType,
			OrgID:    int(orgID),
			Task:     json.RawMessage(eventJSON),
			QueuedOn: time.Now(),
		}

		// queue this event up for handling
		err = QueueHandleTask(rc, contactID, task)
		if err != nil {
			return errors.Wrapf(err, "error queuing retry for task")
		}

		// mark it as queued
		err = retriedMsgs.Add(rc, key)
		if err != nil {
			return errors.Wrapf(err, "error marking task for retry")
		}

		retried++
	}

	log.WithField("retried", retried).WithField("elapsed", time.Since(start)).Info("queued pending messages to be retried")
	return nil
}

const unhandledMsgsQuery = `
SELECT org_id, contact_id, msg_id, ROW_TO_JSON(r) FROM (SELECT
	m.contact_id as contact_id,
	m.org_id as org_id, 
	m.channel_id as channel_id,
	m.id as msg_id,
	m.uuid as msg_uuid,
	m.external_id as msg_external_id,
	u.identity as urn,
	m.contact_urn_id as urn_id,
	m.text as text,
	m.attachments as attachments
FROM
	msgs_msg m
	JOIN contacts_contacturn as u ON m.contact_urn_id = u.id
WHERE
	m.direction = 'I' AND
	m.status = 'P' AND
	m.created_on < now() - INTERVAL '5 min'
) r;
`
