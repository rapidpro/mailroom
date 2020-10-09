package ivr

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(queue.StartIVRFlowBatch, handleFlowStartTask)
}

func handleFlowStartTask(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	// decode our task body
	if task.Type != queue.StartIVRFlowBatch {
		return errors.Errorf("unknown event type passed to ivr worker: %s", task.Type)
	}
	batch := &models.FlowStartBatch{}
	err := json.Unmarshal(task.Task, batch)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start batch: %s", string(task.Task))
	}

	return HandleFlowStartBatch(ctx, mr.Config, mr.DB, mr.RP, batch)
}

// HandleFlowStartBatch starts a batch of contacts in an IVR flow
func HandleFlowStartBatch(bg context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, batch *models.FlowStartBatch) error {
	ctx, cancel := context.WithTimeout(bg, time.Minute*5)
	defer cancel()

	// contacts we will exclude either because they are in a flow or have already been in this one
	exclude := make(map[models.ContactID]bool, 5)

	// filter out anybody who has has a flow run in this flow if appropriate
	if !batch.RestartParticipants() {
		// find all participants that have been in this flow
		started, err := models.FindFlowStartedOverlap(ctx, db, batch.FlowID(), batch.ContactIDs())
		if err != nil {
			return errors.Wrapf(err, "error finding others started flow: %d", batch.FlowID())
		}
		for _, c := range started {
			exclude[c] = true
		}
	}

	// filter out our list of contacts to only include those that should be started
	if !batch.IncludeActive() {
		// find all participants active in other sessions
		active, err := models.FindActiveSessionOverlap(ctx, db, models.IVRFlow, batch.ContactIDs())
		if err != nil {
			return errors.Wrapf(err, "error finding other active sessions: %d", batch.FlowID())
		}
		for _, c := range active {
			exclude[c] = true
		}
	}

	// filter into our final list of contacts
	contactIDs := make([]models.ContactID, 0, len(batch.ContactIDs()))
	for _, c := range batch.ContactIDs() {
		if !exclude[c] {
			contactIDs = append(contactIDs, c)
		}
	}

	// load our org assets
	oa, err := models.GetOrgAssets(ctx, db, batch.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org assets for org: %d", batch.OrgID())
	}

	// ok, we can initiate calls for the remaining contacts
	contacts, err := models.LoadContacts(ctx, db, oa, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error loading contacts")
	}

	// for each contacts, request a call start
	for _, contact := range contacts {
		start := time.Now()

		ctx, cancel := context.WithTimeout(bg, time.Minute)
		session, err := ivr.RequestCallStart(ctx, config, db, oa, batch, contact)
		cancel()
		if err != nil {
			logrus.WithError(err).Errorf("error starting ivr flow for contact: %d and flow: %d", contact.ID(), batch.FlowID())
			continue
		}
		if session == nil {
			logrus.WithFields(logrus.Fields{
				"elapsed":    time.Since(start),
				"contact_id": contact.ID(),
				"start_id":   batch.StartID(),
			}).Info("call start skipped, no suitable channel")
			continue
		}
		logrus.WithFields(logrus.Fields{
			"elapsed":     time.Since(start),
			"contact_id":  contact.ID(),
			"status":      session.Status(),
			"start_id":    batch.StartID(),
			"external_id": session.ExternalID(),
		}).Info("requested call for contact")
	}

	// if this is a last batch, mark our start as started
	if batch.IsLast() {
		err := models.MarkStartComplete(bg, db, batch.StartID())
		if err != nil {
			return errors.Wrapf(err, "error trying to set batch as complete")
		}
	}

	return nil
}
