package ivr

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	mailroom.AddTaskFunction(mailroom.StartIVRFlowBatchType, handleFlowStartBatch)
}

// HandleFlowStartBatch starts a batch of contacts in an IVR flow
func handleFlowStartBatch(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// decode our task body
	if task.Type != mailroom.StartIVRFlowBatchType {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startBatch := &models.FlowStartBatch{}
	err := json.Unmarshal(task.Task, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start batch: %s", string(task.Task))
	}

	// contacts we will exclude either because they are in a flow or already been in this one
	exclude := make(map[flows.ContactID]bool, 5)

	// filter out anybody who has has a flow run in this flow if appropriate
	if !startBatch.RestartParticipants() {
		// find all participants that have been in this flow
		started, err := models.FindFlowStartedOverlap(ctx, mr.DB, startBatch.FlowID(), startBatch.ContactIDs())
		if err != nil {
			return errors.Wrapf(err, "error finding others started flow: %d", startBatch.FlowID())
		}
		for _, c := range started {
			exclude[c] = true
		}
	}

	// filter out our list of contacts to only include those that should be started
	if !startBatch.IncludeActive() {
		// find all participants active in any flow
		active, err := models.FindActiveRunOverlap(ctx, mr.DB, startBatch.ContactIDs())
		if err != nil {
			return errors.Wrapf(err, "error finding other active flow: %d", startBatch.FlowID())
		}
		for _, c := range active {
			exclude[c] = true
		}
	}

	// filter into our final list of contacts
	contactIDs := make([]flows.ContactID, 0, len(startBatch.ContactIDs()))
	for _, c := range startBatch.ContactIDs() {
		if !exclude[c] {
			contactIDs = append(contactIDs, c)
		}
	}

	// load our org assets
	org, err := models.GetOrgAssets(ctx, mr.DB, startBatch.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org assets for org: %d", startBatch.OrgID())
	}

	// ok, we can initiate calls for the remaining contacts
	contacts, err := models.LoadContacts(ctx, mr.DB, org, contactIDs)
	if err != nil {
		return errors.Wrapf(err, "error loading contacts")
	}

	// for each contacts, request a call start
	for _, contact := range contacts {
		start := time.Now()
		session, err := RequestCallStart(ctx, mr.Config, mr.DB, org, startBatch, contact)
		if err != nil {
			logrus.WithError(err).Errorf("error starting ivr flow for contact: %d and flow: %d", contact.ID(), startBatch.FlowID())
			continue
		}
		logrus.WithFields(logrus.Fields{
			"elapsed":     time.Since(start),
			"contact_id":  contact.ID(),
			"status":      session.Status(),
			"start_id":    startBatch.StartID().Int64,
			"external_id": session.ExternalID(),
		}).Debug("requested call for contact")
	}

	// if this is a last batch, mark our start as started
	if startBatch.IsLast() {
		err := models.MarkStartComplete(ctx, mr.DB, startBatch.StartID())
		if err != nil {
			return errors.Wrapf(err, "error trying to set batch as complete")
		}
	}

	return nil
}
