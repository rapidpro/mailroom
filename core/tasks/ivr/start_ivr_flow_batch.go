package ivr

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const TypeStartIVRFlowBatch = "start_ivr_flow_batch"

func init() {
	tasks.RegisterType(TypeStartIVRFlowBatch, func() tasks.Task { return &StartIVRFlowBatchTask{} })
}

// StartIVRFlowBatchTask is the start IVR flow batch task
type StartIVRFlowBatchTask struct {
	*models.FlowStartBatch
}

func (t *StartIVRFlowBatchTask) Type() string {
	return TypeStartIVRFlowBatch
}

// Timeout is the maximum amount of time the task can run for
func (t *StartIVRFlowBatchTask) Timeout() time.Duration {
	return time.Minute * 5
}

func (t *StartIVRFlowBatchTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	return handleFlowStartBatch(ctx, rt, t.FlowStartBatch)
}

// starts a batch of contacts in an IVR flow
func handleFlowStartBatch(ctx context.Context, rt *runtime.Runtime, batch *models.FlowStartBatch) error {
	// load our org assets
	oa, err := models.GetOrgAssets(ctx, rt, batch.OrgID)
	if err != nil {
		return errors.Wrapf(err, "error loading org assets for org: %d", batch.OrgID)
	}

	// ok, we can initiate calls for the remaining contacts
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, batch.ContactIDs)
	if err != nil {
		return errors.Wrapf(err, "error loading contacts")
	}

	// for each contacts, request a call start
	for _, contact := range contacts {
		start := time.Now()

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		session, err := ivr.RequestCall(ctx, rt, oa, batch, contact)
		cancel()
		if err != nil {
			logrus.WithError(err).Errorf("error starting ivr flow for contact: %d and flow: %d", contact.ID(), batch.FlowID)
			continue
		}
		if session == nil {
			logrus.WithFields(logrus.Fields{
				"elapsed":    time.Since(start),
				"contact_id": contact.ID(),
				"start_id":   batch.StartID,
			}).Info("call start skipped, no suitable channel")
			continue
		}
		logrus.WithFields(logrus.Fields{
			"elapsed":     time.Since(start),
			"contact_id":  contact.ID(),
			"status":      session.Status(),
			"start_id":    batch.StartID,
			"external_id": session.ExternalID(),
		}).Info("requested call for contact")
	}

	// if this is a last batch, mark our start as started
	if batch.IsLast {
		err := models.MarkStartComplete(ctx, rt.DB, batch.StartID)
		if err != nil {
			return errors.Wrapf(err, "error trying to set batch as complete")
		}
	}

	return nil
}
