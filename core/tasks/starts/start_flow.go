package starts

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	TypeStartFlow = "start_flow"

	startBatchSize = 100
)

func init() {
	tasks.RegisterType(TypeStartFlow, func() tasks.Task { return &StartFlowTask{} })
}

// StartFlowBatchTask is the start flow batch task
type StartFlowTask struct {
	*models.FlowStart
}

func (t *StartFlowTask) Type() string {
	return TypeStartFlow
}

// Timeout is the maximum amount of time the task can run for
func (t *StartFlowTask) Timeout() time.Duration {
	return time.Minute * 60
}

func (t *StartFlowTask) Perform(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID) error {
	if err := createFlowStartBatches(ctx, rt, t.FlowStart); err != nil {
		models.MarkStartFailed(ctx, rt.DB, t.FlowStart.ID)

		// if error is user created query error.. don't escalate error to sentry
		isQueryError, _ := contactql.IsQueryError(err)
		if !isQueryError {
			return err
		}
	}

	return nil
}

// creates batches of flow starts for all the unique contacts
func createFlowStartBatches(ctx context.Context, rt *runtime.Runtime, start *models.FlowStart) error {
	oa, err := models.GetOrgAssets(ctx, rt, start.OrgID)
	if err != nil {
		return errors.Wrap(err, "error loading org assets")
	}

	flow, err := oa.FlowByID(start.FlowID)
	if err != nil {
		return errors.Wrap(err, "error loading flow")
	}

	var contactIDs []models.ContactID

	if start.CreateContact {
		// if we are meant to create a new contact, do so
		contact, _, err := models.CreateContact(ctx, rt.DB, oa, models.NilUserID, "", envs.NilLanguage, nil)
		if err != nil {
			return errors.Wrapf(err, "error creating new contact")
		}
		contactIDs = []models.ContactID{contact.ID()}
	} else {
		// otherwise resolve recipients across contacts, groups, urns etc

		// queries in start_session flow actions only match a single contact
		limit := -1
		if string(start.Query) != "" && start.StartType == models.StartTypeFlowAction {
			limit = 1
		}

		contactIDs, err = search.ResolveRecipients(ctx, rt, oa, flow, &search.Recipients{
			ContactIDs:      start.ContactIDs,
			GroupIDs:        start.GroupIDs,
			URNs:            start.URNs,
			Query:           string(start.Query),
			ExcludeGroupIDs: start.ExcludeGroupIDs,
		}, limit)
		if err != nil {
			return errors.Wrap(err, "error resolving start recipients")
		}
	}

	// mark our start as starting, last task will mark as complete
	err = models.MarkStartStarted(ctx, rt.DB, start.ID, len(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error marking start as started")
	}

	// if there are no contacts to start, mark our start as complete, we are done
	if len(contactIDs) == 0 {
		err = models.MarkStartComplete(ctx, rt.DB, start.ID)
		if err != nil {
			return errors.Wrapf(err, "error marking start as complete")
		}
		return nil
	}

	// by default we start in the batch queue unless we have two or fewer contacts
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

		batch := start.CreateBatch(idBatch, isLast, len(contactIDs))

		// task is different if we are an IVR flow
		var batchTask tasks.Task
		if start.FlowType == models.FlowTypeVoice {
			batchTask = &ivr.StartIVRFlowBatchTask{FlowStartBatch: batch}
		} else {
			batchTask = &StartFlowBatchTask{FlowStartBatch: batch}
		}

		err = tasks.Queue(rc, q, start.OrgID, batchTask, queue.DefaultPriority)
		if err != nil {
			if i == 0 {
				return errors.Wrap(err, "error queuing flow start batch")
			}
			// if we've already queued other batches.. we don't want to error and have the task be retried
			logrus.WithError(err).Error("error queuing flow start batch")
		}
	}

	return nil
}
