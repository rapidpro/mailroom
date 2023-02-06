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
	err := CreateFlowBatches(ctx, rt, t.FlowStart)
	if err != nil {
		models.MarkStartFailed(ctx, rt.DB, t.FlowStart.ID())

		// if error is user created query error.. don't escalate error to sentry
		isQueryError, _ := contactql.IsQueryError(err)
		if !isQueryError {
			return err
		}
	}

	return nil
}

// CreateFlowBatches takes our master flow start and creates batches of flow starts for all the unique contacts
func CreateFlowBatches(ctx context.Context, rt *runtime.Runtime, start *models.FlowStart) error {
	oa, err := models.GetOrgAssets(ctx, rt, start.OrgID())
	if err != nil {
		return errors.Wrap(err, "error loading org assets")
	}

	var contactIDs, createdContactIDs []models.ContactID

	if start.CreateContact() {
		// if we are meant to create a new contact, do so
		contact, _, err := models.CreateContact(ctx, rt.DB, oa, models.NilUserID, "", envs.NilLanguage, nil)
		if err != nil {
			return errors.Wrapf(err, "error creating new contact")
		}
		contactIDs = []models.ContactID{contact.ID()}
		createdContactIDs = []models.ContactID{contact.ID()}
	} else {
		// otherwise resolve recipients across contacts, groups, urns etc

		// queries in start_session flow actions only match a single contact
		queryLimit := -1
		if start.Type() == models.StartTypeFlowAction {
			queryLimit = 1
		}

		contactIDs, createdContactIDs, err = search.ResolveRecipients(ctx, rt, oa, &search.Recipients{
			ContactIDs:      start.ContactIDs(),
			GroupIDs:        start.GroupIDs(),
			URNs:            start.URNs(),
			Query:           start.Query(),
			QueryLimit:      queryLimit,
			ExcludeGroupIDs: start.ExcludeGroupIDs(),
		})
		if err != nil {
			return errors.Wrap(err, "error resolving start recipients")
		}
	}

	rc := rt.RP.Get()
	defer rc.Close()

	// mark our start as starting, last task will mark as complete
	err = models.MarkStartStarted(ctx, rt.DB, start.ID(), len(contactIDs), createdContactIDs)
	if err != nil {
		return errors.Wrapf(err, "error marking start as started")
	}

	// if there are no contacts to start, mark our start as complete, we are done
	if len(contactIDs) == 0 {
		err = models.MarkStartComplete(ctx, rt.DB, start.ID())
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

	// task is different if we are an IVR flow
	taskType := TypeStartFlowBatch
	if start.FlowType() == models.FlowTypeVoice {
		taskType = queue.StartIVRFlowBatch
	}

	contacts := make([]models.ContactID, 0, 100)
	queueBatch := func(last bool) {
		batch := start.CreateBatch(contacts, last, len(contactIDs))
		err = queue.AddTask(rc, q, taskType, int(start.OrgID()), batch, queue.DefaultPriority)
		if err != nil {
			// TODO: is continuing the right thing here? what do we do if redis is down? (panic!)
			logrus.WithError(err).WithField("start_id", start.ID()).Error("error while queuing start")
		}
		contacts = make([]models.ContactID, 0, 100)
	}

	// build up batches of contacts to start
	for _, c := range contactIDs {
		if len(contacts) == startBatchSize {
			queueBatch(false)
		}
		contacts = append(contacts, c)
	}

	// queue our last batch
	if len(contacts) > 0 {
		queueBatch(true)
	}

	return nil
}
