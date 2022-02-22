package starts

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	startBatchSize = 100
)

func init() {
	mailroom.AddTaskFunction(queue.StartFlow, handleFlowStart)
	mailroom.AddTaskFunction(queue.StartFlowBatch, handleFlowStartBatch)
}

// handleFlowStart creates all the batches of contacts to start in a flow
func handleFlowStart(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*60)
	defer cancel()

	// decode our task body
	if task.Type != queue.StartFlow {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startTask := &models.FlowStart{}
	err := json.Unmarshal(task.Task, startTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start task: %s", string(task.Task))
	}

	err = CreateFlowBatches(ctx, rt, startTask)
	if err != nil {
		models.MarkStartFailed(ctx, rt.DB, startTask.ID())

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
	contactIDs := make(map[models.ContactID]bool)
	createdContactIDs := make([]models.ContactID, 0)

	// we are building a set of contact ids, start with the explicit ones
	for _, id := range start.ContactIDs() {
		contactIDs[id] = true
	}

	oa, err := models.GetOrgAssets(ctx, rt, start.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org assets")
	}

	// look up any contacts by URN
	if len(start.URNs()) > 0 {
		urnContactIDs, err := models.GetOrCreateContactIDsFromURNs(ctx, rt.DB, oa, start.URNs())
		if err != nil {
			return errors.Wrapf(err, "error getting contact ids from urns")
		}
		for _, id := range urnContactIDs {
			if !contactIDs[id] {
				createdContactIDs = append(createdContactIDs, id)
			}
			contactIDs[id] = true
		}
	}

	// if we are meant to create a new contact, do so
	if start.CreateContact() {
		contact, _, err := models.CreateContact(ctx, rt.DB, oa, models.NilUserID, "", envs.NilLanguage, nil)
		if err != nil {
			return errors.Wrapf(err, "error creating new contact")
		}
		contactIDs[contact.ID()] = true
		createdContactIDs = append(createdContactIDs, contact.ID())
	}

	// if we have inclusion groups, add all the contact ids from those groups
	if len(start.GroupIDs()) > 0 {
		rows, err := rt.DB.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(start.GroupIDs()))
		if err != nil {
			return errors.Wrapf(err, "error querying contacts from inclusion groups")
		}
		defer rows.Close()

		var contactID models.ContactID
		for rows.Next() {
			err := rows.Scan(&contactID)
			if err != nil {
				return errors.Wrapf(err, "error scanning contact id")
			}
			contactIDs[contactID] = true
		}
	}

	// if we have a query, add the contacts that match that as well
	if start.Query() != "" {
		// queries in start_session flow actions only match a single contact
		limit := -1
		if start.Type() == models.StartTypeFlowAction {
			limit = 1
		}
		matches, err := models.GetContactIDsForQuery(ctx, rt.ES, oa, start.Query(), limit)
		if err != nil {
			return errors.Wrapf(err, "error performing search for start: %d", start.ID())
		}

		for _, contactID := range matches {
			contactIDs[contactID] = true
		}
	}

	// finally, if we have exclusion groups, remove all the contact ids from those groups
	if len(start.ExcludeGroupIDs()) > 0 {
		rows, err := rt.DB.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(start.ExcludeGroupIDs()))
		if err != nil {
			return errors.Wrapf(err, "error querying contacts from exclusion groups")
		}
		defer rows.Close()

		var contactID models.ContactID
		for rows.Next() {
			err := rows.Scan(&contactID)
			if err != nil {
				return errors.Wrapf(err, "error scanning contact id")
			}
			delete(contactIDs, contactID)
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
	taskType := queue.StartFlowBatch
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
	for c := range contactIDs {
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

// HandleFlowStartBatch starts a batch of contacts in a flow
func handleFlowStartBatch(ctx context.Context, rt *runtime.Runtime, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	// decode our task body
	if task.Type != queue.StartFlowBatch {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startBatch := &models.FlowStartBatch{}
	err := json.Unmarshal(task.Task, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start batch: %s", string(task.Task))
	}

	// start these contacts in our flow
	_, err = runner.StartFlowBatch(ctx, rt, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error starting flow batch: %s", string(task.Task))
	}

	return err
}
