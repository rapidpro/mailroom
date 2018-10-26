package starts

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nyaruka/gocommon/urns"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/nyaruka/mailroom/runner"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	startFlowType      = "start_flow"
	startFlowBatchType = "start_flow_batch"
	startBatchSize     = 100
)

func init() {
	mailroom.AddTaskFunction(startFlowType, handleFlowStart)
	mailroom.AddTaskFunction(startFlowBatchType, handleFlowStartBatch)
}

// handleFlowStart creates all the batches of contacts to start in a flow
func handleFlowStart(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*60)
	defer cancel()

	// decode our task body
	if task.Type != startFlowType {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startTask := &models.FlowStart{}
	err := json.Unmarshal(task.Task, startTask)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start task: %s", string(task.Task))
	}

	return CreateFlowBatches(ctx, mr.DB, mr.RP, startTask)
}

// CreateFlowBatches takes our master flow start and creates batches of flow starts for all the unique contacts
func CreateFlowBatches(ctx context.Context, db *sqlx.DB, rp *redis.Pool, start *models.FlowStart) error {
	// we are building a set of contact ids, start with the explicit ones
	contactIDs := make(map[flows.ContactID]bool)
	for _, id := range start.ContactIDs() {
		contactIDs[id] = true
	}

	var org *models.OrgAssets
	var assets flows.SessionAssets
	var err error

	// look up any contacts by URN
	if len(start.URNs()) > 0 {
		org, err = models.GetOrgAssets(ctx, db, start.OrgID())
		if err != nil {
			return errors.Wrapf(err, "error loading org assets")
		}
		assets, err = models.GetSessionAssets(org)
		if err != nil {
			return errors.Wrapf(err, "error loading session assets")
		}

		urnContactIDs, err := models.ContactIDsFromURNs(ctx, db, org, assets, start.URNs())
		if err != nil {
			return errors.Wrapf(err, "error getting contact ids from urns")
		}
		for _, id := range urnContactIDs {
			contactIDs[id] = true
		}
	}

	// if we are meant to create a new contact, do so
	if start.CreateContact() {
		if org == nil {
			org, err = models.GetOrgAssets(ctx, db, start.OrgID())
			if err != nil {
				return errors.Wrapf(err, "error loading org assets")
			}
			assets, err = models.GetSessionAssets(org)
			if err != nil {
				return errors.Wrapf(err, "error loading session assets")
			}
		}

		newID, err := models.CreateContact(ctx, db, org, assets, urns.NilURN)
		if err != nil {
			return errors.Wrapf(err, "error creating new contact")
		}
		contactIDs[newID] = true
	}

	// now add all the ids for our groups
	rows, err := db.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(start.GroupIDs()))
	if err != nil {
		return errors.Wrapf(err, "error selecting contacts for groups")
	}
	defer rows.Close()

	var contactID flows.ContactID
	for rows.Next() {
		err := rows.Scan(&contactID)
		if err != nil {
			return errors.Wrapf(err, "error scanning contact id")
		}
		contactIDs[contactID] = true
	}

	rc := rp.Get()
	defer rc.Close()

	// build up batches of contacts to start
	contacts := make([]flows.ContactID, 0, 100)
	for c := range contactIDs {
		if len(contacts) == startBatchSize {
			batch := start.CreateBatch(contacts)
			err = queue.AddTask(rc, mailroom.BatchQueue, startFlowBatchType, int(start.OrgID()), batch, queue.DefaultPriority)
			if err != nil {
				// TODO: is continuing the right thing here? what do we do if redis is down? (panic!)
				logrus.WithError(err).WithField("start_id", start.StartID).Error("error while queuing start")
			}
			contacts = make([]flows.ContactID, 0, 100)
		}
		contacts = append(contacts, c)
	}

	// queue our last batch
	if len(contacts) > 0 {
		batch := start.CreateBatch(contacts)
		batch.SetIsLast(true)
		err = queue.AddTask(rc, mailroom.BatchQueue, startFlowBatchType, int(start.OrgID()), batch, queue.DefaultPriority)
		if err != nil {
			logrus.WithError(err).WithField("start_id", start.StartID).Error("error while queuing start")
		}
	}

	// mark our start as started
	err = models.MarkStartStarted(ctx, db, start.StartID(), len(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error marking start as started")
	}

	return nil
}

// HandleFlowStartBatch starts a batch of contacts in a flow
func handleFlowStartBatch(ctx context.Context, mr *mailroom.Mailroom, task *queue.Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// decode our task body
	if task.Type != startFlowBatchType {
		return errors.Errorf("unknown event type passed to start worker: %s", task.Type)
	}
	startBatch := &models.FlowStartBatch{}
	err := json.Unmarshal(task.Task, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error unmarshalling flow start batch: %s", string(task.Task))
	}

	// start these contacts in our flow
	_, err = runner.StartFlowBatch(ctx, mr.DB, mr.RP, startBatch)
	if err != nil {
		return errors.Wrapf(err, "error starting flow batch")
	}

	return err
}
