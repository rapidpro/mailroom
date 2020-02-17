package hooks

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeSessionTriggered, handleSessionTriggered)
}

// StartStartHook is our hook to fire our scene starts
type StartStartHook struct{}

var startStartHook = &StartStartHook{}

// InsertStartHook is our hook to fire insert our starts
type InsertStartHook struct{}

var insertStartHook = &InsertStartHook{}

// Apply queues up our flow starts
func (h *StartStartHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene map[*models.Scene][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	// for each of our scene
	for _, es := range scene {
		for _, e := range es {
			start := e.(*models.FlowStart)

			taskQ := queue.HandlerQueue
			priority := queue.DefaultPriority

			// if we are starting groups, queue to our batch queue instead, but with high priority
			if len(start.GroupIDs()) > 0 || start.Query() != "" {
				taskQ = queue.BatchQueue
				priority = queue.HighPriority
			}

			err := queue.AddTask(rc, taskQ, queue.StartFlow, int(org.OrgID()), start, priority)
			if err != nil {
				return errors.Wrapf(err, "error queuing flow start")
			}
		}
	}

	return nil
}

// Apply inserts our starts
func (h *InsertStartHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene map[*models.Scene][]interface{}) error {
	rc := rp.Get()
	defer rc.Close()

	starts := make([]*models.FlowStart, 0, len(scene))

	// for each of our scene
	for s, es := range scene {
		for _, e := range es {
			event := e.(*events.SessionTriggeredEvent)

			// we skip over any scene starts that involve groups if we are in a batch start
			if len(scene) > 1 && (len(event.Groups) > 0 || event.ContactQuery != "") {
				logrus.WithField("session_id", s.ID).Error("ignoring scene trigger on group or query in batch")
				continue
			}

			// look up our flow
			f, err := org.Flow(event.Flow.UUID)
			if err != nil {
				return errors.Wrapf(err, "unable to load flow with UUID: %s", event.Flow.UUID)
			}
			flow := f.(*models.Flow)

			// load our groups by uuid
			groupIDs := make([]models.GroupID, 0, len(event.Groups))
			for i := range event.Groups {
				group := org.GroupByUUID(event.Groups[i].UUID)
				if group != nil {
					groupIDs = append(groupIDs, group.ID())
				}
			}

			// load our contacts by uuid
			contactIDs, err := models.ContactIDsFromReferences(ctx, tx, org, event.Contacts)
			if err != nil {
				return errors.Wrapf(err, "error loading contacts by reference")
			}

			// create our start
			start := models.NewFlowStart(org.OrgID(), flow.FlowType(), flow.ID(), models.DoRestartParticipants, models.DoIncludeActive).
				WithGroupIDs(groupIDs).
				WithContactIDs(contactIDs).
				WithURNs(event.URNs).
				WithQuery(event.ContactQuery).
				WithCreateContact(event.CreateContact).
				WithParentSummary(event.RunSummary)

			starts = append(starts, start)

			// this will add our task for our start after we commit
			s.AddPostCommitEvent(startStartHook, start)
		}
	}

	// insert all our starts
	err := models.InsertFlowStarts(ctx, tx, starts)
	if err != nil {
		return errors.Wrapf(err, "error inserting flow starts for scene triggers")
	}

	return nil
}

// handleSessionTriggered queues this event for being started after our scene are committed
func handleSessionTriggered(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.SessionTriggeredEvent)

	// must be in a session
	if scene.Session() == nil {
		return errors.Errorf("cannot handle session triggered event without session")
	}

	logrus.WithFields(logrus.Fields{
		"contact_uuid": scene.ContactUUID(),
		"session_id":   scene.ID(),
		"flow":         event.Flow.Name,
		"flow_uuid":    event.Flow.UUID,
	}).Debug("scene triggered")

	scene.AddPreCommitEvent(insertStartHook, event)

	return nil
}
