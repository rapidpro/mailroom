package runner

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/locker"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	commitTimeout     = time.Minute
	postCommitTimeout = time.Minute
)

var startTypeToOrigin = map[models.StartType]string{
	models.StartTypeManual:    "ui",
	models.StartTypeAPI:       "api",
	models.StartTypeAPIZapier: "zapier",
}

// NewStartOptions creates and returns the default start options to be used for flow starts
func NewStartOptions() *StartOptions {
	start := &StartOptions{
		RestartParticipants: true,
		IncludeActive:       true,
		Interrupt:           true,
	}
	return start
}

// StartOptions define the various parameters that can be used when starting a flow
type StartOptions struct {
	// RestartParticipants should be true if the flow start should restart participants already in this flow
	RestartParticipants models.RestartParticipants

	// IncludeActive should be true if we want to interrupt people active in other flows (including this one)
	IncludeActive models.IncludeActive

	// Interrupt should be true if we want to interrupt the flows runs for any contact started in this flow
	Interrupt bool

	// CommitHook is the hook that will be called in the transaction where each session is written
	CommitHook models.SessionCommitHook

	// TriggerBuilder is the builder that will be used to build a trigger for each contact started in the flow
	TriggerBuilder TriggerBuilder
}

// TriggerBuilder defines the interface for building a trigger for the passed in contact
type TriggerBuilder func(contact *flows.Contact) flows.Trigger

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, session *models.Session, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()
	sa := oa.SessionAssets()

	// does the flow this session is part of still exist?
	_, err := oa.FlowByID(session.CurrentFlowID())
	if err != nil {
		// if this flow just isn't available anymore, log this error
		if err == models.ErrNotFound {
			logrus.WithField("contact_uuid", session.Contact().UUID()).WithField("session_id", session.ID()).WithField("flow_id", session.CurrentFlowID()).Error("unable to find flow in resume")
			return nil, models.ExitSessions(ctx, rt.DB, []models.SessionID{session.ID()}, models.SessionStatusFailed)
		}
		return nil, errors.Wrapf(err, "error loading session flow: %d", session.CurrentFlowID())
	}

	// build our flow session
	fs, err := session.FlowSession(rt.Config, sa, oa.Env())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create session from output")
	}

	// resume our session
	resumeStart := time.Now()
	sprint, err := fs.Resume(resume)
	logrus.WithField("contact_id", resume.Contact().ID()).WithField("elapsed", time.Since(resumeStart)).Info("engine resume complete")

	// had a problem resuming our flow? bail
	if err != nil {
		return nil, errors.Wrapf(err, "error resuming flow")
	}

	// write our updated session, applying any events in the process
	txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()

	tx, err := rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction")
	}

	// write our updated session and runs
	err = session.Update(txCTX, rt, tx, oa, fs, sprint, hook)
	if err != nil {
		tx.Rollback()
		return nil, errors.Wrapf(err, "error updating session for resume")
	}

	// commit at once
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, errors.Wrapf(err, "error committing resumption of flow")
	}

	// now take care of any post-commit hooks
	txCTX, cancel = context.WithTimeout(ctx, postCommitTimeout)
	defer cancel()

	tx, err = rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction for post commit hooks")
	}

	err = models.ApplyEventPostCommitHooks(txCTX, rt, tx, oa, []*models.Scene{session.Scene()})
	if err == nil {
		err = tx.Commit()
	}

	if err != nil {
		tx.Rollback()
		return nil, errors.Wrapf(err, "error committing session changes on resume")
	}
	logrus.WithField("contact_uuid", resume.Contact().UUID()).WithField("elapsed", time.Since(start)).Info("resumed session")

	return session, nil
}

// StartFlowBatch starts the flow for the passed in org, contacts and flow
func StartFlowBatch(
	ctx context.Context, rt *runtime.Runtime,
	batch *models.FlowStartBatch) ([]*models.Session, error) {

	start := time.Now()

	// if this is our last start, no matter what try to set the start as complete as a last step
	if batch.IsLast() {
		defer func() {
			err := models.MarkStartComplete(ctx, rt.DB, batch.StartID())
			if err != nil {
				logrus.WithError(err).WithField("start_id", batch.StartID).Error("error marking start as complete")
			}
		}()
	}

	// create our org assets
	oa, err := models.GetOrgAssets(ctx, rt, batch.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", batch.OrgID())
	}

	// try to load our flow
	flow, err := oa.FlowByID(batch.FlowID())
	if err == models.ErrNotFound {
		logrus.WithField("flow_id", batch.FlowID()).Info("skipping flow start, flow no longer active or archived")
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %d", batch.FlowID())
	}

	// get the user that created this flow start if there was one
	var flowUser *flows.User
	if batch.CreatedByID() != models.NilUserID {
		user := oa.UserByID(batch.CreatedByID())
		if user != nil {
			flowUser = oa.SessionAssets().Users().Get(user.Email())
		}
	}

	var params *types.XObject
	if len(batch.Extra()) > 0 {
		params, err = types.ReadXObject(batch.Extra())
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start extra")
		}
	}

	var history *flows.SessionHistory
	if len(batch.SessionHistory()) > 0 {
		history, err = models.ReadSessionHistory(batch.SessionHistory())
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start history")
		}
	}

	// whether engine allows some functions is based on whether there is more than one contact being started
	batchStart := batch.TotalContacts() > 1

	// this will build our trigger for each contact started
	triggerBuilder := func(contact *flows.Contact) flows.Trigger {
		if batch.ParentSummary() != nil {
			tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).FlowAction(history, batch.ParentSummary())
			if batchStart {
				tb = tb.AsBatch()
			}
			return tb.Build()
		}

		tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).Manual()
		if batch.Extra() != nil {
			tb = tb.WithParams(params)
		}
		if batchStart {
			tb = tb.AsBatch()
		}
		return tb.WithUser(flowUser).WithOrigin(startTypeToOrigin[batch.StartType()]).Build()
	}

	// before committing our runs we want to set the start they are associated with
	updateStartID := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// for each run in our sessions, set the start id
		for _, s := range sessions {
			for _, r := range s.Runs() {
				r.SetStartID(batch.StartID())
			}
		}
		return nil
	}

	// options for our flow start
	options := NewStartOptions()
	options.RestartParticipants = batch.RestartParticipants()
	options.IncludeActive = batch.IncludeActive()
	options.Interrupt = flow.FlowType().Interrupts()
	options.TriggerBuilder = triggerBuilder
	options.CommitHook = updateStartID

	sessions, err := StartFlow(ctx, rt, oa, flow, batch.ContactIDs(), options)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow batch")
	}

	// log both our total and average
	librato.Gauge("mr.flow_batch_start_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.flow_batch_start_count", float64(len(sessions)))

	return sessions, nil
}

// FireCampaignEvents starts the flow for the passed in org, contact and flow
func FireCampaignEvents(
	ctx context.Context, rt *runtime.Runtime,
	orgID models.OrgID, fires []*models.EventFire, flowUUID assets.FlowUUID,
	campaign *triggers.CampaignReference, eventUUID triggers.CampaignEventUUID) ([]models.ContactID, error) {

	if len(fires) == 0 {
		return nil, nil
	}

	start := time.Now()

	contactIDs := make([]models.ContactID, 0, len(fires))
	fireMap := make(map[models.ContactID]*models.EventFire, len(fires))
	skippedContacts := make(map[models.ContactID]*models.EventFire, len(fires))
	for _, f := range fires {
		contactIDs = append(contactIDs, f.ContactID)
		fireMap[f.ContactID] = f
		skippedContacts[f.ContactID] = f
	}

	// create our org assets
	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", orgID)
	}

	// find our actual event
	dbEvent := oa.CampaignEventByID(fires[0].EventID)

	// no longer active? delete these event fires and return
	if dbEvent == nil {
		err := models.DeleteEventFires(ctx, rt.DB, fires)
		if err != nil {
			return nil, errors.Wrapf(err, "error deleting events for already fired events")
		}
		return nil, nil
	}

	// try to load our flow
	flow, err := oa.Flow(flowUUID)
	if err == models.ErrNotFound {
		err := models.DeleteEventFires(ctx, rt.DB, fires)
		if err != nil {
			return nil, errors.Wrapf(err, "error deleting events for archived or inactive flow")
		}
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %s", flowUUID)
	}
	dbFlow := flow.(*models.Flow)

	// our start options are based on the start mode for our event
	options := NewStartOptions()
	switch dbEvent.StartMode() {
	case models.StartModeInterrupt:
		options.IncludeActive = true
		options.RestartParticipants = true
		options.Interrupt = true
	case models.StartModePassive:
		options.IncludeActive = true
		options.RestartParticipants = true
		options.Interrupt = false
	case models.StartModeSkip:
		options.IncludeActive = false
		options.RestartParticipants = true
		options.Interrupt = true
	default:
		return nil, errors.Errorf("unknown start mode: %s", dbEvent.StartMode())
	}

	// if this is an ivr flow, we need to create a task to perform the start there
	if dbFlow.FlowType() == models.FlowTypeVoice {
		// Trigger our IVR flow start
		err := TriggerIVRFlow(ctx, rt, oa.OrgID(), dbFlow.ID(), contactIDs, func(ctx context.Context, tx *sqlx.Tx) error {
			return models.MarkEventsFired(ctx, tx, fires, time.Now(), models.FireResultFired)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error triggering ivr flow start")
		}
		return contactIDs, nil
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
		delete(skippedContacts, models.ContactID(contact.ID()))
		return triggers.NewBuilder(oa.Env(), flowRef, contact).Campaign(campaign, eventUUID).Build()
	}

	// this is our pre commit callback for our sessions, we'll mark the event fires associated
	// with the passed in sessions as complete in the same transaction
	fired := time.Now()
	options.CommitHook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// build up our list of event fire ids based on the session contact ids
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, s := range sessions {
			fire, found := fireMap[s.ContactID()]
			if !found {
				return errors.Errorf("unable to find associated event fire for contact %d", s.Contact().ID())
			}
			fires = append(fires, fire)
		}

		// mark those events as fired
		err := models.MarkEventsFired(ctx, tx, fires, fired, models.FireResultFired)
		if err != nil {
			return errors.Wrapf(err, "error marking events fired")
		}

		// now build up our list of skipped contacts (no trigger was built for them)
		fires = make([]*models.EventFire, 0, len(skippedContacts))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}

		// and mark those as skipped
		err = models.MarkEventsFired(ctx, tx, fires, fired, models.FireResultSkipped)
		if err != nil {
			return errors.Wrapf(err, "error marking events skipped")
		}

		// clear those out
		skippedContacts = make(map[models.ContactID]*models.EventFire)
		return nil
	}

	sessions, err := StartFlow(ctx, rt, oa, dbFlow, contactIDs, options)
	if err != nil {
		logrus.WithField("contact_ids", contactIDs).WithError(err).Errorf("error starting flow for campaign event: %s", eventUUID)
	} else {
		// make sure any skipped contacts are marked as fired this can occur if all fires were skipped
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}
		err = models.MarkEventsFired(ctx, rt.DB, fires, fired, models.FireResultSkipped)
		if err != nil {
			logrus.WithField("fire_ids", fires).WithError(err).Errorf("error marking events as skipped: %s", eventUUID)
		}
	}

	// log both our total and average
	librato.Gauge("mr.campaign_event_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.campaign_event_count", float64(len(sessions)))

	// build the list of contacts actually started
	startedContacts := make([]models.ContactID, len(sessions))
	for i := range sessions {
		startedContacts[i] = sessions[i].ContactID()
	}
	return startedContacts, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(
	ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets,
	flow *models.Flow, contactIDs []models.ContactID, options *StartOptions) ([]*models.Session, error) {

	if len(contactIDs) == 0 {
		return nil, nil
	}

	// figures out which contacts need to be excluded if any
	exclude := make(map[models.ContactID]bool, 5)

	// filter out anybody who has has a flow run in this flow if appropriate
	if !options.RestartParticipants {
		// find all participants that have been in this flow
		started, err := models.FindFlowStartedOverlap(ctx, rt.DB, flow.ID(), contactIDs)
		if err != nil {
			return nil, errors.Wrapf(err, "error finding others started flow: %d", flow.ID())
		}
		for _, c := range started {
			exclude[c] = true
		}
	}

	// filter out our list of contacts to only include those that should be started
	if !options.IncludeActive {
		// find all participants active in any flow
		active, err := models.FindActiveSessionOverlap(ctx, rt.DB, flow.FlowType(), contactIDs)
		if err != nil {
			return nil, errors.Wrapf(err, "error finding other active flow: %d", flow.ID())
		}
		for _, c := range active {
			exclude[c] = true
		}
	}

	// filter into our final list of contacts
	includedContacts := make([]models.ContactID, 0, len(contactIDs))
	for _, c := range contactIDs {
		if !exclude[c] {
			includedContacts = append(includedContacts, c)
		}
	}

	// no contacts left? we are done
	if len(includedContacts) == 0 {
		return nil, nil
	}

	// we now need to grab locks for our contacts so that they are never in two starts or handles at the
	// same time we try to grab locks for up to five minutes, but do it in batches where we wait for one
	// second per contact to prevent deadlocks
	sessions := make([]*models.Session, 0, len(includedContacts))
	remaining := includedContacts
	start := time.Now()

	// map of locks we've released
	released := make(map[string]bool)

	for len(remaining) > 0 && time.Since(start) < time.Minute*5 {
		locked := make([]models.ContactID, 0, len(remaining))
		locks := make([]string, 0, len(remaining))
		skipped := make([]models.ContactID, 0, 5)

		// try up to a second to get a lock for a contact
		for _, contactID := range remaining {
			lockID := models.ContactLock(oa.OrgID(), contactID)
			lock, err := locker.GrabLock(rt.RP, lockID, time.Minute*5, time.Second)
			if err != nil {
				return nil, errors.Wrapf(err, "error attempting to grab lock")
			}
			if lock == "" {
				skipped = append(skipped, contactID)
				continue
			}
			locked = append(locked, contactID)
			locks = append(locks, lock)

			// defer unlocking if we exit due to error
			defer func() {
				if !released[lockID] {
					locker.ReleaseLock(rt.RP, lockID, lock)
				}
			}()
		}

		// load our locked contacts
		contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, locked)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading contacts to start")
		}

		// ok, we've filtered our contacts, build our triggers
		triggers := make([]flows.Trigger, 0, len(locked))
		for _, c := range contacts {
			contact, err := c.FlowContact(oa)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating flow contact")
			}
			trigger := options.TriggerBuilder(contact)
			triggers = append(triggers, trigger)
		}

		ss, err := StartFlowForContacts(ctx, rt, oa, flow, triggers, options.CommitHook, options.Interrupt)
		if err != nil {
			return nil, errors.Wrapf(err, "error starting flow for contacts")
		}

		// append all the sessions that were started
		sessions = append(sessions, ss...)

		// release all our locks
		for i := range locked {
			lockID := models.ContactLock(oa.OrgID(), locked[i])
			locker.ReleaseLock(rt.RP, lockID, locks[i])
			released[lockID] = true
		}

		// skipped are now our remaining
		remaining = skipped
	}

	return sessions, nil
}

// StartFlowForContacts runs the passed in flow for the passed in contact
func StartFlowForContacts(
	ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets,
	flow *models.Flow, triggers []flows.Trigger, hook models.SessionCommitHook, interrupt bool) ([]*models.Session, error) {
	sa := oa.SessionAssets()

	// no triggers? nothing to do
	if len(triggers) == 0 {
		return nil, nil
	}

	start := time.Now()
	log := logrus.WithField("flow_name", flow.Name()).WithField("flow_uuid", flow.UUID())

	// for each trigger start the flow
	sessions := make([]flows.Session, 0, len(triggers))
	sprints := make([]flows.Sprint, 0, len(triggers))

	for _, trigger := range triggers {
		// start our flow session
		log := log.WithField("contact_uuid", trigger.Contact().UUID())
		start := time.Now()

		session, sprint, err := goflow.Engine(rt.Config).NewSession(sa, trigger)
		if err != nil {
			log.WithError(err).Errorf("error starting flow")
			continue
		}
		log.WithField("elapsed", time.Since(start)).Info("flow engine start")
		librato.Gauge("mr.flow_start_elapsed", float64(time.Since(start)))

		sessions = append(sessions, session)
		sprints = append(sprints, sprint)
	}

	if len(sessions) == 0 {
		return nil, nil
	}

	// we write our sessions and all their objects in a single transaction
	txCTX, cancel := context.WithTimeout(ctx, commitTimeout*time.Duration(len(sessions)))
	defer cancel()

	tx, err := rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction")
	}

	// build our list of contact ids
	contactIDs := make([]models.ContactID, len(triggers))
	for i := range triggers {
		contactIDs[i] = models.ContactID(triggers[i].Contact().ID())
	}

	// interrupt all our contacts if desired
	if interrupt {
		err = models.InterruptSessionsOfTypeForContacts(txCTX, tx, contactIDs, flow.FlowType())
		if err != nil {
			tx.Rollback()
			return nil, errors.Wrap(err, "error interrupting contacts")
		}
	}

	// write our session to the db
	dbSessions, err := models.WriteSessions(txCTX, rt, tx, oa, sessions, sprints, hook)
	if err == nil {
		// commit it at once
		commitStart := time.Now()
		err = tx.Commit()

		if err == nil {
			logrus.WithField("elapsed", time.Since(commitStart)).WithField("count", len(sessions)).Debug("sessions committed")
		}
	}

	// retry committing our sessions one at a time
	if err != nil {
		logrus.WithError(err).Debug("failed committing bulk transaction, retrying one at a time")

		tx.Rollback()

		// we failed writing our sessions in one go, try one at a time
		for i := range sessions {
			session := sessions[i]
			sprint := sprints[i]

			txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "error starting transaction for retry")
			}

			// interrupt this contact if appropriate
			if interrupt {
				err = models.InterruptSessionsOfTypeForContacts(txCTX, tx, []models.ContactID{models.ContactID(session.Contact().ID())}, flow.FlowType())
				if err != nil {
					tx.Rollback()
					log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error interrupting contact")
					continue
				}
			}

			dbSession, err := models.WriteSessions(txCTX, rt, tx, oa, []flows.Session{session}, []flows.Sprint{sprint}, hook)
			if err != nil {
				tx.Rollback()
				log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error writing session to db")
				continue
			}

			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error comitting session to db")
				continue
			}

			dbSessions = append(dbSessions, dbSession[0])
		}
	}

	// now take care of any post-commit hooks
	txCTX, cancel = context.WithTimeout(ctx, postCommitTimeout*time.Duration(len(sessions)))
	defer cancel()

	tx, err = rt.DB.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction for post commit hooks")
	}

	scenes := make([]*models.Scene, 0, len(triggers))
	for _, s := range dbSessions {
		scenes = append(scenes, s.Scene())
	}

	err = models.ApplyEventPostCommitHooks(txCTX, rt, tx, oa, scenes)
	if err == nil {
		err = tx.Commit()
	}

	if err != nil {
		tx.Rollback()

		// we failed with our post commit hooks, try one at a time, logging those errors
		for _, session := range dbSessions {
			log = log.WithField("contact_uuid", session.ContactUUID())

			txCTX, cancel = context.WithTimeout(ctx, postCommitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				tx.Rollback()
				log.WithError(err).Error("error starting transaction to retry post commits")
				continue
			}

			err = models.ApplyEventPostCommitHooks(ctx, rt, tx, oa, []*models.Scene{session.Scene()})
			if err != nil {
				tx.Rollback()
				log.WithError(err).Errorf("error applying post commit hook")
				continue
			}

			err = tx.Commit()

			if err != nil {
				tx.Rollback()
				log.WithError(err).Errorf("error comitting post commit hook")
				continue
			}
		}
	}

	// figure out both average and total for total execution and commit time for our flows
	log.WithField("elapsed", time.Since(start)).WithField("count", len(dbSessions)).Info("flow started, sessions created")
	return dbSessions, nil
}

type DBHook func(ctx context.Context, tx *sqlx.Tx) error

// TriggerIVRFlow will create a new flow start with the passed in flow and set of contacts. This will cause us to
// request calls to start, which once we get the callback will trigger our actual flow to start.
func TriggerIVRFlow(ctx context.Context, rt *runtime.Runtime, orgID models.OrgID, flowID models.FlowID, contactIDs []models.ContactID, hook DBHook) error {
	tx, _ := rt.DB.BeginTxx(ctx, nil)

	// create our start
	start := models.NewFlowStart(orgID, models.StartTypeTrigger, models.FlowTypeVoice, flowID, models.DoRestartParticipants, models.DoIncludeActive).
		WithContactIDs(contactIDs)

	// insert it
	err := models.InsertFlowStarts(ctx, tx, []*models.FlowStart{start})
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error inserting ivr flow start")
	}

	// call our hook if we have one
	if hook != nil {
		err = hook(ctx, tx)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error while calling db hook")
		}
	}

	// commit our transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error committing transaction for ivr flow starts")
	}

	// create our batch of all our contacts
	task := start.CreateBatch(contactIDs, true, len(contactIDs))

	// queue this to our ivr starter, it will take care of creating the connections then calling back in
	rc := rt.RP.Get()
	defer rc.Close()
	err = queue.AddTask(rc, queue.BatchQueue, queue.StartIVRFlowBatch, int(orgID), task, queue.HighPriority)
	if err != nil {
		return errors.Wrapf(err, "error queuing ivr flow start")
	}

	return nil
}
