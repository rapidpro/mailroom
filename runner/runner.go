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
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/locker"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/queue"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	commitTimeout     = time.Minute
	postCommitTimeout = time.Minute
)

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
type TriggerBuilder func(contact *flows.Contact) (flows.Trigger, error)

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, sa flows.SessionAssets, session *models.Session, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()

	// does the flow this session is part of still exist?
	flow, err := org.FlowByID(session.CurrentFlowID())
	if err != nil {
		// if this flow just isn't available anymore, log this error
		if err == models.ErrNotFound {
			logrus.WithField("contact_uuid", session.Contact().UUID()).WithField("session_id", session.ID()).WithField("flow_id", session.CurrentFlowID()).Error("unable to find flow in resume")
			return nil, models.ExitSessions(ctx, db, []models.SessionID{session.ID()}, models.ExitInterrupted, time.Now())
		}
		return nil, errors.Wrapf(err, "error loading session flow: %d", session.CurrentFlowID())
	}

	// validate our flow
	err = validateFlow(sa, flow.UUID())
	if err != nil {
		return nil, errors.Wrapf(err, "invalid flow: %s, cannot resume", flow.UUID())
	}

	// build our flow session
	fs, err := session.FlowSession(sa, org.Env())
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

	tx, err := db.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction")
	}

	// write our updated session and runs
	err = session.WriteUpdatedSession(txCTX, tx, rp, org, fs, sprint, hook)
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

	tx, err = db.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction for post commit hooks")
	}

	err = models.ApplyPostEventHooks(txCTX, tx, rp, org, []*models.Session{session})
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
	ctx context.Context, db *sqlx.DB, rp *redis.Pool,
	batch *models.FlowStartBatch) ([]*models.Session, error) {

	start := time.Now()

	// if this is our last start, no matter what try to set the start as complete as a last step
	if batch.IsLast() {
		defer func() {
			err := models.MarkStartComplete(ctx, db, batch.StartID())
			if err != nil {
				logrus.WithError(err).WithField("start_id", batch.StartID).Error("error marking start as complete")
			}
		}()
	}

	// create our org assets
	org, err := models.GetOrgAssets(ctx, db, batch.OrgID())
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", batch.OrgID())
	}

	// try to load our flow
	flow, err := org.FlowByID(batch.FlowID())
	if err == models.ErrNotFound {
		logrus.WithField("flow_id", batch.FlowID()).Info("skipping flow start, flow no longer active or archived")
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %d", batch.FlowID())
	}

	var params *types.XObject
	if len(batch.Extra()) > 0 {
		params, err = types.ReadXObject(batch.Extra())
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start extra")
		}
	}

	// this will build our trigger for each contact started
	triggerBuilder := func(contact *flows.Contact) (flows.Trigger, error) {
		if batch.ParentSummary() != nil {
			trigger, err := triggers.NewFlowAction(org.Env(), flow.FlowReference(), contact, batch.ParentSummary())
			if err != nil {
				return nil, errors.Wrap(err, "unable to create flow action trigger")
			}
			return trigger, nil
		}
		if batch.Extra() != nil {
			return triggers.NewManual(org.Env(), flow.FlowReference(), contact, params), nil
		}
		return triggers.NewManual(org.Env(), flow.FlowReference(), contact, nil), nil
	}

	// before committing our runs we want to set the start they are associated with
	updateStartID := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
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
	options.Interrupt = true
	options.TriggerBuilder = triggerBuilder
	options.CommitHook = updateStartID

	sessions, err := StartFlow(ctx, db, rp, org, flow, batch.ContactIDs(), options)
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
	ctx context.Context, db *sqlx.DB, rp *redis.Pool,
	orgID models.OrgID, fires []*models.EventFire, flowUUID assets.FlowUUID,
	event *triggers.CampaignEvent) ([]models.ContactID, error) {

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
	org, err := models.GetOrgAssets(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", orgID)
	}

	// find our actual event
	dbEvent := org.CampaignEventByID(fires[0].EventID)

	// no longer active? delete these event fires and return
	if dbEvent == nil {
		err := models.DeleteEventFires(ctx, db, fires)
		if err != nil {
			return nil, errors.Wrapf(err, "error deleting events for already fired events")
		}
		return nil, nil
	}

	// try to load our flow
	flow, err := org.Flow(flowUUID)
	if err == models.ErrNotFound {
		err := models.DeleteEventFires(ctx, db, fires)
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
	if dbFlow.FlowType() == models.IVRFlow {
		// Trigger our IVR flow start
		err := TriggerIVRFlow(ctx, db, rp, org.OrgID(), dbFlow.ID(), contactIDs, func(ctx context.Context, tx *sqlx.Tx) error {
			return models.MarkEventsFired(ctx, tx, fires, time.Now(), models.FireResultFired)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "error triggering ivr flow start")
		}
		return contactIDs, nil
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	options.TriggerBuilder = func(contact *flows.Contact) (flows.Trigger, error) {
		delete(skippedContacts, models.ContactID(contact.ID()))
		return triggers.NewCampaign(org.Env(), flowRef, contact, event), nil
	}

	// this is our pre commit callback for our sessions, we'll mark the event fires associated
	// with the passed in sessions as complete in the same transaction
	fired := time.Now()
	options.CommitHook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
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

	sessions, err := StartFlow(ctx, db, rp, org, dbFlow, contactIDs, options)
	if err != nil {
		logrus.WithField("contact_ids", contactIDs).WithError(err).Errorf("error starting flow for campaign event: %v", event)
	} else {
		// make sure any skipped contacts are marked as fired this can occur if all fires were skipped
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}
		err = models.MarkEventsFired(ctx, db, fires, fired, models.FireResultSkipped)
		if err != nil {
			logrus.WithField("fire_ids", fires).WithError(err).Errorf("error marking events as skipped: %v", event)
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
	ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets,
	flow *models.Flow, contactIDs []models.ContactID, options *StartOptions) ([]*models.Session, error) {

	if len(contactIDs) == 0 {
		return nil, nil
	}

	// figures out which contacts need to be excluded if any
	exclude := make(map[models.ContactID]bool, 5)

	// filter out anybody who has has a flow run in this flow if appropriate
	if !options.RestartParticipants {
		// find all participants that have been in this flow
		started, err := models.FindFlowStartedOverlap(ctx, db, flow.ID(), contactIDs)
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
		active, err := models.FindActiveSessionOverlap(ctx, db, flow.FlowType(), contactIDs)
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

	// build our session assets
	sa, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow, unable to load assets")
	}

	// validate our flow
	err = validateFlow(sa, flow.UUID())
	if err != nil {
		return nil, errors.Wrapf(err, "invalid flow: %s, cannot start", flow.UUID())
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
			lockID := models.ContactLock(org.OrgID(), contactID)
			lock, err := locker.GrabLock(rp, lockID, time.Minute*5, time.Second)
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
					locker.ReleaseLock(rp, lockID, lock)
				}
			}()
		}

		// load our locked contacts
		contacts, err := models.LoadContacts(ctx, db, org, locked)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading contacts to start")
		}

		// ok, we've filtered our contacts, build our triggers
		triggers := make([]flows.Trigger, 0, len(locked))
		for _, c := range contacts {
			contact, err := c.FlowContact(org, sa)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating flow contact")
			}
			trigger, err := options.TriggerBuilder(contact)
			if err != nil {
				return nil, err
			}
			triggers = append(triggers, trigger)
		}

		ss, err := StartFlowForContacts(ctx, db, rp, org, sa, flow, triggers, options.CommitHook, options.Interrupt)
		if err != nil {
			return nil, errors.Wrapf(err, "error starting flow for contacts")
		}

		// append all the sessions that were started
		for _, s := range ss {
			sessions = append(sessions, s)
		}

		// release all our locks
		for i := range locked {
			lockID := models.ContactLock(org.OrgID(), locked[i])
			locker.ReleaseLock(rp, lockID, locks[i])
			released[lockID] = true
		}

		// skipped are now our remaining
		remaining = skipped
	}

	return sessions, nil
}

// StartFlowForContacts runs the passed in flow for the passed in contact
func StartFlowForContacts(
	ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, assets flows.SessionAssets,
	flow *models.Flow, triggers []flows.Trigger, hook models.SessionCommitHook, interrupt bool) ([]*models.Session, error) {

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

		session, sprint, err := goflow.Engine().NewSession(assets, trigger)
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

	tx, err := db.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction")
	}

	// build our list of contact ids
	contactIDs := make([]flows.ContactID, len(triggers))
	for i := range triggers {
		contactIDs[i] = triggers[i].Contact().ID()
	}

	// interrupt all our contacts if desired
	if interrupt {
		err = models.InterruptContactRuns(txCTX, tx, flow.FlowType(), contactIDs, start)
		if err != nil {
			tx.Rollback()
			return nil, errors.Wrap(err, "error interrupting contacts")
		}
	}

	// write our session to the db
	dbSessions, err := models.WriteSessions(txCTX, tx, rp, org, sessions, sprints, hook)
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

			tx, err := db.BeginTxx(txCTX, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "error starting transaction for retry")
			}

			// interrupt this contact if appropriate
			if interrupt {
				err = models.InterruptContactRuns(txCTX, tx, flow.FlowType(), []flows.ContactID{session.Contact().ID()}, start)
				if err != nil {
					tx.Rollback()
					log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error interrupting contact")
					continue
				}
			}

			dbSession, err := models.WriteSessions(txCTX, tx, rp, org, []flows.Session{session}, []flows.Sprint{sprint}, hook)
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

	tx, err = db.BeginTxx(txCTX, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction for post commit hooks")
	}

	err = models.ApplyPostEventHooks(txCTX, tx, rp, org, dbSessions)
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

			tx, err := db.BeginTxx(txCTX, nil)
			if err != nil {
				tx.Rollback()
				log.WithError(err).Error("error starting transaction to retry post commits")
				continue
			}

			err = models.ApplyPostEventHooks(ctx, tx, rp, org, []*models.Session{session})
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
func TriggerIVRFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, orgID models.OrgID, flowID models.FlowID, contactIDs []models.ContactID, hook DBHook) error {
	tx, _ := db.BeginTxx(ctx, nil)

	// create our start
	start := models.NewFlowStart(orgID, models.IVRFlow, flowID, models.DoRestartParticipants, models.DoIncludeActive).
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
	task := start.CreateBatch(contactIDs)
	task.SetIsLast(true)

	// queue this to our ivr starter, it will take care of creating the connections then calling back in
	rc := rp.Get()
	defer rc.Close()
	err = queue.AddTask(rc, queue.BatchQueue, queue.StartIVRFlowBatch, int(orgID), task, queue.HighPriority)
	if err != nil {
		return errors.Wrapf(err, "error queuing ivr flow start")
	}

	return nil
}

func validateFlow(sa flows.SessionAssets, uuid assets.FlowUUID) error {
	flow, err := sa.Flows().Get(uuid)
	if err != nil {
		return errors.Wrapf(err, "invalid flow: %s, cannot start", uuid)
	}

	// check for missing dependencies and log
	missingDeps := make([]string, 0)
	err = flow.CheckDependenciesRecursive(sa, func(r assets.Reference) {
		missingDeps = append(missingDeps, r.String())
	})

	// one day we might error if we encounter missing dependencies but for now it's too common so log them
	// to help us find whatever problem is creating them
	if len(missingDeps) > 0 {
		logrus.WithField("flow_uuid", flow.UUID()).WithField("missing", missingDeps).Warn("flow being started with missing dependencies")
	}

	return nil
}
