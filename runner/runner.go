package runner

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/librato"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"

	"github.com/nyaruka/goflow/flows/engine"
)

const (
	commitTimeout     = 15 * time.Second
	postCommitTimeout = 30 * time.Second
)

var (
	httpClient = utils.NewHTTPClient("mailroom")
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
	RestartParticipants bool

	// IncludeActive should be true if we want to interrupt people active in other flows (including this one)
	IncludeActive bool

	// Interrupt should be true if we want to interrupt the flows runs for any contact started in this flow
	// (simple campaign events do not currently interrupt)
	Interrupt bool

	// CommitHook is the hook that will be called in the transaction where each session is written
	CommitHook models.SessionCommitHook

	// TriggerBuilder is the builder that will be used to build a trigger for each contact started in the flow
	TriggerBuilder TriggerBuilder
}

// TriggerBuilder defines the interface for building a trigger for the passed in contact
type TriggerBuilder func(contact *flows.Contact) flows.Trigger

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, sa flows.SessionAssets, session *models.Session, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()

	// build our flow session
	fs, err := session.FlowSession(sa, org.Env(), httpClient)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create session from output")
	}

	// resume our session
	resumeStart := time.Now()
	es, err := fs.Resume(resume)
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
	err = session.WriteUpdatedSession(txCTX, tx, rp, org, fs, es)
	if err != nil {
		tx.Rollback()
		return nil, errors.Wrapf(err, "error updating session for resume")
	}

	// call our commit hook before committing our session
	if hook != nil {
		hook(ctx, tx, rp, org, []*models.Session{session})
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
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %d", batch.FlowID())
	}

	// flow is no longer active, skip
	if flow == nil || flow.IsArchived() {
		logrus.WithField("flow_uuid", flow.UUID()).Info("skipping flow start, flow no longer active or archived")
		return nil, nil
	}

	// this will build our trigger for each contact started
	now := time.Now()
	triggerBuilder := func(contact *flows.Contact) flows.Trigger {
		if batch.Parent() != nil {
			return triggers.NewFlowActionTrigger(org.Env(), flow.FlowReference(), contact, batch.Parent(), now)
		}
		return triggers.NewManualTrigger(org.Env(), flow.FlowReference(), contact, nil, now)
	}

	// before committing our runs we want to set the start they are associated with
	updateStartID := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		// for each run in our sessions, set the start id
		for _, s := range sessions {
			for _, r := range s.Runs() {
				r.StartID = batch.StartID()
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
	event *triggers.CampaignEvent) ([]*models.Session, error) {

	if len(fires) == 0 {
		return nil, nil
	}

	start := time.Now()

	contactIDs := make([]flows.ContactID, 0, len(fires))
	fireMap := make(map[flows.ContactID]*models.EventFire, len(fires))
	skippedContacts := make(map[flows.ContactID]*models.EventFire, len(fires))
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
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %s", flowUUID)
	}
	dbFlow := flow.(*models.Flow)

	// flow doesn't exist or is archived, skip
	if dbFlow == nil || dbFlow.IsArchived() {
		tx, _ := db.BeginTxx(ctx, nil)
		err := models.MarkEventsFired(ctx, tx, fires, time.Now())
		if err != nil {
			tx.Rollback()
			logrus.WithError(err).Error("error marking events as fired due to archived or inactive flow")
		}
		return nil, tx.Commit()
	}

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
		options.RestartParticipants = false
		options.Interrupt = false
	default:
		return nil, errors.Errorf("unknown start mode: %s", dbEvent.StartMode())
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	now := time.Now()
	options.TriggerBuilder = func(contact *flows.Contact) flows.Trigger {
		delete(skippedContacts, contact.ID())
		return triggers.NewCampaignTrigger(org.Env(), flowRef, contact, event, now)
	}

	// this is our pre commit callback for our sessions, we'll mark the event fires associated
	// with the passed in sessions as complete in the same transaction
	fired := time.Now()
	options.CommitHook = func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions []*models.Session) error {
		// build up our list of event fire ids based on the session contact ids
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, s := range sessions {
			fire, found := fireMap[s.Contact().ID()]
			if !found {
				return errors.Errorf("unable to find associated event fire for contact %d", s.Contact().ID())
			}
			fires = append(fires, fire)
		}

		// also add in any contacts that were skipped
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}

		// mark those as cleared
		skippedContacts = make(map[flows.ContactID]*models.EventFire)

		// bulk update those event fires
		return models.MarkEventsFired(ctx, tx, fires, fired)
	}

	sessions, err := StartFlow(ctx, db, rp, org, dbFlow, contactIDs, options)
	if err != nil {
		logrus.WithField("contact_ids", contactIDs).WithError(err).Errorf("error starting flow for campaign event: %s", event)
	} else {
		// make sure any skipped contacts are marked as fired
		// this can occur if there were no sessions committed, so our hook above is never called
		fires := make([]*models.EventFire, 0, len(sessions))
		for _, e := range skippedContacts {
			fires = append(fires, e)
		}
		err = models.MarkEventsFired(ctx, db, fires, fired)
		if err != nil {
			logrus.WithField("fire_ids", fires).WithError(err).Errorf("error marking events as fired: %s", event)
		}
	}

	// log both our total and average
	librato.Gauge("mr.campaign_event_elapsed", float64(time.Since(start))/float64(time.Second))
	librato.Gauge("mr.campaign_event_count", float64(len(sessions)))

	return sessions, nil
}

// StartFlow runs the passed in flow for the passed in contact
func StartFlow(
	ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets,
	flow *models.Flow, contactIDs []flows.ContactID, options *StartOptions) ([]*models.Session, error) {

	if len(contactIDs) == 0 {
		return nil, nil
	}

	// figures out which contacts need to be excluded if any
	exclude := make(map[flows.ContactID]bool, 5)

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
		active, err := models.FindActiveRunOverlap(ctx, db, contactIDs)
		if err != nil {
			return nil, errors.Wrapf(err, "error finding other active flow: %d", flow.ID())
		}
		for _, c := range active {
			exclude[c] = true
		}
	}

	// filter into our final list of contacts
	includedContacts := make([]flows.ContactID, 0, len(contactIDs))
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
	assets, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow, unable to load assets")
	}

	// load all our contacts
	contacts, err := models.LoadContacts(ctx, db, org, includedContacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contacts to start")
	}

	// ok, we've filtered our contacts, build our triggers
	triggers := make([]flows.Trigger, 0, len(includedContacts))
	for _, c := range contacts {
		contact, err := c.FlowContact(org, assets)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating flow contact")
		}
		triggers = append(triggers, options.TriggerBuilder(contact))
	}

	return StartFlowForContacts(ctx, db, rp, org, assets, triggers, options.CommitHook, options.Interrupt)
}

// StartFlowForContacts runs the passed in flow for the passed in contact
func StartFlowForContacts(
	ctx context.Context, db *sqlx.DB, rp *redis.Pool, org *models.OrgAssets, assets flows.SessionAssets,
	triggers []flows.Trigger, hook models.SessionCommitHook, interrupt bool) ([]*models.Session, error) {

	// no triggers? nothing to do
	if len(triggers) == 0 {
		return nil, nil
	}

	flow := triggers[0].Flow()

	start := time.Now()
	log := logrus.WithField("flow_name", flow.Name).WithField("flow_uuid", flow.UUID)

	// for each trigger start the flow
	sessions := make([]flows.Session, 0, len(triggers))
	sessionEvents := make([][]flows.Event, 0, len(triggers))

	for _, trigger := range triggers {
		// create the session for this flow and run
		session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)

		// start our flow
		log := log.WithField("contact_uuid", trigger.Contact().UUID())
		start := time.Now()
		events, err := session.Start(trigger)
		if err != nil {
			log.WithError(err).Errorf("error starting flow")
			continue
		}
		log.WithField("elapsed", time.Since(start)).Info("flow engine start")
		librato.Gauge("mr.flow_start_elapsed", float64(time.Since(start)))

		sessions = append(sessions, session)
		sessionEvents = append(sessionEvents, events)
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
		err = models.InterruptContactRuns(txCTX, tx, contactIDs, start)
		if err != nil {
			return nil, errors.Wrap(err, "error interrupting contacts")
		}
	}

	// write our session to the db
	dbSessions, err := models.WriteSessions(txCTX, tx, rp, org, sessions, sessionEvents, hook)
	if err != nil {
		return nil, errors.Wrapf(err, "error writing sessions")
	}

	// commit it at once
	commitStart := time.Now()
	err = tx.Commit()

	if err == nil {
		logrus.WithField("elapsed", time.Since(commitStart)).WithField("count", len(sessions)).Debug("sessions committed")
	}

	// retry committing our sessions one at a time
	if err != nil {
		tx.Rollback()

		// we failed writing our sessions in one go, try one at a time
		for i := range sessions {
			session := sessions[i]
			events := sessionEvents[i]

			txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
			defer cancel()

			tx, err := db.BeginTxx(txCTX, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "error starting transaction for retry")
			}

			// interrupt this contact if appropriate
			if interrupt {
				err = models.InterruptContactRuns(txCTX, tx, []flows.ContactID{session.Contact().ID()}, start)
				if err != nil {
					tx.Rollback()
					log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error interrupting contact")
					continue
				}
			}

			dbSession, err := models.WriteSessions(txCTX, tx, rp, org, []flows.Session{session}, [][]flows.Event{events}, hook)
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
