package runner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
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

// StartOptions define the various parameters that can be used when starting a flow
type StartOptions struct {
	// ExcludeInAFlow excludes contacts with waiting sessions which would otherwise have to be interrupted
	ExcludeInAFlow bool

	// ExcludeStartedPreviously excludes contacts who have been in this flow previously (at least as long as we have runs for)
	ExcludeStartedPreviously bool

	// Interrupt should be true if we want to interrupt the flows runs for any contact started in this flow
	Interrupt bool

	// CommitHook is the hook that will be called in the transaction where each session is written
	CommitHook models.SessionCommitHook

	// TriggerBuilder is the builder that will be used to build a trigger for each contact started in the flow
	TriggerBuilder TriggerBuilder
}

// NewStartOptions creates and returns the default start options to be used for flow starts
func NewStartOptions() *StartOptions {
	return &StartOptions{
		ExcludeInAFlow:           false,
		ExcludeStartedPreviously: false,
		Interrupt:                true,
	}
}

// TriggerBuilder defines the interface for building a trigger for the passed in contact
type TriggerBuilder func(contact *flows.Contact) flows.Trigger

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, session *models.Session, contact *models.Contact, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()
	sa := oa.SessionAssets()

	// does the flow this session is part of still exist?
	_, err := oa.FlowByID(session.CurrentFlowID())
	if err != nil {
		// if this flow just isn't available anymore, log this error
		if err == models.ErrNotFound {
			logrus.WithField("contact_uuid", session.Contact().UUID()).WithField("session_uuid", session.UUID()).WithField("flow_id", session.CurrentFlowID()).Error("unable to find flow for resume")
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
	sprint, err := fs.Resume(resume)

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
	err = session.Update(txCTX, rt, tx, oa, fs, sprint, contact, hook)
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

	logrus.WithField("contact_uuid", resume.Contact().UUID()).WithField("session_uuid", session.UUID()).WithField("resume_type", resume.Type()).WithField("elapsed", time.Since(start)).Info("resumed session")
	return session, nil
}

// StartFlowBatch starts the flow for the passed in org, contacts and flow
func StartFlowBatch(
	ctx context.Context, rt *runtime.Runtime,
	batch *models.FlowStartBatch) ([]*models.Session, error) {

	start := time.Now()

	// if this is our last start, no matter what try to set the start as complete as a last step
	if batch.IsLast {
		defer func() {
			err := models.MarkStartComplete(ctx, rt.DB, batch.StartID)
			if err != nil {
				logrus.WithError(err).WithField("start_id", batch.StartID).Error("error marking start as complete")
			}
		}()
	}

	// create our org assets
	oa, err := models.GetOrgAssets(ctx, rt, batch.OrgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating assets for org: %d", batch.OrgID)
	}

	// try to load our flow
	flow, err := oa.FlowByID(batch.FlowID)
	if err == models.ErrNotFound {
		logrus.WithField("flow_id", batch.FlowID).Info("skipping flow start, flow no longer active or archived")
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaign flow: %d", batch.FlowID)
	}

	// get the user that created this flow start if there was one
	var flowUser *flows.User
	if batch.CreatedByID != models.NilUserID {
		user := oa.UserByID(batch.CreatedByID)
		if user != nil {
			flowUser = oa.SessionAssets().Users().Get(user.Email())
		}
	}

	var params *types.XObject
	if !batch.Extra.IsNull() {
		params, err = types.ReadXObject(batch.Extra)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start extra")
		}
	}

	var history *flows.SessionHistory
	if !batch.SessionHistory.IsNull() {
		history, err = models.ReadSessionHistory(batch.SessionHistory)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start history")
		}
	}

	// whether engine allows some functions is based on whether there is more than one contact being started
	batchStart := batch.TotalContacts > 1

	// this will build our trigger for each contact started
	triggerBuilder := func(contact *flows.Contact) flows.Trigger {
		if !batch.ParentSummary.IsNull() {
			tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).FlowAction(history, json.RawMessage(batch.ParentSummary))
			if batchStart {
				tb = tb.AsBatch()
			}
			return tb.Build()
		}

		tb := triggers.NewBuilder(oa.Env(), flow.Reference(), contact).Manual()
		if batch.Extra != nil {
			tb = tb.WithParams(params)
		}
		if batchStart {
			tb = tb.AsBatch()
		}
		return tb.WithUser(flowUser).WithOrigin(startTypeToOrigin[batch.StartType]).Build()
	}

	// before committing our runs we want to set the start they are associated with
	updateStartID := func(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, oa *models.OrgAssets, sessions []*models.Session) error {
		// for each run in our sessions, set the start id
		for _, s := range sessions {
			for _, r := range s.Runs() {
				r.SetStartID(batch.StartID)
			}
		}
		return nil
	}

	// options for our flow start
	options := NewStartOptions()
	options.ExcludeStartedPreviously = batch.ExcludeStartedPreviously()
	options.ExcludeInAFlow = batch.ExcludeInAFlow()
	options.Interrupt = flow.FlowType().Interrupts()
	options.TriggerBuilder = triggerBuilder
	options.CommitHook = updateStartID

	sessions, err := StartFlow(ctx, rt, oa, flow, batch.ContactIDs, options)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting flow batch")
	}

	// log both our total and average
	analytics.Gauge("mr.flow_batch_start_elapsed", float64(time.Since(start))/float64(time.Second))
	analytics.Gauge("mr.flow_batch_start_count", float64(len(sessions)))

	return sessions, nil
}

// StartFlow runs the passed in flow for the passed in contacts
func StartFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, flow *models.Flow, contactIDs []models.ContactID, options *StartOptions) ([]*models.Session, error) {
	if len(contactIDs) == 0 {
		return nil, nil
	}

	// figures out which contacts need to be excluded if any
	exclude := make(map[models.ContactID]bool, 5)

	// filter out anybody who has has a flow run in this flow if appropriate
	if options.ExcludeStartedPreviously {
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
	if options.ExcludeInAFlow {
		// find all participants active in any flow
		active, err := models.FilterByWaitingSession(ctx, rt.DB, contactIDs)
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

	for len(remaining) > 0 && time.Since(start) < time.Minute*5 {
		ss, skipped, err := tryToStartWithLock(ctx, rt, oa, flow, remaining, options)
		if err != nil {
			return nil, err
		}

		sessions = append(sessions, ss...)
		remaining = skipped // skipped are now our remaining
	}

	return sessions, nil
}

// tries to start the given contacts, returning sessions for those we could, and the ids that were skipped because we
// couldn't get their locks
func tryToStartWithLock(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, flow *models.Flow, ids []models.ContactID, options *StartOptions) ([]*models.Session, []models.ContactID, error) {
	// try to get locks for these contacts, waiting for up to a second for each contact
	locks, skipped, err := models.LockContacts(rt, oa.OrgID(), ids, time.Second)
	if err != nil {
		return nil, nil, err
	}
	locked := maps.Keys(locks)

	// whatever happens, we need to unlock the contacts
	defer models.UnlockContacts(rt, oa.OrgID(), locks)

	// load our locked contacts
	contacts, err := models.LoadContacts(ctx, rt.ReadonlyDB, oa, locked)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error loading contacts to start")
	}

	// build our triggers
	triggers := make([]flows.Trigger, 0, len(locked))
	for _, c := range contacts {
		contact, err := c.FlowContact(oa)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "error creating flow contact")
		}
		trigger := options.TriggerBuilder(contact)
		triggers = append(triggers, trigger)
	}

	ss, err := StartFlowForContacts(ctx, rt, oa, flow, contacts, triggers, options.CommitHook, options.Interrupt)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error starting flow for contacts")
	}

	return ss, skipped, nil
}

// StartFlowForContacts runs the passed in flow for the passed in contact
func StartFlowForContacts(
	ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets,
	flow *models.Flow, contacts []*models.Contact, triggers []flows.Trigger, hook models.SessionCommitHook, interrupt bool) ([]*models.Session, error) {
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
		analytics.Gauge("mr.flow_start_elapsed", float64(time.Since(start)))

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
		err = models.InterruptSessionsForContactsTx(txCTX, tx, contactIDs)
		if err != nil {
			tx.Rollback()
			return nil, errors.Wrap(err, "error interrupting contacts")
		}
	}

	// write our session to the db
	dbSessions, err := models.InsertSessions(txCTX, rt, tx, oa, sessions, sprints, contacts, hook)
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
			contact := contacts[i]

			txCTX, cancel := context.WithTimeout(ctx, commitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "error starting transaction for retry")
			}

			// interrupt this contact if appropriate
			if interrupt {
				err = models.InterruptSessionsForContactsTx(txCTX, tx, []models.ContactID{models.ContactID(session.Contact().ID())})
				if err != nil {
					tx.Rollback()
					log.WithField("contact_uuid", session.Contact().UUID()).WithError(err).Errorf("error interrupting contact")
					continue
				}
			}

			dbSession, err := models.InsertSessions(txCTX, rt, tx, oa, []flows.Session{session}, []flows.Sprint{sprint}, []*models.Contact{contact}, hook)
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
