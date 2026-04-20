package runner

import (
	"context"
	"encoding/json"
	"log/slog"
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

// TriggerBuilder defines the interface for building a trigger for the passed in contact
type TriggerBuilder func(contact *flows.Contact) flows.Trigger

// StartOptions define the various parameters that can be used when starting a flow
type StartOptions struct {
	// Interrupt should be true if we want to interrupt the flows runs for any contact started in this flow
	Interrupt bool

	// CommitHook is the hook that will be called in the transaction where each session is written
	CommitHook models.SessionCommitHook

	// TriggerBuilder is the builder that will be used to build a trigger for each contact started in the flow
	TriggerBuilder TriggerBuilder
}

// ResumeFlow resumes the passed in session using the passed in session
func ResumeFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, session *models.Session, contact *models.Contact, resume flows.Resume, hook models.SessionCommitHook) (*models.Session, error) {
	start := time.Now()
	sa := oa.SessionAssets()

	// does the flow this session is part of still exist?
	_, err := oa.FlowByID(session.CurrentFlowID())
	if err != nil {
		// if this flow just isn't available anymore, log this error
		if err == models.ErrNotFound {
			slog.Error("unable to find flow for resume", "contact_uuid", session.Contact().UUID(), "session_uuid", session.UUID(), "flow_id", session.CurrentFlowID())
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

	slog.Info("resumed session", "contact_uuid", resume.Contact().UUID(), "session_uuid", session.UUID(), "resume_type", resume.Type(), "elapsed", time.Since(start))
	return session, nil
}

// StartFlowBatch starts the flow for the passed in org, contacts and flow
func StartFlowBatch(ctx context.Context, rt *runtime.Runtime, batch *models.FlowStartBatch) ([]*models.Session, error) {
	start := time.Now()

	// if this is our last start, no matter what try to set the start as complete as a last step
	if batch.IsLast {
		defer func() {
			err := models.MarkStartComplete(ctx, rt.DB, batch.StartID)
			if err != nil {
				slog.Error("error marking start as complete", "error", err, "start_id", batch.StartID)
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
		slog.Info("skipping flow start, flow no longer active or archived", "flow_id", batch.FlowID)
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
	if !batch.Params.IsNull() {
		params, err = types.ReadXObject(batch.Params)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read JSON from flow start params")
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
		if !batch.Params.IsNull() {
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

	options := &StartOptions{
		Interrupt:      flow.FlowType().Interrupts(),
		TriggerBuilder: triggerBuilder,
		CommitHook:     updateStartID,
	}

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

	// we now need to grab locks for our contacts so that they are never in two starts or handles at the
	// same time we try to grab locks for up to five minutes, but do it in batches where we wait for one
	// second per contact to prevent deadlocks
	sessions := make([]*models.Session, 0, len(contactIDs))
	remaining := contactIDs
	start := time.Now()

	for len(remaining) > 0 && time.Since(start) < time.Minute*5 {
		if ctx.Err() != nil {
			return sessions, ctx.Err()
		}

		ss, skipped, err := tryToStartWithLock(ctx, rt, oa, flow, remaining, options)
		if err != nil {
			return nil, err
		}

		sessions = append(sessions, ss...)
		remaining = skipped // skipped are now our remaining
	}

	if len(remaining) > 0 {
		slog.Warn("failed to acquire locks for contacts", "contacts", remaining)
	}

	return sessions, nil
}

// tries to start the given contacts, returning sessions for those we could, and the ids that were skipped because we
// couldn't get their locks
func tryToStartWithLock(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, flow *models.Flow, ids []models.ContactID, options *StartOptions) ([]*models.Session, []models.ContactID, error) {
	// try to get locks for these contacts, waiting for up to a second for each contact
	locks, skipped, err := models.LockContacts(ctx, rt, oa.OrgID(), ids, time.Second)
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
	log := slog.With("flow_name", flow.Name(), "flow_uuid", flow.UUID())

	// for each trigger start the flow
	sessions := make([]flows.Session, 0, len(triggers))
	sprints := make([]flows.Sprint, 0, len(triggers))

	for _, trigger := range triggers {
		// start our flow session
		log := log.With("contact_uuid", trigger.Contact().UUID())
		start := time.Now()

		session, sprint, err := goflow.Engine(rt.Config).NewSession(sa, trigger)
		if err != nil {
			log.Error("error starting flow", "error", err)
			continue
		}
		log.Info("flow engine start", "elapsed", time.Since(start))
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
			slog.Debug("sessions committed", "elapsed", time.Since(commitStart), "count", len(sessions))
		}
	}

	// retry committing our sessions one at a time
	if err != nil {
		slog.Debug("failed committing bulk transaction, retrying one at a time", "error", err)

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
					log.Error("error interrupting contact", "error", err, "contact_uuid", session.Contact().UUID())
					continue
				}
			}

			dbSession, err := models.InsertSessions(txCTX, rt, tx, oa, []flows.Session{session}, []flows.Sprint{sprint}, []*models.Contact{contact}, hook)
			if err != nil {
				tx.Rollback()
				log.Error("error writing session to db", "error", err, "contact_uuid", session.Contact().UUID())
				continue
			}

			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				log.Error("error comitting session to db", "error", err, "contact_uuid", session.Contact().UUID())
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
			log = log.With("contact_uuid", session.ContactUUID())

			txCTX, cancel = context.WithTimeout(ctx, postCommitTimeout)
			defer cancel()

			tx, err := rt.DB.BeginTxx(txCTX, nil)
			if err != nil {
				tx.Rollback()
				log.Error("error starting transaction to retry post commits", "error", err)
				continue
			}

			err = models.ApplyEventPostCommitHooks(ctx, rt, tx, oa, []*models.Scene{session.Scene()})
			if err != nil {
				tx.Rollback()
				log.Error("error applying post commit hook", "error", err)
				continue
			}

			err = tx.Commit()

			if err != nil {
				tx.Rollback()
				log.Error("error comitting post commit hook", "error", err)
				continue
			}
		}
	}

	// figure out both average and total for total execution and commit time for our flows
	log.Info("flow started, sessions created", "elapsed", time.Since(start), "count", len(dbSessions))
	return dbSessions, nil
}
