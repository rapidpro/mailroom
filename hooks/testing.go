package hooks

import (
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

var (
	httpClient = utils.NewHTTPClient("mailroom")
)

const (
	Cathy = flows.ContactID(43)
	Bob   = flows.ContactID(58)
	Evan  = flows.ContactID(47)
)

type EventTestCase struct {
	Events     ContactEventMap
	Assertions []SQLAssertion
}

type SQLAssertion struct {
	SQL   string
	Args  []interface{}
	Count int
}

type ContactEventMap map[flows.ContactID][]flows.Event

func RunEventTestCases(t *testing.T, tcs []EventTestCase) {
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	assert.NoError(t, err)

	for i, tc := range tcs {
		sessions, err := GetTestSessions()
		assert.NoError(t, err)

		// build our map of contact id to sessions
		contactMap := make(map[flows.ContactID]*models.Session)
		for _, s := range sessions {
			contactMap[s.ContactID] = s
		}

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			assert.NoError(t, err)
			continue
		}

		// apply all our test case events
		for c, events := range tc.Events {
			session := contactMap[c]
			for _, e := range events {
				err := models.ApplyEvent(ctx, tx, rp, org, session, e)
				if err != nil {
					assert.NoError(t, err, "%d: failed applying event", i)
				}
			}
		}

		// call our pre commit hooks
		err = models.ApplyPreEventHooks(ctx, tx, rp, org, sessions)
		if err != nil {
			assert.NoError(t, err, "%d: error applying pre commit hooks")
			tx.Rollback()
			continue
		}

		// commit
		err = tx.Commit()
		if err != nil {
			assert.NoError(t, err, "%d: error committing after pre event hooks", i)
			tx.Rollback()
			continue
		}

		// and our post commit hooks
		err = models.ApplyPostEventHooks(ctx, tx, rp, org, sessions)
		if err != nil {
			assert.NoError(t, err, "%d: error applying post commit hooks", i)
			tx.Rollback()
			continue
		}

		// now check our assertions
		for ii, a := range tc.Assertions {
			testsuite.AssertQueryCount(t, db, a.SQL, a.Args, a.Count, "%d:%d: mismatch in expected count for query: %s", i, ii, a.SQL)
		}
	}
}

func GetTestSessions() ([]*models.Session, error) {
	db := testsuite.DB()
	ctx := testsuite.CTX()
	org, err := models.GetOrgAssets(ctx, db, models.OrgID(1))
	if err != nil {
		return nil, err
	}

	sessionAssets, err := engine.NewSessionAssets(org)
	if err != nil {
		return nil, err
	}

	// load our contacts
	contacts, err := models.LoadContacts(ctx, db, org, []flows.ContactID{Cathy, Bob, Evan})
	if err != nil {
		return nil, err
	}

	// try to load our flow
	flow, err := org.Flow(assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"))
	if err != nil {
		return nil, errors.Annotatef(err, "error loading test flow")
	}

	// build our triggers
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())

	sessions := make([]*models.Session, len(contacts))
	for i, contact := range contacts {
		// create the session for this flow and run
		session := engine.NewSession(sessionAssets, engine.NewDefaultConfig(), httpClient)

		flowContact, err := contact.FlowContact(org, sessionAssets)
		if err != nil {
			return nil, errors.Annotatef(err, "error building flow contact")
		}

		// create our trigger
		trigger := triggers.NewManualTrigger(org.Env(), flowContact, flowRef, nil, time.Now())
		err = session.Start(trigger)
		if err != nil {
			return nil, errors.Annotate(err, "error running test flow")
		}

		dbSession, err := models.NewSession(org, session)
		if err != nil {
			return nil, errors.Annotate(err, "error creating db session")
		}

		sessions[i] = dbSession
	}
	return sessions, nil
}
