package ctasks_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/aws/dynamo"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/ctasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventReceived(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetAll)

	oa := testdb.Org1.Load(t, rt)

	// stop Bob so we can test that he gets un-stopped on new conversation
	rt.DB.MustExec(`UPDATE contacts_contact SET status = 'S' WHERE id = $1`, testdb.Bob.ID)

	// schedule a campaign fires for Ann and Cat
	rt.DB.MustExec(
		fmt.Sprintf(`UPDATE contacts_contact SET fields = fields || '{"%s": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb WHERE id = $1 OR id = $2`, testdb.JoinedField.UUID),
		testdb.Ann.ID, testdb.Cat.ID,
	)
	testdb.InsertContactFire(t, rt, testdb.Org1, testdb.Ann, models.ContactFireTypeCampaignPoint, fmt.Sprintf("%d:1", testdb.RemindersPoint1.ID), time.Now(), "")
	testdb.InsertContactFire(t, rt, testdb.Org1, testdb.Cat, models.ContactFireTypeCampaignPoint, fmt.Sprintf("%d:1", testdb.RemindersPoint1.ID), time.Now(), "")

	// and Cat to doctors group, Ann is already part of it
	rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2);`, testdb.DoctorsGroup.ID, testdb.Cat.ID)

	// add some channel event triggers
	testdb.InsertNewConversationTrigger(t, rt, testdb.Org1, testdb.Favorites, testdb.FacebookChannel)
	testdb.InsertReferralTrigger(t, rt, testdb.Org1, testdb.PickANumber, "", testdb.VonageChannel)
	testdb.InsertOptInTrigger(t, rt, testdb.Org1, testdb.Favorites, testdb.VonageChannel)
	testdb.InsertOptOutTrigger(t, rt, testdb.Org1, testdb.PickANumber, testdb.VonageChannel)

	testdb.InsertOptIn(t, rt, testdb.Org1, "45aec4dd-945f-4511-878f-7d8516fbd336", "Polls")

	// add a URN for Ann so we can test twitter URNs
	testdb.InsertContactURN(t, rt, testdb.Org1, testdb.Bob, urns.URN("twitterid:123456"), 10, nil)

	// insert a dummy event into the database that will get the updates from handling each event which pretends to be it
	eventID := testdb.InsertChannelEvent(t, rt, testdb.Org1, "019ae4fc-56c6-7a94-b757-dcf0640e5ebc", models.EventTypeMissedCall, testdb.TwilioChannel, testdb.Ann, models.EventStatusPending)

	models.FlushCache()

	type testCase struct {
		Label           string                `json:"label"`
		ContactUUID     flows.ContactUUID     `json:"contact_uuid"`
		Task            *ctasks.EventReceived `json:"task"`
		DBAssertions    []*assertdb.Assert    `json:"db_assertions,omitempty"`
		ExpectedHistory []*dynamo.Item        `json:"expected_history,omitempty"`
	}

	tcs := make([]testCase, 0, 20)
	tcJSON := testsuite.ReadFile(t, "testdata/event_received.json")

	jsonx.MustUnmarshal(tcJSON, &tcs)

	reset := test.MockUniverse()
	defer reset()

	for i, tc := range tcs {
		time.Sleep(time.Millisecond * 5)

		mcs, err := models.LoadContactsByUUID(ctx, rt.DB, oa, []flows.ContactUUID{tc.ContactUUID})
		require.NoError(t, err)
		contact := mcs[0]

		// reset our dummy db event into an unhandled state
		rt.DB.MustExec(`UPDATE channels_channelevent SET status = 'P' WHERE id = $1`, eventID)

		err = tasks.QueueContact(ctx, rt, testdb.Org1.ID, contact.ID(), tc.Task)
		assert.NoError(t, err, "%d: error adding task", i)

		task, err := rt.Queues.Realtime.Pop(ctx, vc)
		assert.NoError(t, err, "%d: error popping next task", i)

		err = tasks.Perform(ctx, rt, task)
		assert.NoError(t, err, "%d: error when handling event", i)

		// check that event is marked as handled
		assertdb.Query(t, rt.DB, `SELECT status FROM channels_channelevent WHERE id = $1`, eventID).Columns(map[string]any{"status": "H"}, "%d: event state mismatch", i)

		actual := tc
		actual.ExpectedHistory = testsuite.GetHistoryItems(t, rt, true, test.MockStartTime)

		actual.DBAssertions = make([]*assertdb.Assert, len(tc.DBAssertions))
		for i, dba := range tc.DBAssertions {
			actual.DBAssertions[i] = dba.Actual(t, rt.DB)
		}

		if !test.UpdateSnapshots {
			for _, dba := range tc.DBAssertions {
				dba.Check(t, rt.DB, "%s: assertion for query '%s' failed", tc.Label, dba.Query)
			}

			if tc.ExpectedHistory == nil {
				tc.ExpectedHistory = []*dynamo.Item{}
			}
			test.AssertEqualJSON(t, jsonx.MustMarshal(tc.ExpectedHistory), jsonx.MustMarshal(actual.ExpectedHistory), "%s: event history mismatch", tc.Label)
		} else {
			tcs[i] = actual
		}
	}

	if test.UpdateSnapshots {
		truth, err := jsonx.MarshalPretty(tcs)
		require.NoError(t, err)

		err = os.WriteFile("testdata/event_received.json", truth, 0644)
		require.NoError(t, err, "failed to update truth file")
	}

	// check that only Cat is left in the group
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdb.DoctorsGroup.ID, testdb.Ann.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup_contacts WHERE contactgroup_id = $1 AND contact_id = $2`, testdb.DoctorsGroup.ID, testdb.Cat.ID).Returns(1)

	// and she has no upcoming campaign events
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'C'`, testdb.Ann.ID).Returns(0)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contactfire WHERE contact_id = $1 AND fire_type = 'C'`, testdb.Cat.ID).Returns(1)
}
