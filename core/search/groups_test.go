package search_test

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
)

func TestSmartGroups(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// insert an event on our campaign
	newEvent := testdata.InsertCampaignFlowEvent(rt, testdata.RemindersCampaign, testdata.Favorites, testdata.JoinedField, 1000, "W")

	// clear Cathy's value
	rt.DB.MustExec(`update contacts_contact set fields = fields - $2 WHERE id = $1`, testdata.Cathy.ID, testdata.JoinedField.UUID)

	// and populate Bob's
	rt.DB.MustExec(
		fmt.Sprintf(`update contacts_contact set fields = fields || '{"%s": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb WHERE id = $1`, testdata.JoinedField.UUID),
		testdata.Bob.ID,
	)

	testsuite.ReindexElastic(ctx)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshCampaigns|models.RefreshGroups)
	assert.NoError(t, err)

	tcs := []struct {
		query              string
		expectedContactIDs []models.ContactID
		expectedEventIDs   []models.ContactID
	}{
		{ // 0
			query:              "cathy",
			expectedContactIDs: []models.ContactID{testdata.Cathy.ID},
			expectedEventIDs:   []models.ContactID{},
		},
		{ // 1
			query:              "bob",
			expectedContactIDs: []models.ContactID{testdata.Bob.ID},
			expectedEventIDs:   []models.ContactID{testdata.Bob.ID},
		},
		{ // 2
			query:              "name = BOB",
			expectedContactIDs: []models.ContactID{testdata.Bob.ID},
			expectedEventIDs:   []models.ContactID{testdata.Bob.ID},
		},
	}

	for i, tc := range tcs {
		err := models.UpdateGroupStatus(ctx, rt.DB, testdata.DoctorsGroup.ID, models.GroupStatusInitializing)
		assert.NoError(t, err)

		count, err := search.PopulateSmartGroup(ctx, rt, rt.ES, oa, testdata.DoctorsGroup.ID, tc.query)
		assert.NoError(t, err, "error populating smart group for: %s", tc.query)

		assert.Equal(t, count, len(tc.expectedContactIDs), "%d: contact count mismatch", i)

		// assert the current group membership
		contactIDs, err := models.ContactIDsForGroupIDs(ctx, rt.DB, []models.GroupID{testdata.DoctorsGroup.ID})
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedContactIDs, contactIDs)

		assertdb.Query(t, rt.DB, `SELECT count(*) from contacts_contactgroup WHERE id = $1 AND status = 'R'`, testdata.DoctorsGroup.ID).
			Returns(1, "wrong number of contacts in group for query: %s", tc.query)

		assertdb.Query(t, rt.DB, `SELECT count(*) from campaigns_eventfire WHERE event_id = $1`, newEvent.ID).
			Returns(len(tc.expectedEventIDs), "wrong number of contacts with events for query: %s", tc.query)

		assertdb.Query(t, rt.DB, `SELECT count(*) from campaigns_eventfire WHERE event_id = $1 AND contact_id = ANY($2)`, newEvent.ID, pq.Array(tc.expectedEventIDs)).
			Returns(len(tc.expectedEventIDs), "wrong contacts with events for query: %s", tc.query)
	}
}
