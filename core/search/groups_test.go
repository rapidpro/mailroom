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
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	// insert an event on our campaign
	newEvent := testdata.InsertCampaignFlowEvent(db, testdata.RemindersCampaign, testdata.Favorites, testdata.JoinedField, 1000, "W")

	// clear Cathy's value
	db.MustExec(
		`update contacts_contact set fields = fields - $2
		WHERE id = $1`, testdata.Cathy.ID, testdata.JoinedField.UUID)

	// and populate Bob's
	db.MustExec(
		fmt.Sprintf(`update contacts_contact set fields = fields ||
		'{"%s": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, testdata.JoinedField.UUID), testdata.Bob.ID)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshCampaigns|models.RefreshGroups)
	assert.NoError(t, err)

	mockES := testsuite.NewMockElasticServer()
	defer mockES.Close()

	es := mockES.Client()

	mockES.AddResponse(testdata.Cathy.ID)
	mockES.AddResponse(testdata.Bob.ID)
	mockES.AddResponse(testdata.Bob.ID)

	tcs := []struct {
		Query           string
		ContactIDs      []models.ContactID
		EventContactIDs []models.ContactID
	}{
		{
			"cathy",
			[]models.ContactID{testdata.Cathy.ID},
			[]models.ContactID{},
		},
		{
			"bob",
			[]models.ContactID{testdata.Bob.ID},
			[]models.ContactID{testdata.Bob.ID},
		},
		{
			"unchanged",
			[]models.ContactID{testdata.Bob.ID},
			[]models.ContactID{testdata.Bob.ID},
		},
	}

	for _, tc := range tcs {
		err := models.UpdateGroupStatus(ctx, db, testdata.DoctorsGroup.ID, models.GroupStatusInitializing)
		assert.NoError(t, err)

		count, err := search.PopulateSmartGroup(ctx, db, es, oa, testdata.DoctorsGroup.ID, tc.Query)
		assert.NoError(t, err, "error populating smart group for: %s", tc.Query)

		assert.Equal(t, count, len(tc.ContactIDs))

		// assert the current group membership
		contactIDs, err := models.ContactIDsForGroupIDs(ctx, db, []models.GroupID{testdata.DoctorsGroup.ID})
		assert.NoError(t, err)
		assert.Equal(t, tc.ContactIDs, contactIDs)

		assertdb.Query(t, db, `SELECT count(*) from contacts_contactgroup WHERE id = $1 AND status = 'R'`, testdata.DoctorsGroup.ID).
			Returns(1, "wrong number of contacts in group for query: %s", tc.Query)

		assertdb.Query(t, db, `SELECT count(*) from campaigns_eventfire WHERE event_id = $1`, newEvent.ID).
			Returns(len(tc.EventContactIDs), "wrong number of contacts with events for query: %s", tc.Query)

		assertdb.Query(t, db, `SELECT count(*) from campaigns_eventfire WHERE event_id = $1 AND contact_id = ANY($2)`, newEvent.ID, pq.Array(tc.EventContactIDs)).
			Returns(len(tc.EventContactIDs), "wrong contacts with events for query: %s", tc.Query)
	}
}
