package models_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/lib/pq"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadGroups(t *testing.T) {
	ctx, _, db0, _ := testsuite.Get()

	db := testsuite.NewMockDB(db0, func(funcName string, call int) error {
		// fail first query for groups
		if funcName == "QueryxContext" && call == 0 {
			return errors.New("boom")
		}
		return nil
	})

	_, err := models.LoadGroups(ctx, db, testdata.Org1.ID)
	require.EqualError(t, err, "error querying groups for org: 1: boom")

	groups, err := models.LoadGroups(ctx, db, 1)
	require.NoError(t, err)

	tcs := []struct {
		ID    models.GroupID
		UUID  assets.GroupUUID
		Name  string
		Query string
	}{
		{testdata.DoctorsGroup.ID, testdata.DoctorsGroup.UUID, "Doctors", ""},
		{testdata.TestersGroup.ID, testdata.TestersGroup.UUID, "Testers", ""},
	}

	assert.Equal(t, 2, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*models.Group)
		assert.Equal(t, tc.UUID, group.UUID())
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}

func TestDynamicGroups(t *testing.T) {
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

	esServer := testsuite.NewMockElasticServer()
	defer esServer.Close()

	es, err := elastic.NewClient(
		elastic.SetURL(esServer.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err)

	contactHit := `{
		"_scroll_id": "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAbgc0WS1hqbHlfb01SM2lLTWJRMnVOSVZDdw==",
		"took": 2,
		"timed_out": false,
		"_shards": {
		  "total": 1,
		  "successful": 1,
		  "skipped": 0,
		  "failed": 0
		},
		"hits": {
		  "total": 1,
		  "max_score": null,
		  "hits": [
			{
			  "_index": "contacts",
			  "_type": "_doc",
			  "_id": "%d",
			  "_score": null,
			  "_routing": "1",
			  "sort": [
				15124352
			  ]
			}
		  ]
		}
	}`

	cathyHit := fmt.Sprintf(contactHit, testdata.Cathy.ID)
	bobHit := fmt.Sprintf(contactHit, testdata.Bob.ID)

	tcs := []struct {
		Query           string
		ESResponse      string
		ContactIDs      []models.ContactID
		EventContactIDs []models.ContactID
	}{
		{
			"cathy",
			cathyHit,
			[]models.ContactID{testdata.Cathy.ID},
			[]models.ContactID{},
		},
		{
			"bob",
			bobHit,
			[]models.ContactID{testdata.Bob.ID},
			[]models.ContactID{testdata.Bob.ID},
		},
		{
			"unchanged",
			bobHit,
			[]models.ContactID{testdata.Bob.ID},
			[]models.ContactID{testdata.Bob.ID},
		},
	}

	for _, tc := range tcs {
		err := models.UpdateGroupStatus(ctx, db, testdata.DoctorsGroup.ID, models.GroupStatusInitializing)
		assert.NoError(t, err)

		esServer.NextResponse = tc.ESResponse
		count, err := models.PopulateDynamicGroup(ctx, db, es, oa, testdata.DoctorsGroup.ID, tc.Query)
		assert.NoError(t, err, "error populating dynamic group for: %s", tc.Query)

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
