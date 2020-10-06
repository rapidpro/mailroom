package models_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/lib/pq"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadGroups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.NewMockDB(testsuite.DB(), func(funcName string, call int) error {
		// fail first query for groups
		if funcName == "QueryxContext" && call == 0 {
			return errors.New("boom")
		}
		return nil
	})

	groups, err := models.LoadGroups(ctx, db, 1)
	require.EqualError(t, err, "error querying groups for org: 1: boom")

	groups, err = models.LoadGroups(ctx, db, 1)
	require.NoError(t, err)

	tcs := []struct {
		ID    models.GroupID
		UUID  assets.GroupUUID
		Name  string
		Query string
	}{
		{models.DoctorsGroupID, models.DoctorsGroupUUID, "Doctors", ""},
		{models.TestersGroupID, models.TestersGroupUUID, "Testers", ""},
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
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// insert an event on our campaign
	var eventID models.CampaignEventID
	testsuite.DB().Get(&eventID,
		`INSERT INTO campaigns_campaignevent(is_active, created_on, modified_on, uuid, "offset", unit, event_type, delivery_hour, 
											 campaign_id, created_by_id, modified_by_id, flow_id, relative_to_id, start_mode)
									   VALUES(TRUE, NOW(), NOW(), $1, 1000, 'W', 'F', -1, $2, 1, 1, $3, $4, 'I') RETURNING id`,
		uuids.New(), models.DoctorRemindersCampaignID, models.FavoritesFlowID, models.JoinedFieldID)

	// clear Cathy's value
	testsuite.DB().MustExec(
		`update contacts_contact set fields = fields - $2
		WHERE id = $1`, models.CathyID, models.JoinedFieldUUID)

	// and populate Bob's
	testsuite.DB().MustExec(
		fmt.Sprintf(`update contacts_contact set fields = fields ||
		'{"%s": { "text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00" }}'::jsonb
		WHERE id = $1`, models.JoinedFieldUUID), models.BobID)

	// clear our org cache so we reload org campaigns and events
	models.FlushCache()
	org, err := models.GetOrgAssets(ctx, db, models.Org1)
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

	cathyHit := fmt.Sprintf(contactHit, models.CathyID)
	bobHit := fmt.Sprintf(contactHit, models.BobID)

	tcs := []struct {
		Query           string
		ESResponse      string
		ContactIDs      []models.ContactID
		EventContactIDs []models.ContactID
	}{
		{
			"cathy",
			cathyHit,
			[]models.ContactID{models.CathyID},
			[]models.ContactID{},
		},
		{
			"bob",
			bobHit,
			[]models.ContactID{models.BobID},
			[]models.ContactID{models.BobID},
		},
		{
			"unchanged",
			bobHit,
			[]models.ContactID{models.BobID},
			[]models.ContactID{models.BobID},
		},
	}

	for _, tc := range tcs {
		err := models.UpdateGroupStatus(ctx, db, models.DoctorsGroupID, models.GroupStatusInitializing)
		assert.NoError(t, err)

		esServer.NextResponse = tc.ESResponse
		count, err := models.PopulateDynamicGroup(ctx, db, es, org, models.DoctorsGroupID, tc.Query)
		assert.NoError(t, err, "error populating dynamic group for: %s", tc.Query)

		assert.Equal(t, count, len(tc.ContactIDs))

		// assert the current group membership
		contactIDs, err := models.ContactIDsForGroupIDs(ctx, db, []models.GroupID{models.DoctorsGroupID})
		assert.Equal(t, tc.ContactIDs, contactIDs)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) from contacts_contactgroup WHERE id = $1 AND status = 'R'`,
			[]interface{}{models.DoctorsGroupID}, 1, "wrong number of contacts in group for query: %s", tc.Query)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) from campaigns_eventfire WHERE event_id = $1`,
			[]interface{}{eventID}, len(tc.EventContactIDs), "wrong number of contacts with events for query: %s", tc.Query)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) from campaigns_eventfire WHERE event_id = $1 AND contact_id = ANY($2)`,
			[]interface{}{eventID, pq.Array(tc.EventContactIDs)}, len(tc.EventContactIDs), "wrong contacts with events for query: %s", tc.Query)
	}
}
