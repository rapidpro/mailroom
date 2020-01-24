package models

import (
	"fmt"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

func TestGroups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	groups, err := loadGroups(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID    GroupID
		UUID  assets.GroupUUID
		Name  string
		Query string
	}{
		{DoctorsGroupID, DoctorsGroupUUID, "Doctors", ""},
		{TestersGroupID, TestersGroupUUID, "Testers", ""},
	}

	assert.Equal(t, 2, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*Group)
		assert.Equal(t, tc.UUID, group.UUID())
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}

func TestDynamicGroups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	esServer := search.NewMockElasticServer()
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

	georgeHit := fmt.Sprintf(contactHit, GeorgeID)
	bobHit := fmt.Sprintf(contactHit, BobID)

	tcs := []struct {
		Query      string
		ESResponse string
		ContactIDs []ContactID
	}{
		{
			"george",
			georgeHit,
			[]ContactID{GeorgeID},
		},
		{
			"bob",
			bobHit,
			[]ContactID{BobID},
		},
		{
			"unchanged",
			bobHit,
			[]ContactID{BobID},
		},
	}

	for _, tc := range tcs {
		err := UpdateGroupStatus(ctx, db, DoctorsGroupID, GroupStatusInitializing)
		assert.NoError(t, err)

		esServer.NextResponse = tc.ESResponse
		count, err := PopulateDynamicGroup(ctx, db, es, org, DoctorsGroupID, tc.Query)
		assert.NoError(t, err, "error populating dynamic group for: %s", tc.Query)

		assert.Equal(t, count, len(tc.ContactIDs))

		// assert the current group membership
		contactIDs, err := ContactIDsForGroupIDs(ctx, db, []GroupID{DoctorsGroupID})
		assert.Equal(t, tc.ContactIDs, contactIDs)

		testsuite.AssertQueryCount(t, db,
			`SELECT count(*) from contacts_contactgroup WHERE id = $1 AND status = 'R'`,
			[]interface{}{DoctorsGroupID}, 1)
	}
}
