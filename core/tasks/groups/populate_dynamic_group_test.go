package groups_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/groups"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/olivere/elastic"
	"github.com/stretchr/testify/require"
)

func TestPopulateTask(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()

	mes := testsuite.NewMockElasticServer()
	defer mes.Close()

	es, err := elastic.NewClient(
		elastic.SetURL(mes.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	require.NoError(t, err)

	mr := &mailroom.Mailroom{Config: config.Mailroom, DB: db, RP: testsuite.RP(), ElasticClient: es}

	mes.NextResponse = fmt.Sprintf(`{
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
				"sort": [15124352]
			}
			]
		}
	}`, models.CathyID)

	groupID := testdata.InsertContactGroup(t, db, models.Org1, "e52fee05-2f95-4445-aef6-2fe7dac2fd56", "Women", "gender = F")

	task := &groups.PopulateDynamicGroupTask{
		GroupID: groupID,
		Query:   "gender = F",
	}
	err = task.Perform(ctx, mr, models.Org1)
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, []interface{}{groupID}, 1)
}
