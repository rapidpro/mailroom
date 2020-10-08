package groups_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/tasks/groups"
	"github.com/nyaruka/mailroom/testsuite"

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

	var groupID models.GroupID
	err = db.Get(&groupID,
		`INSERT INTO contacts_contactgroup(uuid, org_id, group_type, name, query, status, is_active, created_by_id, created_on, modified_by_id, modified_on) 
 						            VALUES($1,   $2,     'U',        $3,   $4,    'R',    TRUE, 1, NOW(), 1, NOW()) RETURNING id`,
		uuids.New(), models.Org1, "Women", "gender = F",
	)
	require.NoError(t, err)

	task := &groups.PopulateDynamicGroupTask{
		OrgID:   models.Org1,
		GroupID: groupID,
		Query:   "gender = F",
	}
	err = task.Perform(ctx, mr)
	require.NoError(t, err)

	testsuite.AssertQueryCount(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, []interface{}{groupID}, 1)
}
