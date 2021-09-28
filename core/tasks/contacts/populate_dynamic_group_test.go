package contacts_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/tasks/contacts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
)

func TestPopulateTask(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	mes := testsuite.NewMockElasticServer()
	defer mes.Close()
	es, err := elastic.NewClient(
		elastic.SetURL(mes.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	require.NoError(t, err)
	rt.ES = es

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
	}`, testdata.Cathy.ID)

	group := testdata.InsertContactGroup(db, testdata.Org1, "e52fee05-2f95-4445-aef6-2fe7dac2fd56", "Women", "gender = F")
	start := dates.Now()

	task := &contacts.PopulateDynamicGroupTask{
		GroupID: group.ID,
		Query:   "gender = F",
	}
	err = task.Perform(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	testsuite.AssertQuery(t, db, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, group.ID).Returns(1)
	testsuite.AssertQuery(t, db, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, group.ID).Returns(int64(testdata.Cathy.ID))
	testsuite.AssertQuery(t, db, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_on > $2`, testdata.Cathy.ID, start).Returns(1)
}
