package tasks_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/models"
	_ "github.com/nyaruka/mailroom/core/runner/handlers"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/require"
)

func TestPopulateGroupTask(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetAll)

	group1 := testdb.InsertContactGroup(t, rt, testdb.Org1, "e52fee05-2f95-4445-aef6-2fe7dac2fd56", "Women", "gender = F")
	group2 := testdb.InsertContactGroup(t, rt, testdb.Org1, "8d1c25ff-d9b3-43c4-9abe-7ef3d2fc6c1a", "Invalid", "!!!", testdb.Bob)

	// insert a campaign for group1 with a flow point relative to the joined field
	campaign := testdb.InsertCampaign(t, rt, testdb.Org1, "Women Campaign", group1)
	point := testdb.InsertCampaignFlowPoint(t, rt, campaign, testdb.Favorites, testdb.JoinedField, 1000, "W")

	// give Ann a value for the joined field so she qualifies for campaign fires
	rt.DB.MustExec(
		fmt.Sprintf(`UPDATE contacts_contact SET fields = fields || '{"%s": {"text": "2029-09-15T12:00:00+00:00", "datetime": "2029-09-15T12:00:00+00:00"}}'::jsonb WHERE id = $1`, testdb.JoinedField.UUID),
		testdb.Ann.ID,
	)

	testsuite.ReindexElastic(t, rt)

	// reload org assets to include new groups and campaign
	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdb.Org1.ID, models.RefreshGroups|models.RefreshCampaigns)
	require.NoError(t, err)

	start := dates.Now()

	// test 1: valid query queues batch tasks which populate group and create campaign fires
	task1 := &tasks.PopulateGroup{
		GroupID: group1.ID,
		Query:   "gender = F",
	}
	err = task1.Perform(ctx, rt, oa)
	require.NoError(t, err)

	// group should be evaluating after parent task completes
	assertdb.Query(t, rt.DB, `SELECT status FROM contacts_contactgroup WHERE id = $1`, group1.ID).Returns("V")

	// flush queued batch tasks
	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT status FROM contacts_contactgroup WHERE id = $1`, group1.ID).Returns("R")
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, group1.ID).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, group1.ID).Returns(int64(testdb.Ann.ID))
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contact WHERE id = $1 AND modified_on > $2`, testdb.Ann.ID, start).Returns(1)

	// verify campaign fire was created for Ann
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contactfire WHERE fire_type = 'C' AND scope = $1 AND contact_id = $2`,
		fmt.Sprintf("%d:1", point.ID), testdb.Ann.ID).Returns(1)

	// test 2: invalid query marks group as invalid directly (no batch tasks)
	task2 := &tasks.PopulateGroup{
		GroupID: group2.ID,
		Query:   "!!!",
	}
	err = task2.Perform(ctx, rt, oa)
	require.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT status FROM contacts_contactgroup WHERE id = $1`, group2.ID).Returns("X")
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contactgroup_contacts WHERE contactgroup_id = $1`, group2.ID).Returns(0)
}
