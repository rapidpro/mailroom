package search_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildStartQuery(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	dates.SetNowSource(dates.NewFixedNowSource(time.Date(2022, 4, 20, 15, 30, 45, 0, time.UTC)))
	defer dates.SetNowSource(dates.DefaultNowSource)

	oa := testdata.Org1.Load(rt)
	flow, err := oa.FlowByID(testdata.Favorites.ID)
	require.NoError(t, err)

	doctors := oa.GroupByID(testdata.DoctorsGroup.ID)
	testers := oa.GroupByID(testdata.TestersGroup.ID)

	tcs := []struct {
		groups       []*models.Group
		contactUUIDs []flows.ContactUUID
		urns         []urns.URN
		userQuery    string
		exclusions   search.Exclusions
		expected     string
	}{
		{
			groups:       []*models.Group{doctors, testers},
			contactUUIDs: []flows.ContactUUID{testdata.Cathy.UUID, testdata.George.UUID},
			urns:         []urns.URN{"tel:+1234567890", "telegram:9876543210"},
			exclusions:   search.Exclusions{},
			expected:     `group = "Doctors" OR group = "Testers" OR uuid = "6393abc0-283d-4c9b-a1b3-641a035c34bf" OR uuid = "8d024bcd-f473-4719-a00a-bd0bb1190135" OR tel = "+1234567890" OR telegram = "9876543210"`,
		},
		{
			groups:       []*models.Group{doctors},
			contactUUIDs: []flows.ContactUUID{testdata.Cathy.UUID},
			urns:         []urns.URN{"tel:+1234567890"},
			exclusions: search.Exclusions{
				NonActive:         true,
				InAFlow:           true,
				StartedPreviously: true,
				NotSeenSinceDays:  90,
			},
			expected: `(group = "Doctors" OR uuid = "6393abc0-283d-4c9b-a1b3-641a035c34bf" OR tel = "+1234567890") AND status = "active" AND flow = "" AND history != "Favorites" AND last_seen_on > 20-01-2022`,
		},
		{
			contactUUIDs: []flows.ContactUUID{testdata.Cathy.UUID},
			exclusions: search.Exclusions{
				NonActive: true,
			},
			expected: `uuid = "6393abc0-283d-4c9b-a1b3-641a035c34bf" AND status = "active"`,
		},
		{
			userQuery:  "gender = M",
			exclusions: search.Exclusions{},
			expected:   "gender = M",
		},
		{
			userQuery: "gender = M",
			exclusions: search.Exclusions{
				NonActive:         true,
				InAFlow:           true,
				StartedPreviously: true,
				NotSeenSinceDays:  30,
			},
			expected: `gender = M AND status = "active" AND flow = "" AND history != "Favorites" AND last_seen_on > 21-03-2022`,
		},
	}

	for _, tc := range tcs {
		actual := search.BuildStartQuery(oa.Env(), flow, tc.groups, tc.contactUUIDs, tc.urns, tc.userQuery, tc.exclusions)
		assert.Equal(t, tc.expected, actual)
	}
}
