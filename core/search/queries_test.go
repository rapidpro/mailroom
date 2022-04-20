package search_test

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildStartQuery(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	oa := testdata.Org1.Load(rt)
	flow, err := oa.FlowByID(testdata.Favorites.ID)
	require.NoError(t, err)

	tcs := []struct {
		groupIDs   []models.GroupID
		contactIDs []models.ContactID
		urns       []urns.URN
		userQuery  string
		exclusions search.Exclusions
		expected   string
	}{
		{
			groupIDs:   []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			contactIDs: []models.ContactID{testdata.Cathy.ID, testdata.George.ID},
			urns:       []urns.URN{"tel:+1234567890", "telegram:9876543210"},
			exclusions: search.Exclusions{},
			expected:   `group = "Doctors" OR group = "Testers" OR id = 10000 OR id = 10002 OR tel = "+1234567890" OR telegram = "9876543210"`,
		},
		{
			groupIDs:   []models.GroupID{testdata.DoctorsGroup.ID},
			contactIDs: []models.ContactID{testdata.Cathy.ID},
			urns:       []urns.URN{"tel:+1234567890"},
			exclusions: search.Exclusions{
				NonActive:         true,
				InAFlow:           true,
				StartedPreviously: true,
				NotSeenRecently:   true,
			},
			expected: `(group = "Doctors" OR id = 10000 OR tel = "+1234567890") AND status = "active" AND flow = "" AND history != "Favorites" AND last_seen_on > 20-01-2022`,
		},
		{
			contactIDs: []models.ContactID{testdata.Cathy.ID},
			exclusions: search.Exclusions{
				NonActive: true,
			},
			expected: `id = 10000 AND status = "active"`,
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
				NotSeenRecently:   true,
			},
			expected: `gender = M AND status = "active" AND flow = "" AND history != "Favorites" AND last_seen_on > 20-01-2022`,
		},
	}

	for _, tc := range tcs {
		actual := search.BuildStartQuery(oa, flow, tc.groupIDs, tc.contactIDs, tc.urns, tc.userQuery, tc.exclusions)
		assert.Equal(t, tc.expected, actual)
	}
}
