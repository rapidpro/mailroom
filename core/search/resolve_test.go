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

func TestResolveRecipients(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	group1 := testdata.InsertContactGroup(rt, testdata.Org1, "a85acec9-3895-4ffd-87c1-c69a25781a85", "Group 1", "", testdata.George, testdata.Alexandria)
	group2 := testdata.InsertContactGroup(rt, testdata.Org1, "eb578345-595e-4e36-a68b-6941e242cdbb", "Group 2", "", testdata.George)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	tcs := []struct {
		recipients  *search.Recipients
		expectedIDs []models.ContactID
	}{
		{
			recipients:  &search.Recipients{},
			expectedIDs: []models.ContactID{},
		},
		{
			recipients: &search.Recipients{
				ContactIDs: []models.ContactID{testdata.Bob.ID},
				GroupIDs:   []models.GroupID{group1.ID},
				Query:      `name = "Cathy" OR name = "Bob"`,
				QueryLimit: 10,
			},
			expectedIDs: []models.ContactID{testdata.Bob.ID, testdata.George.ID, testdata.Alexandria.ID, testdata.Cathy.ID},
		},
		{
			recipients: &search.Recipients{
				ContactIDs:      []models.ContactID{testdata.George.ID, testdata.Bob.ID},
				ExcludeGroupIDs: []models.GroupID{group2.ID},
			},
			expectedIDs: []models.ContactID{testdata.Bob.ID},
		},
		{
			recipients: &search.Recipients{
				ContactIDs: []models.ContactID{testdata.Bob.ID},
				URNs:       []urns.URN{"tel:+1234567890", "tel:+1234567891"},
			},
			expectedIDs: []models.ContactID{testdata.Bob.ID, 30000, 30001},
		},
		{
			recipients: &search.Recipients{
				Query:      `name = "Cathy" OR name = "Bob"`,
				QueryLimit: 1,
			},
			expectedIDs: []models.ContactID{testdata.Cathy.ID},
		},
	}

	for i, tc := range tcs {
		actualIDs, err := search.ResolveRecipients(ctx, rt, oa, tc.recipients)
		assert.NoError(t, err)
		assert.ElementsMatch(t, tc.expectedIDs, actualIDs, "contact ids mismatch in %d", i)
	}
}
