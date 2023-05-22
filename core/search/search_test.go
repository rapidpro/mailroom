package search_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetContactIDsForQueryPage(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetElastic)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	tcs := []struct {
		group            *testdata.Group
		excludeIDs       []models.ContactID
		query            string
		sort             string
		expectedContacts []models.ContactID
		expectedTotal    int64
		expectedError    string
	}{
		{ // 0
			group:            testdata.ActiveGroup,
			query:            "george",
			expectedContacts: []models.ContactID{testdata.George.ID},
			expectedTotal:    1,
		},
		{ // 1
			group:            testdata.BlockedGroup,
			query:            "george",
			expectedContacts: []models.ContactID{},
			expectedTotal:    0,
		},
		{ // 2
			group:            testdata.ActiveGroup,
			query:            "age >= 30",
			sort:             "-age",
			expectedContacts: []models.ContactID{testdata.George.ID},
			expectedTotal:    1,
		},
		{ // 3
			group:            testdata.ActiveGroup,
			excludeIDs:       []models.ContactID{testdata.George.ID},
			query:            "age >= 30",
			sort:             "-age",
			expectedContacts: []models.ContactID{},
			expectedTotal:    0,
		},
		{ // 4
			group:         testdata.BlockedGroup,
			query:         "goats > 2", // no such contact field
			expectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		group := oa.GroupByID(tc.group.ID)

		_, ids, total, err := search.GetContactIDsForQueryPage(ctx, rt, oa, group, tc.excludeIDs, tc.query, tc.sort, 0, 50)

		if tc.expectedError != "" {
			assert.EqualError(t, err, tc.expectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.expectedContacts, ids, "%d: ids mismatch", i)
			assert.Equal(t, tc.expectedTotal, total, "%d: total mismatch", i)
		}
	}
}

func TestGetContactIDsForQuery(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetElastic)

	oa, err := models.GetOrgAssets(ctx, rt, 1)
	require.NoError(t, err)

	tcs := []struct {
		query            string
		limit            int
		expectedContacts []models.ContactID
		expectedError    string
	}{
		{
			query:            "george",
			limit:            -1,
			expectedContacts: []models.ContactID{testdata.George.ID},
		}, {
			query:            "nobody",
			limit:            -1,
			expectedContacts: []models.ContactID{},
		},
		{
			query:            "george",
			limit:            1,
			expectedContacts: []models.ContactID{testdata.George.ID},
		},
		{
			query:         "goats > 2", // no such contact field
			limit:         -1,
			expectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		ids, err := search.GetContactIDsForQuery(ctx, rt, oa, tc.query, tc.limit)

		if tc.expectedError != "" {
			assert.EqualError(t, err, tc.expectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.expectedContacts, ids, "%d: ids mismatch", i)
		}
	}
}
