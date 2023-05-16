package search_test

import (
	"fmt"
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
	ctx, rt, mocks, close := testsuite.RuntimeWithSearch()
	defer close()

	group1 := testdata.InsertContactGroup(rt, testdata.Org1, "a85acec9-3895-4ffd-87c1-c69a25781a85", "Group 1", "", testdata.George, testdata.Alexandria)
	group2 := testdata.InsertContactGroup(rt, testdata.Org1, "eb578345-595e-4e36-a68b-6941e242cdbb", "Group 2", "", testdata.George)

	defer testsuite.Reset(testsuite.ResetData)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	tcs := []struct {
		flow               *testdata.Flow
		recipients         *search.Recipients
		limit              int
		expectedElastic    string
		expectedCreatedIDs []models.ContactID
	}{
		{ // 0
			recipients:      &search.Recipients{},
			expectedElastic: "",
		},
		{ // 1
			recipients: &search.Recipients{
				ContactIDs: []models.ContactID{testdata.Bob.ID},
				GroupIDs:   []models.GroupID{group1.ID},
				Query:      `name = "Cathy" OR name = "Bob"`,
			},
			limit: -1,
			expectedElastic: `{
				"_source": false,
				"query": {
					"bool": {
						"must": [
							{"term": {"org_id": 1}},
							{"term": {"is_active": true}},
							{"term": {"status": "A"}},
							{
								"bool": {
									"should": [
										{"term": {"group_ids": 30000}},
										{"term": {"uuid": "b699a406-7e44-49be-9f01-1a82893e8a10"}},
										{"term": {"name.keyword": "Cathy"}},
										{"term": {"name.keyword": "Bob"}}
									]
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			expectedCreatedIDs: []models.ContactID{},
		},
		{ // 2
			recipients: &search.Recipients{
				ContactIDs:      []models.ContactID{testdata.Bob.ID},
				ExcludeGroupIDs: []models.GroupID{group2.ID},
			},
			limit: -1,
			expectedElastic: `{
				"_source": false,
				"query": {
					"bool": {
						"must": [
							{"term": {"org_id": 1}},
							{"term": {"is_active": true}},
							{"term": {"status": "A"}},
							{
								"bool": {
									"must": [
										{"term": {"uuid": "b699a406-7e44-49be-9f01-1a82893e8a10"}},
										{
											"bool": {
												"must_not": {
													"term": {"group_ids": 30001}
												}
											}
										}
									]
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			expectedCreatedIDs: []models.ContactID{},
		},
		{ // 3
			recipients: &search.Recipients{
				ContactIDs: []models.ContactID{testdata.Bob.ID},
				URNs:       []urns.URN{"tel:+1234567890", "tel:+1234567891"},
			},
			limit: -1,
			expectedElastic: `{
				"_source": false,
				"query": {
					"bool": {
						"must": [
							{"term": {"org_id": 1}}, 
							{"term": {"is_active": true}}, 
							{"term": {"status": "A"}}, 
							{"term": {"uuid": "b699a406-7e44-49be-9f01-1a82893e8a10"}}
						]
					}
				},
				"sort":["_doc"]
			}`,
			expectedCreatedIDs: []models.ContactID{30000, 30001},
		},
		{ // 4
			recipients: &search.Recipients{
				Query: `name = "Cathy" OR name = "Bob"`,
			},
			limit: 1,
			expectedElastic: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{"term": {"org_id": 1}}, 
							{"term": {"is_active": true}}, 
							{"term": {"status": "A"}}, 
							{
								"bool": {
									"should": [
										{"term": {"name.keyword": "Cathy"}}, 
										{"term": {"name.keyword": "Bob"}}
									]
								}
							}
						]
					}
				},
				"size": 1
			}`,
			expectedCreatedIDs: []models.ContactID{},
		},
	}

	for i, tc := range tcs {
		var flow *models.Flow
		if tc.flow != nil {
			flow = tc.flow.Load(rt, oa)
		}

		mocks.ES.AddResponse() // only created ids will be returned (those don't go thru ES)

		actualIDs, err := search.ResolveRecipients(ctx, rt, oa, flow, tc.recipients, tc.limit)
		assert.NoError(t, err)

		fmt.Println(mocks.ES.LastRequestBody)

		if tc.expectedElastic != "" {
			assert.JSONEq(t, tc.expectedElastic, mocks.ES.LastRequestBody, "%d: elastic request mismatch", i)
		} else {
			assert.Equal(t, "", mocks.ES.LastRequestBody)
		}
		assert.ElementsMatch(t, tc.expectedCreatedIDs, actualIDs, "%d: contact ids mismatch", i)

		mocks.ES.LastRequestBody = ""
	}
}
