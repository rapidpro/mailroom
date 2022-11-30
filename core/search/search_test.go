package search_test

import (
	"testing"

	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetContactIDsForQueryPage(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	mockES := testsuite.NewMockElasticServer()
	defer mockES.Close()

	mockES.AddResponse(testdata.George.ID)
	mockES.AddResponse(testdata.George.ID)

	es := mockES.Client()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	tcs := []struct {
		Group             *testdata.Group
		ExcludeIDs        []models.ContactID
		Query             string
		Sort              string
		ExpectedESRequest string
		ExpectedContacts  []models.ContactID
		ExpectedTotal     int64
		ExpectedError     string
	}{
		{
			Group: testdata.ActiveGroup,
			Query: "george",
			ExpectedESRequest: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"group_ids": 1
								}
							},
							{
								"match": {
									"name": {
										"query": "george"
									}
								}
							}
						]
					}
				},
				"size": 50,
				"sort": [
					{
						"id": {
							"order": "desc"
						}
					}
				],
				"track_total_hits": true
			}`,
			ExpectedContacts: []models.ContactID{testdata.George.ID},
			ExpectedTotal:    1,
		},
		{
			Group:      testdata.BlockedGroup,
			ExcludeIDs: []models.ContactID{testdata.Bob.ID, testdata.Cathy.ID},
			Query:      "age > 32",
			Sort:       "-age",
			ExpectedESRequest: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"group_ids": 2
								}
							},
							{
								"nested": {
									"path": "fields",
									"query": {
										"bool": {
											"must": [
												{
													"term": {
														"fields.field": "903f51da-2717-47c7-a0d3-f2f32877013d"
													}
												},
												{
													"range": {
														"fields.number": {
															"from": 32,
															"include_lower": false,
															"include_upper": true,
															"to": null
														}
													}
												}
											]
										}
									}
								}
							}
						],
						"must_not": {
							"ids": {
								"type": "_doc",
								"values": [
									"10001",
									"10000"
								]
							}
						}
					}
				},
				"size": 50,
				"sort": [
					{
						"fields.number": {
							"nested": {
								"filter": {
									"term": {
										"fields.field": "903f51da-2717-47c7-a0d3-f2f32877013d"
									}
								},
								"path": "fields"
							},
							"order": "desc"
						}
					}
				],
				"track_total_hits": true
			}`,
			ExpectedContacts: []models.ContactID{testdata.George.ID},
			ExpectedTotal:    1,
		},
		{
			Group:         testdata.ActiveGroup,
			Query:         "goats > 2", // no such contact field
			ExpectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		group := oa.GroupByID(tc.Group.ID)

		_, ids, total, err := search.GetContactIDsForQueryPage(ctx, es, oa, group, tc.ExcludeIDs, tc.Query, tc.Sort, 0, 50)

		if tc.ExpectedError != "" {
			assert.EqualError(t, err, tc.ExpectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.ExpectedContacts, ids, "%d: ids mismatch", i)
			assert.Equal(t, tc.ExpectedTotal, total, "%d: total mismatch", i)

			test.AssertEqualJSON(t, []byte(tc.ExpectedESRequest), []byte(mockES.LastRequestBody), "%d: ES request mismatch", i)
		}
	}
}

func TestGetContactIDsForQuery(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	mockES := testsuite.NewMockElasticServer()
	defer mockES.Close()

	mockES.AddResponse(testdata.George.ID)
	mockES.AddResponse()
	mockES.AddResponse(testdata.George.ID)

	es, err := elastic.NewClient(elastic.SetURL(mockES.URL()), elastic.SetHealthcheck(false), elastic.SetSniff(false))
	require.NoError(t, err)

	oa, err := models.GetOrgAssets(ctx, rt, 1)
	require.NoError(t, err)

	tcs := []struct {
		query               string
		limit               int
		expectedRequestURL  string
		expectedRequestBody string
		mockedESResponse    string
		expectedContacts    []models.ContactID
		expectedError       string
	}{
		{
			query:              "george",
			limit:              -1,
			expectedRequestURL: "/_search/scroll",
			expectedRequestBody: `{
				"_source":false,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"status": "A"
								}
							},
							{
								"match": {
									"name": {
										"query": "george"
									}
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			expectedContacts: []models.ContactID{testdata.George.ID},
		}, {
			query:              "nobody",
			limit:              -1,
			expectedRequestURL: "/contacts/_search?routing=1&scroll=15m&size=10000",
			expectedRequestBody: `{
				"_source":false,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"status": "A"
								}
							},
							{
								"match": {
									"name": {
										"query": "nobody"
									}
								}
							}
						]
					}
				},
				"sort":["_doc"]
			}`,
			expectedContacts: []models.ContactID{},
		},
		{
			query:              "george",
			limit:              1,
			expectedRequestURL: "/contacts/_search?routing=1",
			expectedRequestBody: `{
				"_source": false,
				"from": 0,
				"query": {
					"bool": {
						"must": [
							{
								"term": {
									"org_id": 1
								}
							},
							{
								"term": {
									"is_active": true
								}
							},
							{
								"term": {
									"status": "A"
								}
							},
							{
								"match": {
									"name": {
										"query": "george"
									}
								}
							}
						]
					}
				},
				"size": 1
			}`,
			expectedContacts: []models.ContactID{testdata.George.ID},
		},
		{
			query:         "goats > 2", // no such contact field
			limit:         -1,
			expectedError: "error parsing query: goats > 2: can't resolve 'goats' to attribute, scheme or field",
		},
	}

	for i, tc := range tcs {
		ids, err := search.GetContactIDsForQuery(ctx, es, oa, tc.query, tc.limit)

		if tc.expectedError != "" {
			assert.EqualError(t, err, tc.expectedError)
		} else {
			assert.NoError(t, err, "%d: error encountered performing query", i)
			assert.Equal(t, tc.expectedContacts, ids, "%d: ids mismatch", i)

			assert.Equal(t, tc.expectedRequestURL, mockES.LastRequestURL, "%d: request URL mismatch", i)
			test.AssertEqualJSON(t, []byte(tc.expectedRequestBody), []byte(mockES.LastRequestBody), "%d: request body mismatch", i)
		}
	}
}
