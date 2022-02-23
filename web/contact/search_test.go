package contact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/test"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	wg := &sync.WaitGroup{}

	es := testsuite.NewMockElasticServer()
	defer es.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(es.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err)

	rt.ES = client

	server := web.NewServer(ctx, rt, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()

	singleESResponse := fmt.Sprintf(`{
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
			  "sort": [
				15124352
			  ]
			}
		  ]
		}
	}`, testdata.Cathy.ID)

	tcs := []struct {
		method               string
		url                  string
		body                 string
		esResponse           string
		expectedStatus       int
		expectedError        string
		expectedHits         []models.ContactID
		expectedQuery        string
		expectedAttributes   []string
		expectedFields       []*assets.FieldReference
		expectedSchemes      []string
		expectedAllowAsGroup bool
		expectedESRequest    string
	}{
		{
			method:         "GET",
			url:            "/mr/contact/search",
			expectedStatus: 405,
			expectedError:  "illegal method: GET",
		},
		{
			method:         "POST",
			url:            "/mr/contact/search",
			body:           fmt.Sprintf(`{"org_id": 1, "query": "birthday = tomorrow", "group_uuid": "%s"}`, testdata.AllContactsGroup.UUID),
			expectedStatus: 400,
			expectedError:  "can't resolve 'birthday' to attribute, scheme or field",
		},
		{
			method:         "POST",
			url:            "/mr/contact/search",
			body:           fmt.Sprintf(`{"org_id": 1, "query": "age > tomorrow", "group_uuid": "%s"}`, testdata.AllContactsGroup.UUID),
			expectedStatus: 400,
			expectedError:  "can't convert 'tomorrow' to a number",
		},
		{
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "Cathy", "group_uuid": "%s"}`, testdata.AllContactsGroup.UUID),
			esResponse:           singleESResponse,
			expectedStatus:       200,
			expectedHits:         []models.ContactID{testdata.Cathy.ID},
			expectedQuery:        `name ~ "Cathy"`,
			expectedAttributes:   []string{"name"},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
		{
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "Cathy", "group_uuid": "%s", "exclude_ids": [%d, %d]}`, testdata.AllContactsGroup.UUID, testdata.Bob.ID, testdata.George.ID),
			esResponse:           singleESResponse,
			expectedStatus:       200,
			expectedHits:         []models.ContactID{testdata.Cathy.ID},
			expectedQuery:        `name ~ "Cathy"`,
			expectedAttributes:   []string{"name"},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
			expectedESRequest: `{
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
									"groups": "d1ee73f0-bdb5-47ce-99dd-0c95d4ebf008"
								}
							},
							{
								"match": {
									"name": {
										"query": "cathy"
									}
								}
							}
						],
						"must_not": {
							"ids": {
								"type": "_doc",
								"values": [
									"10001", "10002"
								]
							}
						}
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
		},
		{
			method:             "POST",
			url:                "/mr/contact/search",
			body:               fmt.Sprintf(`{"org_id": 1, "query": "AGE = 10 and gender = M", "group_uuid": "%s"}`, testdata.AllContactsGroup.UUID),
			esResponse:         singleESResponse,
			expectedStatus:     200,
			expectedHits:       []models.ContactID{testdata.Cathy.ID},
			expectedQuery:      `age = 10 AND gender = "M"`,
			expectedAttributes: []string{},
			expectedFields: []*assets.FieldReference{
				assets.NewFieldReference("age", "Age"),
				assets.NewFieldReference("gender", "Gender"),
			},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
		{
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "", "group_uuid": "%s"}`, testdata.AllContactsGroup.UUID),
			esResponse:           singleESResponse,
			expectedStatus:       200,
			expectedHits:         []models.ContactID{testdata.Cathy.ID},
			expectedQuery:        ``,
			expectedAttributes:   []string{},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
	}

	for i, tc := range tcs {
		var body io.Reader
		es.NextResponse = tc.esResponse

		if tc.body != "" {
			body = bytes.NewReader([]byte(tc.body))
		}

		req, err := http.NewRequest(tc.method, "http://localhost:8090"+tc.url, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "%d: unexpected status", i)

		content, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		// on 200 responses parse them
		if resp.StatusCode == 200 {
			r := &searchResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedHits, r.ContactIDs)
			assert.Equal(t, tc.expectedQuery, r.Query)

			if len(tc.expectedAttributes) > 0 || len(tc.expectedFields) > 0 || len(tc.expectedSchemes) > 0 {
				assert.Equal(t, tc.expectedAttributes, r.Metadata.Attributes)
				assert.Equal(t, tc.expectedFields, r.Metadata.Fields)
				assert.Equal(t, tc.expectedSchemes, r.Metadata.Schemes)
				assert.Equal(t, tc.expectedAllowAsGroup, r.Metadata.AllowAsGroup)
			}

			if tc.expectedESRequest != "" {
				test.AssertEqualJSON(t, []byte(tc.expectedESRequest), []byte(es.LastRequestBody), "elastic request mismatch")
			}
		} else {
			r := &web.ErrorResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedError, r.Error)
		}
	}
}

func TestParseQuery(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)

	web.RunWebTests(t, ctx, rt, "testdata/parse_query.json", nil)
}
