package contact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/olivere/elastic"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	es := search.NewMockElasticServer()
	defer es.Close()

	client, err := elastic.NewClient(
		elastic.SetURL(es.URL()),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err)

	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, client, wg)
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
	}`, models.CathyID)

	tcs := []struct {
		URL        string
		Method     string
		Body       string
		Status     int
		Error      string
		Hits       []models.ContactID
		Query      string
		Fields     []string
		ESResponse string
	}{
		{"/mr/contact/search", "GET", "", 405, "illegal method: GET", nil, "", nil, ""},
		{
			"/mr/contact/search", "POST",
			fmt.Sprintf(`{"org_id": 1, "query": "birthday = tomorrow", "group_uuid": "%s"}`, models.AllContactsGroupUUID),
			400, "can't resolve 'birthday' to attribute, scheme or field",
			nil, "", nil, "",
		},
		{
			"/mr/contact/search", "POST",
			fmt.Sprintf(`{"org_id": 1, "query": "age > tomorrow", "group_uuid": "%s"}`, models.AllContactsGroupUUID),
			400, "can't convert 'tomorrow' to a number",
			nil, "", nil, "",
		},
		{
			"/mr/contact/search", "POST",
			fmt.Sprintf(`{"org_id": 1, "query": "Cathy", "group_uuid": "%s"}`, models.AllContactsGroupUUID),
			200,
			"",
			[]models.ContactID{models.CathyID},
			`name ~ "Cathy"`,
			[]string{"name"},
			singleESResponse,
		},
		{
			"/mr/contact/search", "POST",
			fmt.Sprintf(`{"org_id": 1, "query": "AGE = 10 and gender = M", "group_uuid": "%s"}`, models.AllContactsGroupUUID),
			200,
			"",
			[]models.ContactID{models.CathyID},
			`age = 10 AND gender = "M"`,
			[]string{"age", "gender"},
			singleESResponse,
		},
		{
			"/mr/contact/search", "POST",
			fmt.Sprintf(`{"org_id": 1, "query": "", "group_uuid": "%s"}`, models.AllContactsGroupUUID),
			200,
			"",
			[]models.ContactID{models.CathyID},
			``,
			[]string{},
			singleESResponse,
		},
	}

	for i, tc := range tcs {
		var body io.Reader
		es.NextResponse = tc.ESResponse

		if tc.Body != "" {
			body = bytes.NewReader([]byte(tc.Body))
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.Status, resp.StatusCode, "%d: unexpected status", i)

		content, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		// on 200 responses parse them
		if resp.StatusCode == 200 {
			r := &searchResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.Hits, r.ContactIDs)
			assert.Equal(t, tc.Query, r.Query)
			assert.Equal(t, tc.Fields, r.Fields)
		} else {
			r := &web.ErrorResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.Error, r.Error)
		}
	}
}

func TestParse(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	time.Sleep(time.Second)

	defer server.Stop()

	tcs := []struct {
		URL    string
		Method string
		Body   string
		Status int
		Error  string
		Query  string
		Fields []string
	}{
		{
			"/mr/contact/parse_query", "GET",
			"",
			405, "illegal method: GET",
			"", nil,
		},
		{
			"/mr/contact/parse_query", "POST",
			`{"org_id": 1, "query": "birthday = tomorrow"}`,
			400, "can't resolve 'birthday' to attribute, scheme or field",
			"", nil,
		},
		{
			"/mr/contact/parse_query", "POST",
			`{"org_id": 1, "query": "age > 10"}`,
			200, "",
			"age > 10", []string{"age"},
		},
	}

	for i, tc := range tcs {
		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, bytes.NewReader([]byte(tc.Body)))
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.Status, resp.StatusCode, "%d: unexpected status", i)

		content, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		// on 200 responses parse them
		if resp.StatusCode == 200 {
			r := &searchResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.Query, r.Query)
		} else {
			r := &web.ErrorResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.Error, r.Error)
		}
	}
}
