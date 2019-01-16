package simulation

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

const (
	startBody = `
	{
		"org_id": 1,
		"trigger": {
			"contact": {
				"created_on": "2000-01-01T00:00:00.000000000-00:00",
				"fields": {},
				"id": 1234567,
				"language": "eng",
				"name": "Ben Haggerty",
				"timezone": "America/Guayaquil",
				"urns": [
					"tel:+12065551212"
				],
				"uuid": "ba96bf7f-bc2a-4873-a7c7-254d1927c4e3"
			},
			"environment": {
				"allowed_languages": [
					"eng",
					"fra"
				],
				"date_format": "YYYY-MM-DD",
				"default_language": "eng",
				"time_format": "hh:mm",
				"timezone": "America/Los_Angeles"
			},
			"flow": {
				"name": "Favorites",
				"uuid": "51e3c67d-8483-449c-abf7-25e50686f0db"
			},
			"triggered_on": "2000-01-01T00:00:00.000000000-00:00",
			"type": "manual"
		}
	}`

	resumeBody = `
	{
		"org_id": 1,
		"resume": {
			"contact": {
                "created_on": "2000-01-01T00:00:00.000000000-00:00",
                "fields": {},
                "id": 1234567,
                "language": "eng",
                "name": "Ben Haggerty",
                "timezone": "America/Guayaquil",
                "urns": [
                    "tel:+12065551212"
                ],
                "uuid": "ba96bf7f-bc2a-4873-a7c7-254d1927c4e3"
            },
            "environment": {
                "allowed_languages": [
                    "eng",
                    "fra"
                ],
                "date_format": "YYYY-MM-DD",
                "default_language": "eng",
                "time_format": "hh:mm",
                "timezone": "America/New_York"
            },
            "msg": {
                "channel": {
                    "name": "Nexmo",
                    "uuid": "c534272e-817d-4a78-a70c-f21df34407f8"
                },
                "text": "I like blue!",
                "urn": "tel:+12065551212",
                "uuid": "9bf91c2b-ce58-4cef-aacc-281e03f69ab5"
            },
            "resumed_on": "2000-01-01T00:00:00.000000000-00:00",
            "type": "msg"
		},
		"session": $$SESSION$$
	}`

	minLegacyDef = `
	{
		"flow": {
			"base_language": "eng",
			"action_sets": [{
				"y": 0,
				"x": 100,
				"destination": null,
				"uuid": "e41e7aad-de93-4cc0-ae56-d6af15ba1ac5",
				"actions": [{
					"msg": {
						"eng": "Hello world"
					},
					"type": "reply"
				}],
				"exit_uuid": "40c6cb36-bb44-479a-8ed1-d3f8df3a134d"
			}],
			"version": 8,
			"flow_type": "F",
			"entry": "e41e7aad-de93-4cc0-ae56-d6af15ba1ac5",
			"rule_sets": [],
			"metadata": {
				"uuid": "42362831-f376-4df1-b6d9-a80b102821d9",
				"expires": 10080,
				"revision": 1,
				"id": 41049,
				"name": "No ruleset flow",
				"saved_on": "2015-11-20T11:02:19.790131Z"
			}
		},
		"include_ui": true
	}
	`
)

func TestServer(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()

	// TODO: test custom flow definitions
	startSession := ""

	tcs := []struct {
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{"/mr/sim/start", "GET", "", 405, "illegal"},
		{"/mr/sim/start", "POST", startBody, 200, "What is your favorite color?"},
		{"/mr/sim/resume", "GET", "", 405, "illegal"},
		{"/mr/sim/resume", "POST", resumeBody, 200, "Good choice, I like Blue too! What is your favorite beer?"},
		{"/mr/flow/migrate", "GET", "", 405, "illegal"},
		{"/mr/flow/migrate", "POST", minLegacyDef, 200, `"type":"send_msg"`},
	}

	for i, tc := range tcs {
		var body io.Reader

		// in the case of a resume, we have to sub in our session body from our start
		if tc.Body == resumeBody {
			tc.Body = strings.Replace(tc.Body, "$$SESSION$$", startSession, -1)
		}

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

		// if this was a start and the start was successful
		if tc.Body == startBody && resp.StatusCode == 200 {
			// save the start session for use in our resume
			parsed := make(map[string]interface{})
			json.Unmarshal(content, &parsed)
			sessionJSON, _ := json.Marshal(parsed["session"])
			startSession = string(sessionJSON)
		}

		assert.True(t, strings.Contains(string(content), tc.Response), "%d: did not find string: %s in body: %s", i, tc.Response, string(content))
	}
}
