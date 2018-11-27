package web

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/testsuite"
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
		"session": {"environment": {"date_format":"YYYY-MM-DD","time_format":"hh:mm","timezone":"America/Los_Angeles","default_language":"eng","allowed_languages":["eng","fra"],"redaction_policy":"none"},"trigger":{"type":"manual","environment":{"date_format":"YYYY-MM-DD","time_format":"hh:mm","timezone":"America/Los_Angeles","default_language":"eng","allowed_languages":["eng","fra"],"redaction_policy":"none"},"flow":{"uuid":"51e3c67d-8483-449c-abf7-25e50686f0db","name":"Favorites"},"contact":{"uuid":"ba96bf7f-bc2a-4873-a7c7-254d1927c4e3","id":1234567,"name":"Ben Haggerty","language":"eng","timezone":"America/Guayaquil","created_on":"2000-01-01T00:00:00Z","urns":["tel:+12065551212"]},"triggered_on":"2000-01-01T00:00:00Z"},"contact":{"uuid":"ba96bf7f-bc2a-4873-a7c7-254d1927c4e3","id":1234567,"name":"Ben Haggerty","language":"eng","timezone":"America/Guayaquil","created_on":"2000-01-01T00:00:00Z","urns":["tel:+12065551212"],"groups":[{"uuid":"caae117e-c26c-4625-96d5-ec4a0e7b8cdb","name":"Unregistered (Dynamic)"}]},"runs":[{"uuid":"27f05877-a718-403f-b6bf-dcf4a9afc824","flow":{"uuid":"51e3c67d-8483-449c-abf7-25e50686f0db","name":"Favorites"},"path":[{"uuid":"054ca842-a577-482c-a90f-1dbd37508a5d","node_uuid":"b9ecb56c-9879-473c-9de2-620a40b22ce6","exit_uuid":"7edf43ec-f7d4-4256-b086-778026df4e75","arrived_on":"2018-11-27T12:05:18.6555-05:00"},{"uuid":"f73e6f6e-e860-4316-bf1f-69b30f9fe3bf","node_uuid":"a5d6a9c0-a76f-48e1-b4ea-2096d3f6a5ec","arrived_on":"2018-11-27T12:05:18.656119-05:00"}],"events":[{"type":"msg_created","created_on":"2018-11-27T12:05:18.655966-05:00","step_uuid":"054ca842-a577-482c-a90f-1dbd37508a5d","msg":{"uuid":"e2500f6f-1b88-426d-acb6-c2b16706f367","urn":"tel:+12065551212","channel":{"uuid":"ac4c718a-db3f-4d8a-ae43-321f1a5bd44a","name":"Android"},"text":"What is your favorite color?"}},{"type":"msg_wait","created_on":"2018-11-27T12:05:18.656121-05:00","step_uuid":"f73e6f6e-e860-4316-bf1f-69b30f9fe3bf"}],"status":"waiting","created_on":"2018-11-27T12:05:18.655488-05:00","modified_on":"2018-11-27T12:05:18.656158-05:00","expires_on":"2018-11-28T00:05:18.655494-05:00","exited_on":null}],"status":"waiting","wait":{"type":"msg"}}
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

	server := NewServer(ctx, db, rp, config.Mailroom, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()

	// TODO: test custom flow definitions

	tcs := []struct {
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{"/arst", "GET", "", 404, "not found"},
		{"/", "POST", "", 405, "illegal"},
		{"/", "GET", "", 200, "mailroom"},
		{"/mr/", "POST", "", 405, "illegal"},
		{"/mr/", "GET", "", 200, "mailroom"},
		{"/mr/sim/start", "GET", "", 405, "illegal"},
		{"/mr/sim/start", "POST", startBody, 200, "What is your favorite color?"},
		{"/mr/sim/resume", "GET", "", 405, "illegal"},
		{"/mr/sim/resume", "POST", resumeBody, 200, "Good choice, I like Blue too! What is your favorite beer?"},
		{"/mr/flow/migrate", "GET", "", 405, "illegal"},
		{"/mr/flow/migrate", "POST", minLegacyDef, 200, `"type":"send_msg"`},
	}

	for i, tc := range tcs {
		var body io.Reader
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

		assert.True(t, strings.Contains(string(content), tc.Response), "%d: did not find string: %s in body: %s", i, tc.Response, string(content))
	}
}
