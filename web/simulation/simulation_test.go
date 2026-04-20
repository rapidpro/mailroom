package simulation

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
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
				"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85"
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
                    "name": "Twitter",
                    "uuid": "0f661e8b-ea9d-4bd3-9953-d368340acf91"
                },
                "text": "$$MESSAGE$$",
                "urn": "tel:+12065551212",
                "uuid": "9bf91c2b-ce58-4cef-aacc-281e03f69ab5"
            },
            "resumed_on": "2000-01-01T00:00:00.000000000-00:00",
            "type": "msg"
		},
		"assets": {
			"channels": [
				{
					"uuid": "440099cf-200c-4d45-a8e7-4a564f4a0e8b",
					"name": "Test Channel",
					"address": "+18005551212",
					"schemes": ["tel"],
					"roles": ["send", "receive", "call"],
					"country": "US"
				}
			]
		},
		"session": $$SESSION$$
	}`

	customStartBody = `
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
			"msg": {
				"uuid": "2d611e17-fb22-457f-b802-b8f7ec5cda5b",
				"channel": {"uuid": "440099cf-200c-4d45-a8e7-4a564f4a0e8b", "name": "Test Channel"},
				"urn": "tel:+12065551212",
				"text": "hi there"
			},
			"flow": {
				"name": "Favorites",
				"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85"
			},
			"triggered_on": "2000-01-01T00:00:00.000000000-00:00",
			"type": "msg"
		},
		"flows": [
			{
				"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
				"definition": {
					"base_language": "eng",
					"action_sets": [{
						"y": 0,
						"x": 100,
						"destination": null,
						"uuid": "87edb79e-f46c-4970-bcd3-c715dfededb7",
						"actions": [{
							"uuid": "0aaa6871-15fb-408c-9f33-2d7d8f6d5baf",
							"msg": {
								"eng": "Your channel is @channel.name"
							},
							"type": "reply"
						}],
						"exit_uuid": "40c6cb36-bb44-479a-8ed1-d3f8df3a134d"
					}],
					"version": 8,
					"flow_type": "F",
					"entry": "87edb79e-f46c-4970-bcd3-c715dfededb7",
					"rule_sets": [],
					"metadata": {
						"uuid": "9de3663f-c5c5-4c92-9f45-ecbc09abcc85",
						"expires": 10080,
						"revision": 1,
						"id": 41049,
						"name": "No ruleset flow",
						"saved_on": "2015-11-20T11:02:19.790131Z"
					}
				}
			}
		],
		"assets": {
			"channels": [
				{
					"uuid": "440099cf-200c-4d45-a8e7-4a564f4a0e8b",
					"name": "Test Channel",
					"address": "+12065551441",
					"schemes": ["tel"],
					"roles": ["send", "receive", "call"],
					"country": "US"
				}
			]
		}
	}`
)

func TestServer(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	wg := &sync.WaitGroup{}

	server := web.NewServer(ctx, rt, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()

	var session json.RawMessage

	// add a trigger for our campaign flow with 'trigger'
	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.CampaignFlow, []string{"trigger"}, models.MatchOnly, nil, nil, nil)

	// and a trigger which will trigger an IVR flow
	testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.IVRFlow, []string{"ivr"}, models.MatchOnly, nil, nil, nil)

	// also add a catch all
	testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.CampaignFlow, nil, nil, nil)

	tcs := []struct {
		URL              string
		Method           string
		Body             string
		Message          string
		ExpectedStatus   int
		ExpectedResponse string
	}{
		{"/mr/sim/start", "GET", "", "", 405, "illegal"},
		{"/mr/sim/start", "POST", startBody, "", 200, "What is your favorite color?"},
		{"/mr/sim/resume", "POST", resumeBody, "I like blue!", 200, "Good choice, I like Blue too! What is your favorite beer?"},

		// start with a definition of the flow to override what we have in assets
		{"/mr/sim/start", "POST", customStartBody, "", 200, "Your channel is Test Channel"},

		// start regular flow again but resume with a message that matches the campaign flow trigger
		{"/mr/sim/start", "POST", startBody, "", 200, "What is your favorite color?"},
		{"/mr/sim/resume", "POST", resumeBody, "trigger", 200, "it is time to consult with your patients"},
		{"/mr/sim/resume", "POST", resumeBody, "I like blue!", 200, "it is time to consult with your patients"},

		// start favorties again but this time resume with a message that matches the IVR flow trigger
		{"/mr/sim/start", "POST", startBody, "", 200, "What is your favorite color?"},
		{"/mr/sim/resume", "POST", resumeBody, "ivr", 200, "Hello there. Please enter one or two."},
	}

	for i, tc := range tcs {
		bodyStr := strings.Replace(tc.Body, "$$MESSAGE$$", tc.Message, -1)

		// in the case of a resume, we have to sub in our session body from our start
		bodyStr = strings.Replace(bodyStr, "$$SESSION$$", string(session), -1)

		var body io.Reader
		if tc.Body != "" {
			body = bytes.NewReader([]byte(bodyStr))
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.ExpectedStatus, resp.StatusCode, "%d: unexpected status", i)

		content, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		// if this was a success, save our session
		if resp.StatusCode == 200 {
			// save the session for use in a resume
			parsed := make(map[string]any)
			jsonx.MustUnmarshal(content, &parsed)
			session = jsonx.MustMarshal(parsed["session"])

			context, hasContext := parsed["context"]
			if hasContext {
				assert.Contains(t, context, "contact")
				assert.Contains(t, context, "globals")
			}
		}

		assert.Contains(t, string(content), tc.ExpectedResponse, "%d: did not find expected response content")
	}
}
