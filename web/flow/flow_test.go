package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

const (
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
					"uuid": "0aaa6871-15fb-408c-9f33-2d7d8f6d5baf",
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

	validateWithValidLegacyFlow = `
	{
		"org_id": 1,
		"flow": {
			"entry": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
			"action_sets": [
				{
				"uuid": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
				"x": 107,
				"y": 0,
				"destination": null,
				"actions": [
					{
						"type": "add_group",
						"uuid": "23337aa9-0d3d-4e70-876e-9a2633d1e5e4",
						"groups": [
							{
							"uuid": "5e9d8fab-5e7e-4f51-b533-261af5dea70d",
							"name": "Testers"
							}
						]
					},
					{
						"type": "reply",
						"uuid": "05a5cb7c-bb8a-4ad9-af90-ef9887cc370e",
						"msg": {
							"eng": "Your birthdate is @contact.birthdate"
						},
						"media": {},
						"quick_replies": [],
						"send_all": false
					}
				],
				"exit_uuid": "d3f3f024-a90e-43a5-bd5a-7056f5bea699"
				}
			],
			"rule_sets": [],
			"base_language": "eng",
			"flow_type": "M",
			"version": "11.12",
			"metadata": {
				"expires": 10080,
				"saved_on": "2019-03-04T17:37:06.873734Z",
				"uuid": "8f107d42-7416-4cf2-9a51-9490361ad517",
				"name": "Valid Legacy Flow",
				"revision": 106
			}
		}
	}`

	validateWithInvalidLegacyFlow = `
	{
		"org_id": 1,
		"flow": {
			"entry": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
			"action_sets": [
				{
				"uuid": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
				"x": 107,
				"y": 0,
				"destination": null,
				"actions": [
					{
						"type": "add_group",
						"uuid": "23337aa9-0d3d-4e70-876e-9a2633d1e5e4",
						"groups": [
							{
							"uuid": "1465eb20-066d-4933-a8b4-62fe7b19fd39",
							"name": "I Don't Exist"
							}
						]
					},
					{
						"type": "reply",
						"uuid": "05a5cb7c-bb8a-4ad9-af90-ef9887cc370e",
						"msg": {
							"eng": "Your birthdate is @contact.birthdate"
						},
						"media": {},
						"quick_replies": [],
						"send_all": false
					}
				],
				"exit_uuid": "d3f3f024-a90e-43a5-bd5a-7056f5bea699"
				}
			],
			"rule_sets": [],
			"base_language": "eng",
			"flow_type": "M",
			"version": "11.12",
			"metadata": {
				"expires": 10080,
				"saved_on": "2019-03-04T17:37:06.873734Z",
				"uuid": "8f107d42-7416-4cf2-9a51-9490361ad517",
				"name": "Valid Legacy Flow",
				"revision": 106
			}
		}
	}`

	validateWithValidFlow = `
	{
		"org_id": 1,
		"flow": {
			"uuid": "8f107d42-7416-4cf2-9a51-9490361ad517",
			"name": "Valid Legacy Flow",
			"spec_version": "12.0.0",
			"language": "eng",
			"type": "messaging",
			"revision": 106,
			"expire_after_minutes": 10080,
			"localization": {},
			"nodes": [
				{
					"uuid": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
					"actions": [
						{
							"type": "add_contact_groups",
							"uuid": "23337aa9-0d3d-4e70-876e-9a2633d1e5e4",
							"groups": [
								{
									"uuid": "5e9d8fab-5e7e-4f51-b533-261af5dea70d",
									"name": "Testers"
								}
							]
						},
						{
							"type": "send_msg",
							"uuid": "05a5cb7c-bb8a-4ad9-af90-ef9887cc370e",
							"text": "Your birthdate is @contact.fields.birthdate"
						}
					],
					"exits": [
						{
							"uuid": "d3f3f024-a90e-43a5-bd5a-7056f5bea699"
						}
					]
				}
			]
		}
	}`

	validateWithInvalidFlow = `
	{
		"org_id": 1,
		"flow": {
			"uuid": "8f107d42-7416-4cf2-9a51-9490361ad517",
			"name": "Valid Legacy Flow",
			"spec_version": "12.0.0",
			"language": "eng",
			"type": "messaging",
			"revision": 106,
			"expire_after_minutes": 10080,
			"localization": {},
			"nodes": [
				{
					"uuid": "6fde1a09-3997-47dd-aff0-92e8aff3a642",
					"actions": [
						{
							"type": "add_contact_groups",
							"uuid": "23337aa9-0d3d-4e70-876e-9a2633d1e5e4",
							"groups": [
								{
									"uuid": "5e9d8fab-5e7e-4f51-b533-261af5dea70d",
									"name": "Testers"
								}
							]
						},
						{
							"type": "send_msg",
							"uuid": "05a5cb7c-bb8a-4ad9-af90-ef9887cc370e",
							"text": "Your birthdate is @contact.fields.birthdate"
						}
					],
					"exits": [
						{
							"uuid": "d3f3f024-a90e-43a5-bd5a-7056f5bea699",
							"destination_node_uuid": "55fbef81-4151-4589-9f0a-8e5c44f6b5a3"
						}
					]
				}
			]
		}
	}`
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
	session := ""

	// add a trigger for our campaign flow with 'trigger'
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), 'trigger', false, $1, 'K', 'O', 1, 1, 1, 0) RETURNING id`,
		models.CampaignFlowID,
	)

	// also add a catch all
	db.MustExec(
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES(TRUE, now(), now(), NULL, false, $1, 'C', NULL, 1, 1, 1, 0) RETURNING id`,
		models.CampaignFlowID,
	)

	tcs := []struct {
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{"/mr/flow/migrate", "GET", "", 405, "illegal"},
		{"/mr/flow/migrate", "POST", minLegacyDef, 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "GET", "", 405, "illegal"},
		{"/mr/flow/validate", "POST", validateWithValidLegacyFlow, 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "POST", validateWithInvalidLegacyFlow, 422, `"error": "missing dependencies: group[uuid=146`},
		{"/mr/flow/validate", "POST", validateWithValidFlow, 200, `"type": "send_msg"`},
		{"/mr/flow/validate", "POST", validateWithInvalidFlow, 422, `isn't a known node`},
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

		// if this was a success, save our session
		if resp.StatusCode == 200 {
			// save the session for use in a resume
			parsed := make(map[string]interface{})
			json.Unmarshal(content, &parsed)
			sessionJSON, _ := json.Marshal(parsed["session"])
			session = string(sessionJSON)
			fmt.Println(session)
		}

		assert.True(t, strings.Contains(string(content), tc.Response), "%d: did not find string: %s in body: %s", i, tc.Response, string(content))
	}
}
