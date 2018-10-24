package web

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"

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
		"session": {
			"environment": {
				"date_format": "YYYY-MM-DD",
				"time_format": "tt:mm",
				"timezone": "UTC",
				"redaction_policy": "none"
			},
			"trigger": {
				"type": "manual",
				"flow": {
					"uuid": "51e3c67d-8483-449c-abf7-25e50686f0db",
					"name": "Registration"
				},
				"contact": {
					"uuid": "ba96bf7f-bc2a-4873-a7c7-254d1927c4e3",
					"id": 1234567,
					"name": "Ben Haggerty",
					"language": "eng",
					"created_on": "2000-01-01T00:00:00Z",
					"urns": [
						"tel:+12065551212"
					]
				},
				"triggered_on": "2000-01-01T00:00:00Z"
			},
			"contact": {
				"uuid": "ba96bf7f-bc2a-4873-a7c7-254d1927c4e3",
				"id": 1234567,
				"name": "Ben Haggerty",
				"language": "eng",
				"created_on": "2000-01-01T00:00:00Z",
				"urns": [
					"tel:+12065551212"
				],
				"groups": [
					{
						"uuid": "caae117e-c26c-4625-96d5-ec4a0e7b8cdb",
						"name": "Unregistered (Dynamic)"
					}
				]
			},
			"runs": [
				{
					"uuid": "e276a417-a460-4b58-8e1d-a9bc7c9dd508",
					"flow": {
						"uuid": "51e3c67d-8483-449c-abf7-25e50686f0db",
						"name": "Favorites"
					},
					"path": [
						{
							"uuid": "4799ff0d-c2f8-483b-bfd9-a2286f8bb539",
							"node_uuid": "9c8f9c1b-4d67-4deb-94d7-411434c12c82",
							"exit_uuid": "ccd78ace-36cc-4528-8401-9d1de3bf1a27",
							"arrived_on": "2018-10-24T12:07:27.186692-05:00"
						},
						{
							"uuid": "2106dc49-9269-4559-85d5-884e1ea334e5",
							"node_uuid": "5272947a-b80b-47ff-ad76-182ec9185d31",
							"arrived_on": "2018-10-24T12:07:27.186754-05:00"
						}
					],
					"events": [
						{
							"type": "msg_created",
							"created_on": "2018-10-24T12:07:27.186751-05:00",
							"step_uuid": "4799ff0d-c2f8-483b-bfd9-a2286f8bb539",
							"msg": {
								"uuid": "17b9b827-0eff-4abe-91dd-77dcda64de2a",
								"urn": "tel:+12065551212",
								"channel": {
									"uuid": "ac4c718a-db3f-4d8a-ae43-321f1a5bd44a",
									"name": "Android"
								},
								"text": "What is your favorite color?"
							}
						},
						{
							"type": "msg_wait",
							"created_on": "2018-10-24T12:07:27.186755-05:00",
							"step_uuid": "2106dc49-9269-4559-85d5-884e1ea334e5"
						}
					],
					"status": "waiting",
					"created_on": "2018-10-24T12:07:27.186687-05:00",
					"modified_on": "2018-10-24T12:07:27.186756-05:00",
					"expires_on": "2018-10-25T00:07:27.186689-05:00",
					"exited_on": null
				}
			],
			"status": "waiting",
			"wait": {
				"type": "msg"
			}
		},
		"events": [
			{
				"type": "contact_groups_changed",
				"created_on": "2018-10-24T12:07:27.186686-05:00",
				"groups_added": [
					{
						"uuid": "caae117e-c26c-4625-96d5-ec4a0e7b8cdb",
						"name": "Unregistered (Dynamic)"
					}
				]
			},
			{
				"type": "msg_created",
				"created_on": "2018-10-24T12:07:27.186751-05:00",
				"step_uuid": "4799ff0d-c2f8-483b-bfd9-a2286f8bb539",
				"msg": {
					"uuid": "17b9b827-0eff-4abe-91dd-77dcda64de2a",
					"urn": "tel:+12065551212",
					"channel": {
						"uuid": "ac4c718a-db3f-4d8a-ae43-321f1a5bd44a",
						"name": "Android"
					},
					"text": "What is your favorite color?"
				}
			},
			{
				"type": "msg_wait",
				"created_on": "2018-10-24T12:07:27.186755-05:00",
				"step_uuid": "2106dc49-9269-4559-85d5-884e1ea334e5"
			}
		]
	}`
)

func TestServer(t *testing.T) {
	testsuite.Reset()
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()
	wg := &sync.WaitGroup{}

	server := NewServer(ctx, db, rp, config.Mailroom, wg)
	server.Start()
	defer server.Stop()

	// TODO: test custom flow definitions

	tcs := []struct {
		URL      string
		Method   string
		Body     string
		Status   int
		Response string
	}{
		{"/", "POST", "", 400, "illegal"},
		{"/", "GET", "", 200, "mailroom"},
		{"/sim/start", "GET", "", 400, "illegal"},
		{"/sim/start", "POST", startBody, 200, "What is your favorite color?"},
		{"/sim/resume", "GET", "", 400, "illegal"},
		{"/sim/resume", "POST", resumeBody, 200, "Good choice, I like Blue too! What is your favorite beer?"},
	}

	for i, tc := range tcs {
		var body io.Reader
		if tc.Body != "" {
			body = bytes.NewReader([]byte(tc.Body))
		}

		req, err := http.NewRequest(tc.Method, "http://localhost:8090"+tc.URL, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error marking request", i)

		assert.Equal(t, tc.Status, resp.StatusCode, "%d: unexpected status", i)

		content, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		assert.True(t, strings.Contains(string(content), tc.Response), "%d: did not find string: %s in body: %s", i, tc.Response, string(content))
	}
}
