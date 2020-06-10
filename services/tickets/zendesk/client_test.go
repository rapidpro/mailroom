package zendesk_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/services/tickets/zendesk"

	"github.com/stretchr/testify/assert"
)

func TestCreateTarget(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/targets.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"description": "Something went wrong", "error": "Unknown"}`), // non-200 response
			httpx.NewMockResponse(200, nil, `xx`), // non-JSON response
			httpx.NewMockResponse(201, nil, `{
				"target": {
					"id": 1234567,
					"title": "Temba",
					"target_url": "http://temba.io/updates",
					"method": "POST",
					"content_type": "application/json"
				}
			}`),
		},
	}))

	client := zendesk.NewRESTClient(http.DefaultClient, nil, "nyaruka", "123456789")
	target := &zendesk.Target{
		Title:       "Temba",
		TargetURL:   "http://temba.io/updates",
		Method:      "POST",
		ContentType: "application/json",
	}

	_, _, err := client.CreateTarget(target)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateTarget(target)
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.CreateTarget(target)
	assert.EqualError(t, err, "invalid character 'x' looking for beginning of value")

	target, trace, err := client.CreateTarget(target)
	assert.NoError(t, err)
	assert.Equal(t, int64(1234567), target.ID)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 180\r\n\r\n", string(trace.ResponseTrace))
}

func TestCreateTrigger(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/triggers.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"description": "Something went wrong", "error": "Unknown"}`), // non-200 response
			httpx.NewMockResponse(200, nil, `xx`), // non-JSON response
			httpx.NewMockResponse(201, nil, `{
				"trigger": {
					"id": 1234567,
					"title": "Notify Temba",
					"conditions": {
						"all": [
							{
								"field": "status", 
								"operator": "changed"
							}
						]
					},
					"actions": [
						{
							"field": "notification_target",
							"value": ["123", "{}"]
						}
					]
				}
			}`),
		},
	}))

	client := zendesk.NewRESTClient(http.DefaultClient, nil, "nyaruka", "123456789")
	trigger := &zendesk.Trigger{
		Title: "Temba",
		Conditions: zendesk.Conditions{
			All: []zendesk.Condition{
				{"status", "changed", ""},
			},
		},
		Actions: []zendesk.Action{
			{"notification_target", []string{"123", "{}"}},
		},
	}

	_, _, err := client.CreateTrigger(trigger)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateTrigger(trigger)
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.CreateTrigger(trigger)
	assert.EqualError(t, err, "invalid character 'x' looking for beginning of value")

	trigger, trace, err := client.CreateTrigger(trigger)
	assert.NoError(t, err)
	assert.Equal(t, int64(1234567), trigger.ID)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 317\r\n\r\n", string(trace.ResponseTrace))
}

func TestUpdateManyTickets(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/tickets/update_many.json?ids=123,234": {
			httpx.NewMockResponse(201, nil, `{
				"job_status": {
					"id": "1234-abcd",
					"url": "http://zendesk.com",
					"status": "queued"
				}
			}`),
		},
	}))

	client := zendesk.NewRESTClient(http.DefaultClient, nil, "nyaruka", "123456789")

	jobStatus, trace, err := client.UpdateManyTickets([]int64{123, 234}, "solved")

	assert.NoError(t, err)
	assert.Equal(t, "queued", jobStatus.Status)
	assert.Equal(t, "PUT /api/v2/tickets/update_many.json?ids=123,234 HTTP/1.1\r\nHost: nyaruka.zendesk.com\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 30\r\nAuthorization: Bearer 123456789\r\nContent-Type: application/json\r\nAccept-Encoding: gzip\r\n\r\n{\"ticket\":{\"status\":\"solved\"}}", string(trace.RequestTrace))
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 114\r\n\r\n", string(trace.ResponseTrace))
}

func TestPush(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/any_channel/push.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"description": "Something went wrong", "error": "Unknown"}`), // non-200 response
			httpx.NewMockResponse(200, nil, `xx`), // non-JSON response
			httpx.NewMockResponse(201, nil, `{
				"results": [
					{
						"external_resource_id": "123",
						"status": {"code": "success"}
					},
					{
						"external_resource_id": "234",
						"status": {"code":"processing_error", "description":"Boom"}
					}
				]
			}`),
		},
	}))

	client := zendesk.NewPushClient(http.DefaultClient, nil, "nyaruka", "123456789")

	_, _, err := client.Push("1234-abcd", "5678-edfg", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.Push("1234-abcd", "5678-edfg", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.Push("1234-abcd", "5678-edfg", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "invalid character 'x' looking for beginning of value")

	results, trace, err := client.Push("1234-abcd", "5678-edfg", []*zendesk.ExternalResource{
		{
			ExternalID:  "234",
			Message:     "A useful comment",
			HTMLMessage: "A <b>very</b> useful comment",
			ParentID:    "123",
			CreatedAt:   time.Date(2015, 1, 13, 8, 59, 26, 0, time.UTC),
			Author: zendesk.Author{
				ExternalID: "456",
				Name:       "Fred",
				Locale:     "de",
			},
			DisplayInfo: []zendesk.DisplayInfo{
				{
					Type: "9ef45ff7-4aaa-4a58-8e77-a7c74dfa51c4",
					Data: map[string]string{"whatever": "I want"},
				},
			},
			AllowChannelback: true,
		},
		{
			ExternalID: "636",
			Message:    "Hi there",
			ThreadID:   "347",
			CreatedAt:  time.Date(2020, 1, 13, 8, 59, 26, 0, time.UTC),
			Author: zendesk.Author{
				ExternalID: "123",
				Name:       "Jim",
				Locale:     "en",
			},
			AllowChannelback: true,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, "success", results[0].Status.Code)
	assert.Equal(t, "processing_error", results[1].Status.Code)
	assert.Equal(t, "Boom", results[1].Status.Description)
	assert.Equal(t, "POST /api/v2/any_channel/push.json HTTP/1.1\r\nHost: nyaruka.zendesk.com\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 589\r\nAuthorization: Bearer 123456789\r\nContent-Type: application/json\r\nAccept-Encoding: gzip\r\n\r\n{\"instance_push_id\":\"1234-abcd\",\"request_id\":\"5678-edfg\",\"external_resources\":[{\"external_id\":\"234\",\"message\":\"A useful comment\",\"html_message\":\"A <b>very</b> useful comment\",\"parent_id\":\"123\",\"created_at\":\"2015-01-13T08:59:26Z\",\"author\":{\"external_id\":\"456\",\"name\":\"Fred\",\"locale\":\"de\"},\"display_info\":[{\"type\":\"9ef45ff7-4aaa-4a58-8e77-a7c74dfa51c4\",\"data\":{\"whatever\":\"I want\"}}],\"allow_channelback\":true},{\"external_id\":\"636\",\"message\":\"Hi there\",\"thread_id\":\"347\",\"created_at\":\"2020-01-13T08:59:26Z\",\"author\":{\"external_id\":\"123\",\"name\":\"Jim\",\"locale\":\"en\"},\"allow_channelback\":true}]}", string(trace.RequestTrace))
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 234\r\n\r\n", string(trace.ResponseTrace))
}
