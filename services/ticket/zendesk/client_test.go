package zendesk_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/services/ticket/zendesk"

	"github.com/stretchr/testify/assert"
)

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

	client := zendesk.NewClient(http.DefaultClient, nil, "nyaruka", "123456789")

	_, _, err := client.Push("1234-abcd", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.Push("1234-abcd", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.Push("1234-abcd", []*zendesk.ExternalResource{})
	assert.EqualError(t, err, "invalid character 'x' looking for beginning of value")

	results, trace, err := client.Push("1234-abcd", []*zendesk.ExternalResource{
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
					Data: json.RawMessage(`{"whatever": "I want"}`),
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
	assert.Equal(t, "POST /api/v2/any_channel/push.json HTTP/1.1\r\nHost: nyaruka.zendesk.com\r\nUser-Agent: Go-http-client/1.1\r\nContent-Length: 564\r\nAuthorization: Bearer 123456789\r\nContent-Type: application/json\r\nAccept-Encoding: gzip\r\n\r\n{\"instance_push_id\":\"1234-abcd\",\"external_resources\":[{\"external_id\":\"234\",\"message\":\"A useful comment\",\"html_message\":\"A <b>very</b> useful comment\",\"parent_id\":\"123\",\"created_at\":\"2015-01-13T08:59:26Z\",\"author\":{\"external_id\":\"456\",\"name\":\"Fred\",\"locale\":\"de\"},\"display_info\":[{\"type\":\"9ef45ff7-4aaa-4a58-8e77-a7c74dfa51c4\",\"data\":{\"whatever\":\"I want\"}}],\"allow_channelback\":true},{\"external_id\":\"636\",\"message\":\"Hi there\",\"thread_id\":\"347\",\"created_at\":\"2020-01-13T08:59:26Z\",\"author\":{\"external_id\":\"123\",\"name\":\"Jim\",\"locale\":\"en\"},\"allow_channelback\":true}]}", string(trace.RequestTrace))
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 234\r\n\r\n", string(trace.ResponseTrace))
}
