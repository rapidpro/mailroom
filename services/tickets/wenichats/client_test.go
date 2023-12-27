package wenichats_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/services/tickets/wenichats"
	"github.com/stretchr/testify/assert"
)

const (
	authToken = "token"
	baseURL   = "https://chats-engine.dev.cloud.weni.ai/v1/external"
)

func TestCreateRoom(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/rooms/", baseURL): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, []byte(`{"detail":"Something went wrong"}`)),
			httpx.NewMockResponse(201, nil, []byte(`{
				"uuid": "8ecb1e4a-b457-4645-a161-e2b02ddffa88",
				"user": {
					"first_name": "John",
					"last_name": "Doe",
					"email": "john.doe@chats.weni.ai"
				},
				"contact": {
					"external_id": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
					"name": "Foo Bar",
					"email": "FooBar@weni.ai",
					"status": "string",
					"phone": "+250788123123",
					"custom_fields": {},
					"created_on": "2019-08-24T14:15:22Z"
				},
				"queue": {
					"uuid": "449f48d9-4905-4d6f-8abf-f1ff6afb803e",
					"created_on": "2019-08-24T14:15:22Z",
					"modified_on": "2019-08-24T14:15:22Z",
					"name": "CHATS",
					"sector": "f3d496ff-c154-4a96-a678-6a8879583ddb"
				},
				"created_on": "2019-08-24T14:15:22Z",
				"modified_on": "2019-08-24T14:15:22Z",
				"is_active": true,
				"custom_fields": {
					"country": "brazil",
					"mood": "angry"
				},
				"callback_url": "http://example.com"
			}`)),
		},
	}))

	client := wenichats.NewClient(http.DefaultClient, nil, baseURL, authToken)
	data := &wenichats.RoomRequest{
		QueueUUID: "449f48d9-4905-4d6f-8abf-f1ff6afb803e",
		Contact:   &wenichats.Contact{},
	}
	data.Contact.ExternalID = "095be615-a8ad-4c33-8e9c-c7612fbf6c9f"
	data.UserEmail = "john.doe@chats.weni.ai"
	data.Contact.Name = "John"

	_, _, err := client.CreateRoom(data)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateRoom(data)
	assert.EqualError(t, err, "Something went wrong")

	room, trace, err := client.CreateRoom(data)
	assert.NoError(t, err)
	assert.Equal(t, "8ecb1e4a-b457-4645-a161-e2b02ddffa88", room.UUID)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 898\r\n\r\n", string(trace.ResponseTrace))
}

func TestUpdateRoom(t *testing.T) {
	roomUUID := "8ecb1e4a-b457-4645-a161-e2b02ddffa88"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/rooms/%s/", baseURL, roomUUID): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, []byte(`{"detail":"Something went wrong"}`)),
			httpx.NewMockResponse(200, nil, []byte(`{
				"uuid": "8ecb1e4a-b457-4645-a161-e2b02ddffa88",
				"user": {
					"first_name": "John",
					"last_name": "Doe",
					"email": "john.doe@chats.weni.ai"
				},
				"contact": {
					"external_id": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
					"name": "Foo Bar",
					"email": "FooBar@weni.ai",
					"status": "string",
					"phone": "+250788123123",
					"custom_fields": {},
					"created_on": "2019-08-24T14:15:22Z"
				},
				"queue": {
					"uuid": "449f48d9-4905-4d6f-8abf-f1ff6afb803e",
					"created_on": "2019-08-24T14:15:22Z",
					"modified_on": "2019-08-24T14:15:22Z",
					"name": "CHATS",
					"sector": "f3d496ff-c154-4a96-a678-6a8879583ddb"
				},
				"created_on": "2019-08-24T14:15:22Z",
				"modified_on": "2019-08-24T14:15:22Z",
				"is_active": true,
				"custom_fields": {
					"country": "brazil",
					"mood": "angry"
				},
				"callback_url": "http://example.com"
			}`)),
		},
	}))

	client := wenichats.NewClient(http.DefaultClient, nil, baseURL, authToken)
	data := &wenichats.RoomRequest{
		CallbackURL: "http://example.com",
	}

	_, _, err := client.UpdateRoom(roomUUID, data)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.UpdateRoom(roomUUID, data)
	assert.EqualError(t, err, "Something went wrong")

	room, trace, err := client.UpdateRoom(roomUUID, data)
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", room.CallbackURL)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 898\r\n\r\n", string(trace.ResponseTrace))
}

func TestCloseRoom(t *testing.T) {
	roomUUID := "8ecb1e4a-b457-4645-a161-e2b02ddffa88"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/rooms/%s/close/", baseURL, roomUUID): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, []byte(`{"detail":"Something went wrong"}`)),
			httpx.NewMockResponse(200, nil, []byte(`{
				"uuid": "8ecb1e4a-b457-4645-a161-e2b02ddffa88",
				"user": {
					"first_name": "John",
					"last_name": "Doe",
					"email": "john.doe@chats.weni.ai"
				},
				"contact": {
					"external_id": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
					"name": "Foo Bar",
					"email": "FooBar@weni.ai",
					"status": "string",
					"phone": "+250788123123",
					"custom_fields": {},
					"created_on": "2019-08-24T14:15:22Z"
				},
				"queue": {
					"uuid": "449f48d9-4905-4d6f-8abf-f1ff6afb803e",
					"created_on": "2019-08-24T14:15:22Z",
					"modified_on": "2019-08-24T14:15:22Z",
					"name": "CHATS",
					"sector": "f3d496ff-c154-4a96-a678-6a8879583ddb"
				},
				"created_on": "2019-08-24T14:15:22Z",
				"modified_on": "2019-08-24T14:15:22Z",
				"is_active": true,
				"custom_fields": {
					"country": "brazil",
					"mood": "angry"
				},
				"callback_url": "http://example.com"
			}`)),
		},
	}))

	client := wenichats.NewClient(http.DefaultClient, nil, baseURL, authToken)

	_, _, err := client.CloseRoom(roomUUID)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CloseRoom(roomUUID)
	assert.EqualError(t, err, "Something went wrong")

	room, trace, err := client.CloseRoom(roomUUID)
	assert.NoError(t, err)
	assert.Equal(t, "http://example.com", room.CallbackURL)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 898\r\n\r\n", string(trace.ResponseTrace))
}

func TestSendMessage(t *testing.T) {
	roomUUID := "8ecb1e4a-b457-4645-a161-e2b02ddffa88"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/msgs/", baseURL): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, []byte(`{"detail": "Something went wrong"}`)),
			httpx.NewMockResponse(201, nil, []byte(`{
				"uuid": "b9312612-c26d-45ec-b9bb-7f116771fdd6",
				"user": null,
				"room": "8ecb1e4a-b457-4645-a161-e2b02ddffa88",
				"contact": {
					"uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
					"name": "Foo Bar",
					"email": "FooBar@weni.ai",
					"status": "string",
					"phone": "+250788123123",
					"custom_fields": {},
					"created_on": "2019-08-24T14:15:22Z"
				},
				"text": "hello",
				"seen": false,
				"media": [
					{
						"content_type": "audio/wav",
						"url": "http://domain.com/recording.wav"
					}
				],
				"created_on": "2022-08-25T02:06:55.885000-03:00"
			}`)),
		},
	}))

	client := wenichats.NewClient(http.DefaultClient, nil, baseURL, authToken)

	msg := &wenichats.MessageRequest{
		Room:      roomUUID,
		Text:      "hello",
		Direction: "incoming",
		Attachments: []wenichats.Attachment{
			{
				ContentType: "audio/wav",
				URL:         "http://domain.com/recording.wav",
			},
		},
	}

	_, _, err := client.CreateMessage(msg)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateMessage(msg)
	assert.EqualError(t, err, "Something went wrong")

	response, trace, err := client.CreateMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, "hello", response.Text)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 596\r\n\r\n", string(trace.ResponseTrace))
}

func TestGetQueues(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/queues/", baseURL): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, []byte(`{"detail": "Something went wrong"}`)),
			httpx.NewMockResponse(200, nil, []byte(`{
				"count": 1,
				"next": "http://example.com",
				"previous": "http://example.com",
				"results": [
					{
						"uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
						"name": "Queue 1"
					}
				]
			}`)),
		},
	}))

	client := wenichats.NewClient(http.DefaultClient, nil, baseURL, authToken)

	_, _, err := client.GetQueues(nil)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.GetQueues(nil)
	assert.EqualError(t, err, "Something went wrong")

	response, trace, err := client.GetQueues(nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(response.Results))
	assert.Equal(t, 200, trace.Response.StatusCode)
	assert.Equal(t, "095be615-a8ad-4c33-8e9c-c7612fbf6c9f", response.Results[0].UUID)
	assert.Equal(t, "Queue 1", response.Results[0].Name)
}
