package rocketchat_test

import (
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/services/tickets/rocketchat"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

const (
	baseURL = "https://my.rocket.chat/api/apps/public/684202ed-1461-4983-9ea7-fde74b15026c"
	secret  = "123456789"
)

func TestCreateRoom(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		baseURL + "/room": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{ "error": "There's no agents online" }`),
			httpx.NewMockResponse(201, nil, `{ "id": "uiF7ybjsv7PSJGSw6" }`),
		},
	}))

	client := rocketchat.NewClient(http.DefaultClient, nil, baseURL, secret)
	room := &rocketchat.Room{
		Visitor: rocketchat.Visitor{
			Token:       "1234",
			ContactUUID: "88ff1e41-c1f8-4637-af8e-d56acbde9171",
			Name:        "Bob",
			Email:       "bob@acme.com",
			Phone:       "+16055741111",
		},
		TicketID:     "88ff1e41-c1f8-4637-af8e-d56acbde9171",
	}

	_, _, err := client.CreateRoom(room)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateRoom(room)
	assert.EqualError(t, err, "There's no agents online")

	id, trace, err := client.CreateRoom(room)
	assert.NoError(t, err)
	assert.Equal(t, id, "uiF7ybjsv7PSJGSw6")
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 29\r\n\r\n", string(trace.ResponseTrace))
}

func TestCloseRoom(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		baseURL + "/room.close": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{ "error": "Could not find a room for visitor token: 1234" }`),
			httpx.NewMockResponse(204, nil, ``),
		},
	}))

	client := rocketchat.NewClient(http.DefaultClient, nil, baseURL, secret)
	visitor := &rocketchat.Visitor{Token: "1234"}

	_, err := client.CloseRoom(visitor)
	assert.EqualError(t, err, "unable to connect to server")

	_, err = client.CloseRoom(visitor)
	assert.EqualError(t, err, "Could not find a room for visitor token: 1234")

	trace, err := client.CloseRoom(visitor)
	assert.NoError(t, err)
	assert.Equal(t, "HTTP/1.0 204 No Content\r\n\r\n", string(trace.ResponseTrace))
}

func TestSendMessage(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		baseURL + "/visitor-message": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{ "error": "Could not find a room for visitor token: 1234" }`),
			httpx.NewMockResponse(201, nil, `{ "id": "tyLrD97j8TFZmT3Y6" }`),
		},
	}))

	client := rocketchat.NewClient(http.DefaultClient, nil, baseURL, secret)
	msg := &rocketchat.VisitorMsg{
		Visitor: rocketchat.Visitor{Token: "1234"},
		Text:    "Can you help me?",
		Attachments: []rocketchat.Attachment{
			{Type: "image/jpg", URL: "https://link.to/image.jpg"},
			{Type: "video/mp4", URL: "https://link.to/video.mp4"},
			{Type: "audio/ogg", URL: "https://link.to/audio.ogg"},
		},
	}

	_, _, err := client.SendMessage(msg)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.SendMessage(msg)
	assert.EqualError(t, err, "Could not find a room for visitor token: 1234")

	id, trace, err := client.SendMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, id, "tyLrD97j8TFZmT3Y6")
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 29\r\n\r\n", string(trace.ResponseTrace))
}
