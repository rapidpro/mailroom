package rocketchat_test

import (
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/services/tickets/rocketchat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestOpenAndForward(t *testing.T) {
	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 0, time.UTC)))

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		baseURL + "/room": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{ "id": "uiF7ybjsv7PSJGSw6" }`),
		},
		baseURL + "/visitor-message": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{ "id": "tyLrD97j8TFZmT3Y6" }`),
		},
	}))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "rocketchat"))

	_, err = rocketchat.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing base_url or secret config")

	svc, err := rocketchat.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"base_url": baseURL,
			"secret":   secret,
		},
	)
	assert.NoError(t, err)

	logger := &flows.HTTPLogger{}
	_, err = svc.Open(session, "Need help", "Where are my cookies?", logger.Log)
	assert.EqualError(t, err, "error calling RocketChat: unable to connect to server")

	logger = &flows.HTTPLogger{}
	ticket, err := svc.Open(session, "Need help", "Where are my cookies?", logger.Log)
	assert.NoError(t, err)

	assert.Equal(t, &flows.Ticket{
		UUID:       flows.TicketUUID("59d74b86-3e2f-4a93-aece-b05d2fdcde0c"),
		Ticketer:   ticketer.Reference(),
		Subject:    "Need help",
		Body:       "Where are my cookies?",
		ExternalID: "uiF7ybjsv7PSJGSw6",
	}, ticket)

	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "open_ticket", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID, models.Org1, models.CathyID, models.RocketChatID, "", "Need help", "Where are my cookies?", map[string]interface{}{
		"contact-uuid":    string(models.CathyUUID),
		"contact-display": "Cathy",
	})
	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.EqualError(t, err, "error calling RocketChat: unable to connect to server")

	logger = &flows.HTTPLogger{}
	attachments := []utils.Attachment{
		"image/jpg:https://link.to/image.jpg",
		"video/mp4:https://link.to/video.mp4",
		"audio/ogg:https://link.to/audio.ogg",
	}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", attachments, logger.Log)
	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "forward_message", logger.Logs[0].Request)
}

func TestCloseAndReopen(t *testing.T) {
	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		baseURL + "/room.close": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(204, nil, ``),
			httpx.NewMockResponse(204, nil, ``),
		},
	}))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "rocketchat"))
	svc, err := rocketchat.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"base_url": baseURL,
			"secret":   secret,
		},
	)
	require.NoError(t, err)

	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", models.Org1, models.CathyID, models.RocketChatID, "X5gwXeaxbnGDaq8Q3", "New ticket", "Where my cookies?", nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", models.Org1, models.BobID, models.RocketChatID, "cq7AokJHKkGhAMoBK", "Second ticket", "Where my shoes?", nil)

	logger := &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.EqualError(t, err, "error calling RocketChat: unable to connect to server")

	logger = &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.NoError(t, err)
	test.AssertSnapshot(t, "close_tickets", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)
	assert.EqualError(t, err, "RocketChat ticket type doesn't support reopening")
}
