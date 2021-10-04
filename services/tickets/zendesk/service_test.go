package zendesk_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/services/tickets/zendesk"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer dates.SetNowSource(dates.DefaultNowSource)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 0, time.UTC)))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/any_channel/push.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{
				"results": [
					{
						"external_resource_id": "123",
						"status": {"code": "success"}
					}
				]
			}`),
			httpx.NewMockResponse(201, nil, `{
				"results": [
					{
						"external_resource_id": "124",
						"status": {"code": "success"}
					}
				]
			}`),
		},
	}))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "zendesk"))

	_, err = zendesk.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing subdomain or secret or oauth_token or push_id or push_token in zendesk config")

	svc, err := zendesk.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"subdomain":   "nyaruka",
			"secret":      "sesame",
			"oauth_token": "987654321",
			"push_id":     "1234-abcd",
			"push_token":  "123456789",
		},
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	defaultTopic := oa.SessionAssets().Topics().FindByName("General")

	// try with connection failure
	_, err = svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)
	assert.EqualError(t, err, "error pushing message to zendesk: unable to connect to server")

	logger = &flows.HTTPLogger{}

	ticket, err := svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, flows.TicketUUID("59d74b86-3e2f-4a93-aece-b05d2fdcde0c"), ticket.UUID())
	assert.Equal(t, "General", ticket.Topic().Name())
	assert.Equal(t, "Where are my cookies?", ticket.Body())
	assert.Equal(t, "", ticket.ExternalID())
	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "open_ticket", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID(), testdata.Org1.ID, testdata.Cathy.ID, testdata.Zendesk.ID, "", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
		"contact-uuid":    string(testdata.Cathy.UUID),
		"contact-display": "Cathy",
	})

	logger = &flows.HTTPLogger{}
	err = svc.Forward(
		dbTicket,
		flows.MsgUUID("ca5607f0-cba8-4c94-9cd5-c4fbc24aa767"),
		"It's urgent",
		[]utils.Attachment{utils.Attachment("image/jpg:http://myfiles.com/media/0123/attachment1.jpg")},
		logger.Log,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "forward_message", logger.Logs[0].Request)
}

func TestCloseAndReopen(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/tickets/update_many.json?ids=12,14": {
			httpx.NewMockResponse(201, nil, `{
				"job_status": {
					"id": "1234-abcd",
					"url": "http://zendesk.com",
					"status": "queued"
				}
			}`),
		},
		"https://nyaruka.zendesk.com/api/v2/tickets/update_many.json?ids=14": {
			httpx.NewMockResponse(201, nil, `{
				"job_status": {
					"id": "1234-abcd",
					"url": "http://zendesk.com",
					"status": "queued"
				}
			}`),
		},
	}))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "zendesk"))
	svc, err := zendesk.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"subdomain":   "nyaruka",
			"secret":      "sesame",
			"oauth_token": "987654321",
			"push_id":     "1234-abcd",
			"push_token":  "123456789",
		},
	)
	require.NoError(t, err)

	logger := &flows.HTTPLogger{}
	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", testdata.Org1.ID, testdata.Cathy.ID, testdata.Zendesk.ID, "12", testdata.DefaultTopic.ID, "Where my cookies?", models.NilUserID, nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Bob.ID, testdata.Zendesk.ID, "14", testdata.DefaultTopic.ID, "Where my shoes?", models.NilUserID, nil)

	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)

	assert.NoError(t, err)
	test.AssertSnapshot(t, "close_tickets", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)

	assert.NoError(t, err)
	test.AssertSnapshot(t, "reopen_tickets", logger.Logs[1].Request)
}
