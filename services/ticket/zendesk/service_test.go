package zendesk_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/test"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/uuids"
	"github.com/nyaruka/mailroom/services/ticket/zendesk"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService(t *testing.T) {
	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer dates.SetNowSource(dates.DefaultNowSource)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 123456789, time.UTC)))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://nyaruka.zendesk.com/api/v2/users/create_or_update.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{
				"user": {
					"id": 12345,
					"url": "https://nyaruka.zendesk.com/api/v2/users/12345.json",
					"name": "Jim",
					"role": "end-user",
					"external_id": "a78c5d9d-283a-4be9-ad6d-690e4307c961",
					"created_at": "2009-07-20T22:55:29Z"
				}
			}`),
			httpx.NewMockResponse(201, nil, `{
				"user": {
					"id": 12345,
					"url": "https://nyaruka.zendesk.com/api/v2/users/12345.json",
					"name": "Jim",
					"role": "end-user",
					"external_id": "a78c5d9d-283a-4be9-ad6d-690e4307c961",
					"created_at": "2009-07-20T22:55:29Z"
				}
			}`),
		},
		"https://nyaruka.zendesk.com/api/v2/tickets.json": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{
				"ticket":{
					"id": 12345,
					"url": "https://nyaruka.zendesk.com/api/v2/tickets/12345.json",
					"external_id": "a78c5d9d-283a-4be9-ad6d-690e4307c961",
					"created_at": "2009-07-20T22:55:29Z",
					"subject": "Need help"
				}
			}`),
		},
	}))

	ticketer := flows.NewTicketer(types.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "zendesk"))

	_, err = zendesk.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing subdomain or oauth_token in zendesk config")

	svc, err := zendesk.NewService(
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"subdomain":   "nyaruka",
			"oauth_token": "123456789",
		},
	)
	require.NoError(t, err)

	httpLogger := &flows.HTTPLogger{}

	// call to create user can fail
	_, err = svc.Open(session, "Need help", "Where are my cookies?", httpLogger.Log)
	assert.EqualError(t, err, "error creating zendesk user: unable to connect to server")

	httpLogger = &flows.HTTPLogger{}

	// as can call to create ticket
	_, err = svc.Open(session, "Need help", "Where are my cookies?", httpLogger.Log)
	assert.EqualError(t, err, "error creating zendesk ticket: unable to connect to server")

	httpLogger = &flows.HTTPLogger{}

	ticket, err := svc.Open(session, "Need help", "Where are my cookies?", httpLogger.Log)

	assert.NoError(t, err)
	assert.Equal(t, &flows.Ticket{
		UUID:       flows.TicketUUID("9688d21d-95aa-4bed-afc7-f31b35731a3d"),
		Ticketer:   ticketer.Reference(),
		Subject:    "Need help",
		Body:       "Where are my cookies?",
		ExternalID: "12345",
	}, ticket)

	assert.Equal(t, 2, len(httpLogger.Logs))
	assert.Equal(t, "https://nyaruka.zendesk.com/api/v2/users/create_or_update.json", httpLogger.Logs[0].URL)
	assert.Equal(t, "https://nyaruka.zendesk.com/api/v2/tickets.json", httpLogger.Logs[1].URL)
}
