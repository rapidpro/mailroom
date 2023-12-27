package wenichats_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
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
	"github.com/nyaruka/mailroom/services/tickets/wenichats"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()
	testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 0, time.UTC)))

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/rooms/", baseURL): {
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
					"mood": "angry",
					"age": 23,
					"join_date": "2017-12-02",
					"gender": "male"
				},
				"callback_url": "http://example.com"
			}`)),
		},
		fmt.Sprintf("%s/rooms/8ecb1e4a-b457-4645-a161-e2b02ddffa88/", baseURL): {
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
		fmt.Sprintf("%s/msgs/", baseURL): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(200, nil, []byte(`{
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
				"text": "Where are my cookies?",
				"seen": false,
				"media": [
					{
						"content_type": "audio/wav",
						"url": "http://domain.com/recording.wav"
					}
				],
				"created_on": "2022-08-25T02:06:55.885000-03:00"
			}`)),
			httpx.NewMockResponse(200, nil, []byte(`{
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
				"text": "Where are my cookies?",
				"seen": false,
				"media": [
					{
						"content_type": "image/jpg",
						"url": "https://link.to/dummy_image.jpg"
					},
					{
						"content_type": "video/mp4",
						"url": "https://link.to/dummy_video.mp4"
					},
					{
						"content_type": "audio/ogg",
						"url": "https://link.to/dummy_audio.ogg"
					}
				],
				"created_on": "2022-08-25T02:06:55.885000-03:00"
			}`)),
		},
		"https://link.to/dummy_image.jpg": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "image/jpeg"}, []byte(`imagebytes`)),
		},
		"https://link.to/dummy_video.mp4": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "video/mp4"}, []byte(`videobytes`)),
		},
		"https://link.to/dummy_audio.ogg": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "audio/ogg"}, []byte(`audiobytes`)),
		},
	}))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "wenichats"))

	_, err = wenichats.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing project_auth or sector_uuid")

	mockDB, mock, _ := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	rows := sqlmock.NewRows([]string{"id", "uuid", "text", "high_priority", "created_on", "modified_on", "sent_on", "queued_on", "direction", "status", "visibility", "msg_type", "msg_count", "error_count", "next_attempt", "external_id", "attachments", "metadata", "broadcast_id", "channel_id", "contact_id", "contact_urn_id", "org_id", "topup_id"})

	after, err := time.Parse("2006-01-02T15:04:05", "2019-10-07T15:21:30")
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT").
		WithArgs(1234567, after).
		WillReturnRows(rows)

	wenichats.SetDB(sqlxDB)

	svc, err := wenichats.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"project_auth": authToken,
			"sector_uuid":  "1a4bae05-993c-4f3b-91b5-80f4e09951f2",
		},
	)
	assert.NoError(t, err)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	defaultTopic := oa.SessionAssets().Topics().FindByName("General")

	logger := &flows.HTTPLogger{}
	ticket, err := svc.Open(session.Environment(), session.Contact(), defaultTopic, `{"custom_fields":{"country": "brazil","mood": "angry"}}`, nil, logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, flows.TicketUUID("e7187099-7d38-4f60-955c-325957214c42"), ticket.UUID())
	assert.Equal(t, "General", ticket.Topic().Name())
	assert.Equal(t, `{"custom_fields":{"country": "brazil","mood": "angry"}}`, ticket.Body())
	assert.Equal(t, "8ecb1e4a-b457-4645-a161-e2b02ddffa88", ticket.ExternalID())
	assert.Equal(t, 2, len(logger.Logs))
	test.AssertSnapshot(t, "open_ticket", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID(), testdata.Org1.ID, testdata.Admin.ID, models.NilFlowID, testdata.Cathy.ID, testdata.Wenichats.ID, "8ecb1e4a-b457-4645-a161-e2b02ddffa88", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
		"contact-uuid":    string(testdata.Cathy.UUID),
		"contact-display": "Cathy",
	})
	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.EqualError(t, err, "error send message to wenichats: unable to connect to server")

	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "forward_message", logger.Logs[0].Request)

	dbTicket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Admin.ID, models.NilFlowID, testdata.Cathy.ID, testdata.Wenichats.ID, "8ecb1e4a-b457-4645-a161-e2b02ddffa88", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
		"contact-uuid":    string(testdata.Cathy.UUID),
		"contact-display": "Cathy",
	})

	logger = &flows.HTTPLogger{}
	attachments := []utils.Attachment{
		"image/jpg:https://link.to/dummy_image.jpg",
		"video/mp4:https://link.to/dummy_video.mp4",
		"audio/ogg:https://link.to/dummy_audio.ogg",
	}
	err = svc.Forward(dbTicket2, flows.MsgUUID("5ga340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", attachments, logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
}

func TestCloseAndReopen(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	roomUUID := "8ecb1e4a-b457-4645-a161-e2b02ddffa88"

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]*httpx.MockResponse{
		fmt.Sprintf("%s/rooms/%s/close/", baseURL, roomUUID): {
			httpx.MockConnectionError,
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

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "wenichats"))

	svc, err := wenichats.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"project_auth": authToken,
			"sector_uuid":  "1a4bae05-993c-4f3b-91b5-80f4e09951f2",
		},
	)
	assert.NoError(t, err)

	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", testdata.Org1.ID, testdata.Admin.ID, models.NilFlowID, testdata.Cathy.ID, testdata.Wenichats.ID, roomUUID, testdata.DefaultTopic.ID, "Where my cookies?", models.NilUserID, nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Admin.ID, models.NilFlowID, testdata.Bob.ID, testdata.Wenichats.ID, roomUUID, testdata.DefaultTopic.ID, "Where my shoes?", models.NilUserID, nil)

	logger := &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.EqualError(t, err, "error calling wenichats API: unable to connect to server")

	logger = &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.NoError(t, err)
	test.AssertSnapshot(t, "close_tickets", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)
	assert.EqualError(t, err, "wenichats ticket type doesn't support reopening")
}
