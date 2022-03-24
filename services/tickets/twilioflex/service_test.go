package twilioflex_test

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
	"github.com/nyaruka/mailroom/services/tickets/twilioflex"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAndForward(t *testing.T) {
	ctx, rt, _, _ := testsuite.Get()

	defer dates.SetNowSource(dates.DefaultNowSource)
	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2019, 10, 7, 15, 21, 30, 0, time.UTC)))

	session, _, err := test.CreateTestSession("", envs.RedactionPolicyNone)
	require.NoError(t, err)

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/1234567": {
			httpx.NewMockResponse(404, nil, `{
				"code": 20404,
				"message": "The requested resource /Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/1234567 was not found",
				"more_info": "https://www.twilio.com/docs/errors/20404",
				"status": 404
			}`),
		},
		"https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users": {
			httpx.NewMockResponse(201, nil, `{
				"is_notifiable": null,
				"date_updated": "2022-03-08T22:18:23Z",
				"is_online": null,
				"friendly_name": "dummy user",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f",
				"date_created": "2022-03-08T22:18:23Z",
				"role_sid": "RL6f3f490b35534130845f98202673ffb9",
				"sid": "USf4015a97250d482889459f8e8819e09f",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"joined_channels_count": 0,
				"identity": "10000",
				"links": {
						"user_channels": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Channels",
						"user_bindings": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Bindings"
				}
			}`),
		},
		"https://flex-api.twilio.com/v1/Channels": {
			httpx.NewMockResponse(201, nil, `{
				"task_sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"flex_flow_sid": "FOedbb8c9e54f04afaef409246f728a44d",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"user_sid": "USf4015a97250d482889459f8e8819e09f",
				"url": "https://flex-api.twilio.com/v1/Channels/CH6442c09c93ba4d13966fa42e9b78f620",
				"date_updated": "2022-03-08T22:38:30Z",
				"sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"date_created": "2022-03-08T22:38:30Z"
			}`),
		},
		"https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Webhooks": {
			httpx.NewMockResponse(201, nil, `{
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Webhooks/WHa8a9ae86063e494d9f3b754a8da85f8e",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"date_updated": "2022-03-09T19:54:49Z",
				"configuration": {
						"url": "https://mailroom.com/mr/tickets/types/twilioflex/event_callback/1234/4567",
						"retry_count": 1,
						"method": "POST",
						"filters": [
								"onMessageSent"
						]
				},
				"sid": "WHa8a9ae86063e494d9f3b754a8da85f8e",
				"date_created": "2022-03-09T19:54:49Z",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"type": "webhook"
			}`),
		},
		"https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Messages": {
			httpx.NewMockResponse(201, nil, `{
				"body": "Hi! I'll try to help you!",
				"index": 0,
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"from": "10000",
				"date_updated": "2022-03-09T20:27:47Z",
				"type": "text",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH6442c09c93ba4d13966fa42e9b78f620",
				"last_updated_by": null,
				"date_created": "2022-03-09T20:27:47Z",
				"media": null,
				"sid": "IM8842e723153b459b9e03a0bae87298d8",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Messages/IM8842e723153b459b9e03a0bae87298d8",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"body": "Where are you from?",
				"index": 0,
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"from": "10000",
				"date_updated": "2022-03-09T20:27:47Z",
				"type": "text",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH6442c09c93ba4d13966fa42e9b78f620",
				"last_updated_by": null,
				"date_created": "2022-03-09T20:27:47Z",
				"media": null,
				"sid": "IM8842e723153b459b9e03a0bae87298d8",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Messages/IM8842e723153b459b9e03a0bae87298d8",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"body": "I'm from Brazil",
				"index": 0,
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"from": "10000",
				"date_updated": "2022-03-09T20:27:47Z",
				"type": "text",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH6442c09c93ba4d13966fa42e9b78f620",
				"last_updated_by": null,
				"date_created": "2022-03-09T20:27:47Z",
				"media": null,
				"sid": "IM8842e723153b459b9e03a0bae87298d8",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Messages/IM8842e723153b459b9e03a0bae87298d8",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.MockConnectionError,
			httpx.NewMockResponse(201, nil, `{
				"body": "It's urgent",
				"index": 0,
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"from": "10000",
				"date_updated": "2022-03-09T20:27:47Z",
				"type": "text",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH6442c09c93ba4d13966fa42e9b78f620",
				"last_updated_by": null,
				"date_created": "2022-03-09T20:27:47Z",
				"media": null,
				"sid": "IM8842e723153b459b9e03a0bae87298d8",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH6442c09c93ba4d13966fa42e9b78f620/Messages/IM8842e723153b459b9e03a0bae87298d8",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
		},
		"https://link.to/dummy_image.jpg": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "image/jpeg"}, `imagebytes`),
		},
		"https://link.to/dummy_video.mp4": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "video/mp4"}, `videobytes`),
		},
		"https://link.to/dummy_audio.ogg": {
			httpx.NewMockResponse(200, map[string]string{"Content-Type": "audio/ogg"}, `audiobytes`),
		},
		"https://mcs.us1.twilio.com/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media": {
			httpx.NewMockResponse(201, nil, `{
				"sid": "ME59b872f1e52fbd6fe6ad956bbb4fa9bd",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"date_created": "2022-03-14T13:10:38.897143-07:00",
				"date_upload_updated": "2022-03-14T13:10:38.906058-07:00",
				"date_updated": "2022-03-14T13:10:38.897143-07:00",
				"links": {
					"content": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME59b872f1e52fbd6fe6ad956bbb4fa9bd/Content"
				},
				"size": 153611,
				"content_type": "image/jpeg",
				"filename": "dummy_image.jpg",
				"author": "10000",
				"category": "media",
				"message_sid": null,
				"channel_sid": null,
				"url": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME59b872f1e52fbd6fe6ad956bbb4fa9bd",
				"is_multipart_upstream": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"sid": "ME60b872f1e52fbd6fe6ad956bbb4fa9ce",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"date_created": "2022-03-14T13:10:38.897143-07:00",
				"date_upload_updated": "2022-03-14T13:10:38.906058-07:00",
				"date_updated": "2022-03-14T13:10:38.897143-07:00",
				"links": {
					"content": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME60b872f1e52fbd6fe6ad956bbb4fa9ce/Content"
				},
				"size": 153611,
				"content_type": "video/mp4",
				"filename": "dummy_video.mp4",
				"author": "10000",
				"category": "media",
				"message_sid": null,
				"channel_sid": null,
				"url": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME60b872f1e52fbd6fe6ad956bbb4fa9ce",
				"is_multipart_upstream": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"sid": "ME71b872f1e52fbd6fe6ad956bbb4fa9df",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"date_created": "2022-03-14T13:10:38.897143-07:00",
				"date_upload_updated": "2022-03-14T13:10:38.906058-07:00",
				"date_updated": "2022-03-14T13:10:38.897143-07:00",
				"links": {
					"content": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME71b872f1e52fbd6fe6ad956bbb4fa9df/Content"
				},
				"size": 153611,
				"content_type": "audio/ogg",
				"filename": "dummy_audio.ogg",
				"author": "10000",
				"category": "media",
				"message_sid": null,
				"channel_sid": null,
				"url": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME71b872f1e52fbd6fe6ad956bbb4fa9df",
				"is_multipart_upstream": false
			}`),
		},
		"https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH180fa48ef2ba40a08fa5c9fb5c8ddd99/Messages": {
			httpx.NewMockResponse(201, nil, `{
				"body": null,
				"index": 0,
				"channel_sid": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"from": "10000",
				"date_updated": "2022-03-14T20:11:08Z",
				"type": "media",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"last_updated_by": null,
				"date_created": "2022-03-14T20:11:08Z",
				"media": {
						"size": 153611,
						"filename": "dummy_image.jpg",
						"content_type": "image/jpeg",
						"sid": "ME59b872f1e52fbd6fe6ad956bbb4fa9bd"
				},
				"sid": "IMadceb005ef924c728b6abde17d02775c",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH180fa48ef2ba40a08fa5c9fb5c8ddd99/Messages/IMadceb005ef924c728b6abde17d02775c",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"body": null,
				"index": 1,
				"channel_sid": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"from": "10000",
				"date_updated": "2022-03-14T20:11:08Z",
				"type": "media",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"last_updated_by": null,
				"date_created": "2022-03-14T20:11:08Z",
				"media": {
						"size": 153611,
						"filename": "dummy_video.mp4",
						"content_type": "video/mp4",
						"sid": "ME60b872f1e52fbd6fe6ad956bbb4fa9ce"
				},
				"sid": "IMbcdeb005ef924c728b6abde17d02786d",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH180fa48ef2ba40a08fa5c9fb5c8ddd99/Messages/IMbcdeb005ef924c728b6abde17d02786d",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"body": null,
				"index": 2,
				"channel_sid": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"from": "10000",
				"date_updated": "2022-03-14T20:11:08Z",
				"type": "media",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"last_updated_by": null,
				"date_created": "2022-03-14T20:11:08Z",
				"media": {
						"size": 153611,
						"filename": "dummy_sound.ogg",
						"content_type": "sound/ogg",
						"sid": "ME71b872f1e52fbd6fe6ad956bbb4fa9df"
				},
				"sid": "IMcedfb005ef924c728b6abde17d02798e",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH180fa48ef2ba40a08fa5c9fb5c8ddd99/Messages/IMcedfb005ef924c728b6abde17d02798e",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
			httpx.NewMockResponse(201, nil, `{
				"body": "It's urgent",
				"index": 0,
				"channel_sid": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"from": "10000",
				"date_updated": "2022-03-09T20:27:47Z",
				"type": "text",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"to": "CH6442c09c93ba4d13966fa42e9b78f620",
				"last_updated_by": null,
				"date_created": "2022-03-09T20:27:47Z",
				"media": null,
				"sid": "IM8842e723153b459b9e03a0bae87298d8",
				"url": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Channels/CH180fa48ef2ba40a08fa5c9fb5c8ddd99/Messages/IM8842e723153b459b9e03a0bae87298d8",
				"attributes": "{}",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"was_edited": false
			}`),
		},
	}))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "twilioflex"))

	_, err = twilioflex.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{},
	)
	assert.EqualError(t, err, "missing auth_token or account_sid or chat_service_sid or workspace_sid in twilio flex config")

	mockDB, mock, err := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	dummyTime, _ := time.Parse(time.RFC1123, "2019-10-07T15:21:30")

	rows := sqlmock.NewRows([]string{"id", "uuid", "text", "high_priority", "created_on", "modified_on", "sent_on", "queued_on", "direction", "status", "visibility", "msg_type", "msg_count", "error_count", "next_attempt", "external_id", "attachments", "metadata", "broadcast_id", "channel_id", "connection_id", "contact_id", "contact_urn_id", "org_id", "response_to_id", "topup_id"}).
		AddRow(100, "1348d654-e3dc-4f2f-add0-a9163dc48895", "Hi! I'll try to help you!", true, dummyTime, dummyTime, dummyTime, dummyTime, "O", "W", "V", "F", 1, 0, nil, "398", nil, nil, nil, 3, nil, 2, 2, 3, 325, 3).
		AddRow(101, "b9568e35-3a59-4f91-882f-fa021f591b13", "Where are you from?", true, dummyTime, dummyTime, dummyTime, dummyTime, "O", "W", "V", "F", 1, 0, nil, "399", nil, nil, nil, 3, nil, 2, 2, 3, 325, 3).
		AddRow(102, "c864c4e0-9863-4fd3-9f76-bee481b4a138", "I'm from Brazil", false, dummyTime, dummyTime, dummyTime, dummyTime, "I", "P", "V", "F", 1, 0, nil, "400", nil, nil, nil, 3, nil, 2, 2, 3, nil, nil)

	after, err := time.Parse("2006-01-02T15:04:05", "2019-10-07T15:21:30")
	assert.NoError(t, err)

	mock.ExpectQuery("SELECT").
		WithArgs(1234567, after).
		WillReturnRows(rows)

	twilioflex.SetDB(sqlxDB)

	svc, err := twilioflex.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"auth_token":       authToken,
			"account_sid":      accountSid,
			"chat_service_sid": serviceSid,
			"workspace_sid":    workspaceSid,
			"flex_flow_sid":    flexFlowSid,
		},
	)
	assert.NoError(t, err)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)
	defaultTopic := oa.SessionAssets().Topics().FindByName("General")

	logger := &flows.HTTPLogger{}
	ticket, err := svc.Open(session, defaultTopic, "Where are my cookies?", nil, logger.Log)

	assert.NoError(t, err)
	assert.Equal(t, flows.TicketUUID("e7187099-7d38-4f60-955c-325957214c42"), ticket.UUID())
	assert.Equal(t, "General", ticket.Topic().Name())
	assert.Equal(t, "Where are my cookies?", ticket.Body())
	assert.Equal(t, "CH6442c09c93ba4d13966fa42e9b78f620", ticket.ExternalID())
	assert.Equal(t, 7, len(logger.Logs))
	test.AssertSnapshot(t, "open_ticket", logger.Logs[0].Request)

	dbTicket := models.NewTicket(ticket.UUID(), testdata.Org1.ID, testdata.Cathy.ID, testdata.Twilioflex.ID, "CH6442c09c93ba4d13966fa42e9b78f620", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
		"contact-uuid":    string(testdata.Cathy.UUID),
		"contact-display": "Cathy",
	})
	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.EqualError(t, err, "error calling Twilio: unable to connect to server")

	logger = &flows.HTTPLogger{}
	err = svc.Forward(dbTicket, flows.MsgUUID("4fa340ae-1fb0-4666-98db-2177fe9bf31c"), "It's urgent", nil, logger.Log)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logger.Logs))
	test.AssertSnapshot(t, "forward_message", logger.Logs[0].Request)

	dbTicket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Cathy.ID, testdata.Twilioflex.ID, "CH180fa48ef2ba40a08fa5c9fb5c8ddd99", testdata.DefaultTopic.ID, "Where are my cookies?", models.NilUserID, map[string]interface{}{
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
	assert.Equal(t, 4, len(logger.Logs))
}

func TestCloseAndReopen(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer uuids.SetGenerator(uuids.DefaultGenerator)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	uuids.SetGenerator(uuids.NewSeededGenerator(12345))
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://flex-api.twilio.com/v1/Channels/CH6442c09c93ba4d13966fa42e9b78f620": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(200, nil, `{
				"task_sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"flex_flow_sid": "FOedbb8c9e54f04afaef409246f728a44d",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"user_sid": "USf4015a97250d482889459f8e8819e09f",
				"url": "https://flex-api.twilio.com/v1/Channels/CH6442c09c93ba4d13966fa42e9b78f620",
				"date_updated": "2022-03-08T22:38:30Z",
				"sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"date_created": "2022-03-08T22:38:30Z"
			}`),
			httpx.NewMockResponse(200, nil, `{
				"task_sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"flex_flow_sid": "FOedbb8c9e54f04afaef409246f728a44d",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"user_sid": "USf4015a97250d482889459f8e8819e09f",
				"url": "https://flex-api.twilio.com/v1/Channels/CH6442c09c93ba4d13966fa42e9b78f620",
				"date_updated": "2022-03-08T22:38:30Z",
				"sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"date_created": "2022-03-08T22:38:30Z"
			}`),
		},
		"https://flex-api.twilio.com/v1/Channels/CH8692e17c93ba4d13966fa42e9b78f853": {
			httpx.NewMockResponse(200, nil, `{
				"task_sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"flex_flow_sid": "FOedbb8c9e54f04afaef409246f728a44d",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"user_sid": "USf4015a97250d482889459f8e8819e09f",
				"url": "https://flex-api.twilio.com/v1/Channels/CH8692e17c93ba4d13966fa42e9b78f853",
				"date_updated": "2022-03-08T22:38:30Z",
				"sid": "CH8692e17c93ba4d13966fa42e9b78f853",
				"date_created": "2022-03-08T22:38:30Z"
			}`),
		},
		fmt.Sprintf("https://taskrouter.twilio.com/v1/Workspaces/%s/Tasks/WT1d187abc335f7f16ff050a66f9b6a6b2", workspaceSid): {
			httpx.NewMockResponse(200, nil, `{
				"workspace_sid": "WS954611f5aebc7672d71de836c0179113",
				"assignment_status": "completed",
				"date_updated": "2022-03-09T21:57:00Z",
				"task_queue_entered_date": "2022-03-08T22:38:30Z",
				"age": 83910,
				"sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"priority": 0,
				"url": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Tasks/WT1d187abc335f7f16ff050a66f9b6a6b2",
				"reason": "resolved",
				"task_queue_sid": "WQa9e71cb17d52c8b75e4934b75e3297bc",
				"workflow_friendly_name": "Assign to Anyone",
				"timeout": 86400,
				"attributes": "{\"channelSid\":\"CH6442c09c93ba4d13966fa42e9b78f620\",\"name\":\"dummy user\",\"channelType\":\"web\"}",
				"date_created": "2022-03-08T22:38:30Z",
				"task_channel_sid": "TCf7fafe38a5210ee6b328b2bc42a1e950",
				"addons": "{}",
				"task_channel_unique_name": "chat",
				"workflow_sid": "WWfaeaff148cfdefce03443a4980149558",
				"task_queue_friendly_name": "Everyone",
				"links": {
						"reservations": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Tasks/WT1d187abc335f7f16ff050a66f9b6a6b2/Reservations",
						"task_queue": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/TaskQueues/WQa9e71cb17d52c8b75e4934b75e3297bc",
						"workspace": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113",
						"workflow": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Workflows/WWfaeaff148cfdefce03443a4980149558"
				}
			}`),
			httpx.NewMockResponse(200, nil, `{
				"workspace_sid": "WS954611f5aebc7672d71de836c0179113",
				"assignment_status": "completed",
				"date_updated": "2022-03-09T21:57:00Z",
				"task_queue_entered_date": "2022-03-08T22:38:30Z",
				"age": 83910,
				"sid": "WT1d187abc335f7f16ff050a66f9b6a6b2",
				"account_sid": "AC81d44315e19372138bdaffcc13cf3b94",
				"priority": 0,
				"url": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Tasks/WT1d187abc335f7f16ff050a66f9b6a6b2",
				"reason": "resolved",
				"task_queue_sid": "WQa9e71cb17d52c8b75e4934b75e3297bc",
				"workflow_friendly_name": "Assign to Anyone",
				"timeout": 86400,
				"attributes": "{\"channelSid\":\"CH6442c09c93ba4d13966fa42e9b78f620\",\"name\":\"dummy user\",\"channelType\":\"web\"}",
				"date_created": "2022-03-08T22:38:30Z",
				"task_channel_sid": "TCf7fafe38a5210ee6b328b2bc42a1e950",
				"addons": "{}",
				"task_channel_unique_name": "chat",
				"workflow_sid": "WWfaeaff148cfdefce03443a4980149558",
				"task_queue_friendly_name": "Everyone",
				"links": {
						"reservations": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Tasks/WT1d187abc335f7f16ff050a66f9b6a6b2/Reservations",
						"task_queue": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/TaskQueues/WQa9e71cb17d52c8b75e4934b75e3297bc",
						"workspace": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113",
						"workflow": "https://taskrouter.twilio.com/v1/Workspaces/WS954611f5aebc7672d71de836c0179113/Workflows/WWfaeaff148cfdefce03443a4980149558"
				}
			}`),
		},
	}))

	ticketer := flows.NewTicketer(static.NewTicketer(assets.TicketerUUID(uuids.New()), "Support", "twilioflex"))

	svc, err := twilioflex.NewService(
		rt.Config,
		http.DefaultClient,
		nil,
		ticketer,
		map[string]string{
			"auth_token":       authToken,
			"account_sid":      accountSid,
			"chat_service_sid": serviceSid,
			"workspace_sid":    workspaceSid,
			"flex_flow_sid":    flexFlowSid,
		},
	)
	assert.NoError(t, err)

	ticket1 := models.NewTicket("88bfa1dc-be33-45c2-b469-294ecb0eba90", testdata.Org1.ID, testdata.Cathy.ID, testdata.RocketChat.ID, "CH6442c09c93ba4d13966fa42e9b78f620", testdata.DefaultTopic.ID, "Where my cookies?", models.NilUserID, nil)
	ticket2 := models.NewTicket("645eee60-7e84-4a9e-ade3-4fce01ae28f1", testdata.Org1.ID, testdata.Bob.ID, testdata.RocketChat.ID, "CH8692e17c93ba4d13966fa42e9b78f853", testdata.DefaultTopic.ID, "Where my shoes?", models.NilUserID, nil)

	logger := &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.EqualError(t, err, "error calling Twilio API: unable to connect to server")

	logger = &flows.HTTPLogger{}
	err = svc.Close([]*models.Ticket{ticket1, ticket2}, logger.Log)
	assert.NoError(t, err)
	test.AssertSnapshot(t, "close_tickets", logger.Logs[0].Request)

	err = svc.Reopen([]*models.Ticket{ticket2}, logger.Log)
	assert.EqualError(t, err, "Twilio Flex ticket type doesn't support reopening")
}
