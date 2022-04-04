package twilioflex_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/mailroom/services/tickets/twilioflex"
	"github.com/stretchr/testify/assert"
)

const (
	authToken    = "token"
	accountSid   = "AC81d44315e19372138bdaffcc13cf3b94"
	serviceSid   = "IS38067ec392f1486bb6e4de4610f26fb3"
	workspaceSid = "WS954611f5aebc7672d71de836c0179113"
	flexFlowSid  = "FOedbb8c9e54f04afaef409246f728a44d"
)

func TestCreateUser(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Users", serviceSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
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
				"identity": "123",
				"links": {
						"user_channels": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Channels",
						"user_bindings": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Bindings"
				}
			}`),
		},
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)
	params := &twilioflex.CreateChatUserParams{
		Identity:     "123",
		FriendlyName: "dummy user",
	}

	_, _, err := client.CreateUser(params)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateUser(params)
	assert.EqualError(t, err, "Something went wrong")

	user, trace, err := client.CreateUser(params)
	assert.NoError(t, err)
	assert.Equal(t, "123", user.Identity)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 915\r\n\r\n", string(trace.ResponseTrace))
}

func TestFetchUser(t *testing.T) {
	userSid := "USf4015a97250d482889459f8e8819e09f"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Users/%s", serviceSid, userSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
			httpx.NewMockResponse(200, nil, `{
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
				"identity": "123",
				"links": {
						"user_channels": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Channels",
						"user_bindings": "https://chat.twilio.com/v2/Services/IS38067ec392f1486bb6e4de4610f26fb3/Users/USf4015a97250d482889459f8e8819e09f/Bindings"
				}
			}`),
		},
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)
	_, _, err := client.FetchUser(userSid)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.FetchUser(userSid)
	assert.EqualError(t, err, "Something went wrong")

	user, trace, err := client.FetchUser(userSid)
	assert.NoError(t, err)
	assert.Equal(t, "123", user.Identity)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 915\r\n\r\n", string(trace.ResponseTrace))
}

func TestCreateFlexChannel(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://flex-api.twilio.com/v1/Channels": {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
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
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	params := &twilioflex.CreateFlexChannelParams{
		FlexFlowSid:          flexFlowSid,
		Identity:             "123",
		ChatUserFriendlyName: "dummy user",
		ChatFriendlyName:     "dummy user",
	}

	_, _, err := client.CreateFlexChannel(params)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateFlexChannel(params)
	assert.EqualError(t, err, "Something went wrong")

	channel, trace, err := client.CreateFlexChannel(params)
	assert.NoError(t, err)
	assert.Equal(t, "FOedbb8c9e54f04afaef409246f728a44d", channel.FlexFlowSid)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 455\r\n\r\n", string(trace.ResponseTrace))
}

func TestFetchFlexChannel(t *testing.T) {
	channelSid := "CH6442c09c93ba4d13966fa42e9b78f620"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://flex-api.twilio.com/v1/Channels/%s", channelSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
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
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	_, _, err := client.FetchFlexChannel(channelSid)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.FetchFlexChannel(channelSid)
	assert.EqualError(t, err, "Something went wrong")

	channel, trace, err := client.FetchFlexChannel(channelSid)
	assert.NoError(t, err)
	assert.Equal(t, "FOedbb8c9e54f04afaef409246f728a44d", channel.FlexFlowSid)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 455\r\n\r\n", string(trace.ResponseTrace))
}

func TestCreateFlexChannelWebhook(t *testing.T) {
	channelSid := "CH6442c09c93ba4d13966fa42e9b78f620"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Channels/%s/Webhooks", serviceSid, channelSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
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
	}))

	callbackURL := fmt.Sprintf(
		"https://%s/mr/tickets/types/twilioflex/event_callback/%s/%s",
		"mailroom.domain.com",
		"ticketer-uuid-1234-4567-7890",
		"ticket-uuid-1234-4567-7890",
	)

	channelWebhook := &twilioflex.CreateChatChannelWebhookParams{
		ConfigurationUrl:        callbackURL,
		ConfigurationFilters:    []string{"onMessageSent", "onChannelUpdated"},
		ConfigurationMethod:     "POST",
		ConfigurationRetryCount: 1,
		Type:                    "webhook",
	}

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	_, _, err := client.CreateFlexChannelWebhook(channelWebhook, channelSid)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateFlexChannelWebhook(channelWebhook, channelSid)
	assert.EqualError(t, err, "Something went wrong")

	webhook, trace, err := client.CreateFlexChannelWebhook(channelWebhook, channelSid)
	assert.NoError(t, err)
	assert.Equal(t, "CH6442c09c93ba4d13966fa42e9b78f620", webhook.ChannelSid)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 728\r\n\r\n", string(trace.ResponseTrace))
}

func TestCreateMessage(t *testing.T) {
	channelSid := "CH6442c09c93ba4d13966fa42e9b78f620"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://chat.twilio.com/v2/Services/%s/Channels/%s/Messages", serviceSid, channelSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
			httpx.NewMockResponse(201, nil, `{
				"body": "hello",
				"index": 0,
				"channel_sid": "CH6442c09c93ba4d13966fa42e9b78f620",
				"from": "123",
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
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	msg := &twilioflex.CreateChatMessageParams{
		From:       "123",
		Body:       "hello",
		ChannelSid: channelSid,
	}

	_, _, err := client.CreateMessage(msg)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateMessage(msg)
	assert.EqualError(t, err, "Something went wrong")

	response, trace, err := client.CreateMessage(msg)
	assert.NoError(t, err)
	assert.Equal(t, "hello", response.Body)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 708\r\n\r\n", string(trace.ResponseTrace))
}

func TestCompleteTask(t *testing.T) {
	taskSid := "WT1d187abc335f7f16ff050a66f9b6a6b2"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://taskrouter.twilio.com/v1/Workspaces/%s/Tasks/%s", workspaceSid, taskSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
			httpx.NewMockResponse(400, nil, `{
				"code": 20001,
				"message": "Cannot complete task WT1d187abc335f7f16ff050a66f9b6a6b2 in workspace WS954611f5aebc7672d71de836c0179113 for account AC81d44315e19372138bdaffcc13cf3b94 because it is not currently assigned.",
				"more_info": "https://www.twilio.com/docs/errors/20001",
				"status": 400
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

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	_, _, err := client.CompleteTask(taskSid)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CompleteTask(taskSid)
	assert.EqualError(t, err, "Something went wrong")

	_, _, err = client.CompleteTask(taskSid)
	assert.EqualError(t, err, "Cannot complete task WT1d187abc335f7f16ff050a66f9b6a6b2 in workspace WS954611f5aebc7672d71de836c0179113 for account AC81d44315e19372138bdaffcc13cf3b94 because it is not currently assigned.")

	response, trace, err := client.CompleteTask(taskSid)
	assert.NoError(t, err)
	assert.Equal(t, "completed", response.AssignmentStatus)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 1602\r\n\r\n", string(trace.ResponseTrace))
}

func TestCreateMediaResource(t *testing.T) {
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://mcs.us1.twilio.com/v1/Services/%s/Media", serviceSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
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
					"filename": "00ac28a5d76a30d5c8ec4f3a73964887.jpg",
					"author": "system",
					"category": "media",
					"message_sid": null,
					"channel_sid": null,
					"url": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME59b872f1e52fbd6fe6ad956bbb4fa9bd",
					"is_multipart_upstream": false
			}`),
		},
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	mediaContent := &twilioflex.CreateMediaParams{
		FileName: "00ac28a5d76a30d5c8ec4f3a73964887.jpg",
		Media:    []byte(""),
	}

	_, _, err := client.CreateMedia(mediaContent)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.CreateMedia(mediaContent)
	assert.EqualError(t, err, "Something went wrong")

	response, trace, err := client.CreateMedia(mediaContent)
	assert.NoError(t, err)
	assert.Equal(t, "00ac28a5d76a30d5c8ec4f3a73964887.jpg", response.Filename)
	assert.Equal(t, "HTTP/1.0 201 Created\r\nContent-Length: 788\r\n\r\n", string(trace.ResponseTrace))
}

func TestFetchMedia(t *testing.T) {
	mediaSid := "ME59b872f1e52fbd6fe6ad956bbb4fa9bd"
	defer httpx.SetRequestor(httpx.DefaultRequestor)
	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		fmt.Sprintf("https://mcs.us1.twilio.com/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/%s", mediaSid): {
			httpx.MockConnectionError,
			httpx.NewMockResponse(400, nil, `{"message": "Something went wrong", "detail": "Unknown", "code": 1234, "more_info": "https://www.twilio.com/docs/errors/1234"}`),
			httpx.NewMockResponse(200, nil, `{
				"sid": "ME59b872f1e52fbd6fe6ad956bbb4fa9bd",
				"service_sid": "IS38067ec392f1486bb6e4de4610f26fb3",
				"date_created": "2022-03-14T13:10:38.897143-07:00",
				"date_upload_updated": "2022-03-14T13:10:38.906058-07:00",
				"date_updated": "2022-03-14T13:10:38.897143-07:00",
				"links": {
					"content": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME59b872f1e52fbd6fe6ad956bbb4fa9bd/Content",
					"content_direct_temporary": "https://media.us1.twilio.com/ME59b872f1e52fbd6fe6ad956bbb4fa9bd?Expires=1647355356&Signature=n05WWrmDwS4yQ521cNeL9LSH7g1RZg3gpmZ83TAy6eHHuW8KqAGn~wl0p5KGlTJYIhGmfTKhYS8o~zSr1L2iDmFyZDawiueHXqeebFNJiM~tviKn5Inna0mgI~nKSl6iV6F6sKUPnkeAc~AVb7Z3qfDaiyf87ucjyBKRTYkKT7a85c2hhBy4z8DOOeVBWNCEZxA08x-iZDsKYwPtIp~jJIwXrHA5nn3GE62jomjLkfd7RoFVggQhPjmrQQsF9Ock-piPiTb-J3o1risNaHux2rycKCO~U4hndnyo26FEeS71iemIK71hxV7MHtfFEubx04eRYijYRfaUEoWc6IXdxQ__&Key-Pair-Id=APKAJWF6YVTMIIYOF3AA"
				},
				"size": 153611,
				"content_type": "image/jpeg",
				"filename": "00ac28a5d76a30d5c8ec4f3a73964887.jpg",
				"author": "system",
				"category": "media",
				"message_sid": "IMadceb005ef924c728b6abde17d02775c",
				"channel_sid": "CH180fa48ef2ba40a08fa5c9fb5c8ddd99",
				"url": "/v1/Services/IS38067ec392f1486bb6e4de4610f26fb3/Media/ME59b872f1e52fbd6fe6ad956bbb4fa9bd",
				"is_multipart_upstream": false
			}`),
		},
	}))

	client := twilioflex.NewClient(http.DefaultClient, nil, authToken, accountSid, serviceSid, workspaceSid, flexFlowSid)

	_, _, err := client.FetchMedia(mediaSid)
	assert.EqualError(t, err, "unable to connect to server")

	_, _, err = client.FetchMedia(mediaSid)
	assert.EqualError(t, err, "Something went wrong")

	response, trace, err := client.FetchMedia(mediaSid)
	assert.NoError(t, err)
	assert.Equal(t, "ME59b872f1e52fbd6fe6ad956bbb4fa9bd", response.Sid)
	assert.Equal(t, "HTTP/1.0 200 OK\r\nContent-Length: 1342\r\n\r\n", string(trace.ResponseTrace))
}
