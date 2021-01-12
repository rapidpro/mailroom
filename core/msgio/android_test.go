package msgio_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/edganiukov/fcm"
)

type MockFCMEndpoint struct {
	server   *httptest.Server
	tokens   []string
	messages []*fcm.Message
}

func (m *MockFCMEndpoint) Handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	requestBody, _ := ioutil.ReadAll(r.Body)

	message := &fcm.Message{}
	jsonx.Unmarshal(requestBody, message)

	m.messages = append(m.messages, message)

	w.Header().Set("Content-Type", "application/json")

	if utils.StringSliceContains(m.tokens, message.Token, false) {
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	} else {
		w.WriteHeader(200)
		w.Write([]byte(`{"error": "bad_token"}`))
	}
}

func newMockFCMEndpoint(tokens ...string) *MockFCMEndpoint {
	mock := &MockFCMEndpoint{tokens: tokens}
	mock.server = httptest.NewServer(http.HandlerFunc(mock.Handle))
	return mock
}

func TestSyncAndroidChannels(t *testing.T) {
	ctx, db, _ := testsuite.Reset()

	mockFCM := newMockFCMEndpoint("FCMID3")
	defer mockFCM.server.Close()

	client, _ := fcm.NewClient("FCMKEY123", fcm.WithEndpoint(mockFCM.server.URL))
	msgio.SetFCMClient(client)

	// convert the existing channels to be Android channels
	db.MustExec(`UPDATE channels_channel SET name = 'Android 1', channel_type = 'A', config = '{"FCM_ID": ""}'::json WHERE id = $1`, models.TwitterChannelID)      // no FCM ID
	db.MustExec(`UPDATE channels_channel SET name = 'Android 2', channel_type = 'A', config = '{"FCM_ID": "FCMID2"}'::json WHERE id = $1`, models.TwilioChannelID) // invalid FCM ID
	db.MustExec(`UPDATE channels_channel SET name = 'Android 3', channel_type = 'A', config = '{"FCM_ID": "FCMID3"}'::json WHERE id = $1`, models.NexmoChannelID)  // valid FCM ID

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	require.NoError(t, err)

	channel1 := oa.ChannelByUUID(models.TwitterChannelUUID)
	channel2 := oa.ChannelByUUID(models.TwilioChannelUUID)
	channel3 := oa.ChannelByUUID(models.NexmoChannelUUID)

	msgio.SyncAndroidChannels([]*models.Channel{channel1, channel2, channel3})

	// check that we try to sync the 2 channels with FCM IDs, even tho one fails
	assert.Equal(t, 2, len(mockFCM.messages))
	assert.Equal(t, "FCMID2", mockFCM.messages[0].Token)
	assert.Equal(t, "FCMID3", mockFCM.messages[1].Token)

	assert.Equal(t, "high", mockFCM.messages[0].Priority)
	assert.Equal(t, "sync", mockFCM.messages[0].CollapseKey)
	assert.Equal(t, map[string]interface{}{"msg": "sync"}, mockFCM.messages[0].Data)
}
