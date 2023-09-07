package msgio_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edganiukov/fcm"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockFCMEndpoint struct {
	server *httptest.Server
	tokens []string

	// log of messages sent to this endpoint
	Messages []*fcm.Message
}

func (m *MockFCMEndpoint) Handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	requestBody, _ := io.ReadAll(r.Body)

	message := &fcm.Message{}
	jsonx.Unmarshal(requestBody, message)

	m.Messages = append(m.Messages, message)

	w.Header().Set("Content-Type", "application/json")

	if utils.StringSliceContains(m.tokens, message.Token, false) {
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	} else {
		w.WriteHeader(401)
		w.Write([]byte(`{"error": "bad_token"}`))
	}
}

func (m *MockFCMEndpoint) Stop() {
	m.server.Close()
}

func (m *MockFCMEndpoint) Client(apiKey string) *fcm.Client {
	client, _ := fcm.NewClient("FCMKEY123", fcm.WithEndpoint(m.server.URL))
	return client
}

func newMockFCMEndpoint(tokens ...string) *MockFCMEndpoint {
	mock := &MockFCMEndpoint{tokens: tokens}
	mock.server = httptest.NewServer(http.HandlerFunc(mock.Handle))
	return mock
}

func TestSyncAndroidChannel(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	mockFCM := newMockFCMEndpoint("FCMID3")
	defer mockFCM.Stop()

	fc := mockFCM.Client("FCMKEY123")

	// create some Android channels
	testChannel1 := testdata.InsertChannel(rt, testdata.Org1, "A", "Android 1", []string{"tel"}, "SR", map[string]any{"FCM_ID": ""})       // no FCM ID
	testChannel2 := testdata.InsertChannel(rt, testdata.Org1, "A", "Android 2", []string{"tel"}, "SR", map[string]any{"FCM_ID": "FCMID2"}) // invalid FCM ID
	testChannel3 := testdata.InsertChannel(rt, testdata.Org1, "A", "Android 3", []string{"tel"}, "SR", map[string]any{"FCM_ID": "FCMID3"}) // valid FCM ID

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshChannels)
	require.NoError(t, err)

	channel1 := oa.ChannelByID(testChannel1.ID)
	channel2 := oa.ChannelByID(testChannel2.ID)
	channel3 := oa.ChannelByID(testChannel3.ID)

	err = msgio.SyncAndroidChannel(nil, channel1)
	assert.EqualError(t, err, "instance has no FCM configuration")
	err = msgio.SyncAndroidChannel(fc, channel1)
	assert.EqualError(t, err, "channel has no FCM ID")
	err = msgio.SyncAndroidChannel(fc, channel2)
	assert.EqualError(t, err, "error syncing channel: 401 error: 401 Unauthorized")
	err = msgio.SyncAndroidChannel(fc, channel3)
	assert.NoError(t, err)

	// check that we try to sync the 2 channels with FCM IDs, even tho one fails
	assert.Equal(t, 2, len(mockFCM.Messages))
	assert.Equal(t, "FCMID2", mockFCM.Messages[0].Token)
	assert.Equal(t, "FCMID3", mockFCM.Messages[1].Token)

	assert.Equal(t, "high", mockFCM.Messages[0].Priority)
	assert.Equal(t, "sync", mockFCM.Messages[0].CollapseKey)
	assert.Equal(t, map[string]any{"msg": "sync"}, mockFCM.Messages[0].Data)
}

func TestCreateFCMClient(t *testing.T) {
	_, rt := testsuite.Runtime()

	rt.Config.FCMKey = "1234"

	assert.NotNil(t, msgio.CreateFCMClient(rt.Config))

	rt.Config.FCMKey = ""

	assert.Nil(t, msgio.CreateFCMClient(rt.Config))
}
