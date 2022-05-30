package msgio_test

import (
	"context"
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type msgSpec struct {
	Channel      *testdata.Channel
	Contact      *testdata.Contact
	Failed       bool
	HighPriority bool
}

func (m *msgSpec) createMsg(t *testing.T, rt *runtime.Runtime, oa *models.OrgAssets) *models.Msg {
	status := models.MsgStatusQueued
	if m.Failed {
		status = models.MsgStatusFailed
	}

	flowMsg := testdata.InsertOutgoingMsg(rt.DB, testdata.Org1, m.Channel, m.Contact, "Hello", nil, status, m.HighPriority)
	msgs, err := models.GetMessagesByID(context.Background(), rt.DB, testdata.Org1.ID, models.DirectionOut, []models.MsgID{models.MsgID(flowMsg.ID())})
	require.NoError(t, err)

	msg := msgs[0]
	msg.SetURN(m.Contact.URN)

	// use the channel instances in org assets so they're shared between msg instances
	if msg.ChannelID() != models.NilChannelID {
		msg.SetChannel(oa.ChannelByID(msg.ChannelID()))
	}
	return msg
}

func TestSendMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData)

	mockFCM := newMockFCMEndpoint("FCMID3")
	defer mockFCM.Stop()

	fc := mockFCM.Client("FCMKEY123")

	// create some Andoid channels
	androidChannel1 := testdata.InsertChannel(db, testdata.Org1, "A", "Android 1", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID1"})
	androidChannel2 := testdata.InsertChannel(db, testdata.Org1, "A", "Android 2", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID2"})
	testdata.InsertChannel(db, testdata.Org1, "A", "Android 3", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID3"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshChannels)
	require.NoError(t, err)

	tests := []struct {
		Description     string
		Msgs            []msgSpec
		QueueSizes      map[string][]int
		FCMTokensSynced []string
		PendingMsgs     int
	}{
		{
			Description:     "no messages",
			Msgs:            []msgSpec{},
			QueueSizes:      map[string][]int{},
			FCMTokensSynced: []string{},
			PendingMsgs:     0,
		},
		{
			Description: "2 messages for Courier, and 1 Android",
			Msgs: []msgSpec{
				{
					Channel: testdata.TwilioChannel,
					Contact: testdata.Cathy,
				},
				{
					Channel: androidChannel1,
					Contact: testdata.Bob,
				},
				{
					Channel: testdata.TwilioChannel,
					Contact: testdata.Cathy,
				},
				{
					Channel:      testdata.TwilioChannel,
					Contact:      testdata.Bob,
					HighPriority: true,
				},
			},
			QueueSizes: map[string][]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2}, // 2 default priority messages for Cathy
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/1": {1}, // 1 high priority message for Bob
			},
			FCMTokensSynced: []string{"FCMID1"},
			PendingMsgs:     0,
		},
		{
			Description: "each Android channel synced once",
			Msgs: []msgSpec{
				{
					Channel: androidChannel1,
					Contact: testdata.Cathy,
				},
				{
					Channel: androidChannel2,
					Contact: testdata.Bob,
				},
				{
					Channel: androidChannel1,
					Contact: testdata.Cathy,
				},
			},
			QueueSizes:      map[string][]int{},
			FCMTokensSynced: []string{"FCMID1", "FCMID2"},
			PendingMsgs:     0,
		},
		{
			Description: "messages with FAILED status ignored",
			Msgs: []msgSpec{
				{
					Channel: testdata.TwilioChannel,
					Contact: testdata.Cathy,
					Failed:  true,
				},
			},
			QueueSizes:      map[string][]int{},
			FCMTokensSynced: []string{},
			PendingMsgs:     0,
		},
		{
			Description: "messages without channels set to PENDING",
			Msgs: []msgSpec{
				{
					Channel: nil,
					Contact: testdata.Cathy,
				},
			},
			QueueSizes:      map[string][]int{},
			FCMTokensSynced: []string{},
			PendingMsgs:     1,
		},
	}

	for _, tc := range tests {
		msgs := make([]*models.Msg, len(tc.Msgs))
		for i, ms := range tc.Msgs {
			msgs[i] = ms.createMsg(t, rt, oa)
		}

		rc.Do("FLUSHDB")
		mockFCM.Messages = nil

		msgio.SendMessages(ctx, rt, db, fc, msgs)

		testsuite.AssertCourierQueues(t, tc.QueueSizes, "courier queue sizes mismatch in '%s'", tc.Description)

		// check the FCM tokens that were synced
		actualTokens := make([]string, len(mockFCM.Messages))
		for i := range mockFCM.Messages {
			actualTokens[i] = mockFCM.Messages[i].Token
		}

		assert.Equal(t, tc.FCMTokensSynced, actualTokens, "FCM tokens mismatch in '%s'", tc.Description)

		testsuite.AssertQuery(t, db, `SELECT count(*) FROM msgs_msg WHERE status = 'P'`).Returns(tc.PendingMsgs, `pending messages mismatch in '%s'`, tc.Description)
	}
}
