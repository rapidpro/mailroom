package msgio_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type msgSpec struct {
	ChannelID models.ChannelID
	ContactID models.ContactID
	URNID     models.URNID
	Failed    bool
}

func (m *msgSpec) createMsg(t *testing.T, rt *runtime.Runtime, oa *models.OrgAssets) *models.Msg {
	// Only way to create a failed outgoing message is to suspend the org and reload the org.
	// However the channels have to be fetched from the same org assets thus why this uses its
	// own org assets instance.
	ctx := context.Background()
	rt.DB.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, m.Failed, testdata.Org1.ID)
	oaOrg, _ := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg)

	var channel *models.Channel
	var channelRef *assets.ChannelReference

	if m.ChannelID != models.NilChannelID {
		channel = oa.ChannelByID(m.ChannelID)
		channelRef = channel.ChannelReference()
	}
	urn := urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", m.URNID))

	flowMsg := flows.NewMsgOut(urn, channelRef, "Hello", nil, nil, nil, flows.NilMsgTopic)
	msg, err := models.NewOutgoingMsg(rt.Config, oaOrg.Org(), channel, m.ContactID, flowMsg, time.Now())
	require.NoError(t, err)

	models.InsertMessages(ctx, rt.DB, []*models.Msg{msg})
	require.NoError(t, err)

	return msg
}

func TestSendMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

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
			Description: "2 messages for Courier, and 1 Android",
			Msgs: []msgSpec{
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
				{
					ChannelID: androidChannel1.ID,
					ContactID: testdata.Bob.ID,
					URNID:     testdata.Bob.URNID,
				},
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
			},
			QueueSizes: map[string][]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {2},
			},
			FCMTokensSynced: []string{"FCMID1"},
			PendingMsgs:     0,
		},
		{
			Description: "each Android channel synced once",
			Msgs: []msgSpec{
				{
					ChannelID: androidChannel1.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
				{
					ChannelID: androidChannel2.ID,
					ContactID: testdata.Bob.ID,
					URNID:     testdata.Bob.URNID,
				},
				{
					ChannelID: androidChannel1.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
			},
			QueueSizes:      map[string][]int{},
			FCMTokensSynced: []string{"FCMID1", "FCMID2"},
			PendingMsgs:     0,
		},
		{
			Description: "messages without channels set to PENDING",
			Msgs: []msgSpec{
				{
					ChannelID: models.NilChannelID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
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
