package msgio_test

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueCourierMessages(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()
	rc := rp.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// create an Andoid channel
	androidChannel := testdata.InsertChannel(db, testdata.Org1, "A", "Android 1", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOrg|models.RefreshChannels)
	require.NoError(t, err)

	tests := []struct {
		Description string
		Msgs        []msgSpec
		QueueSizes  map[string][]int
	}{
		{
			Description: "2 queueable messages",
			Msgs: []msgSpec{
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
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
		},
		{
			Description: "1 queueable message and 1 failed",
			Msgs: []msgSpec{
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
					Failed:    true,
				},
				{
					ChannelID: testdata.TwilioChannel.ID,
					ContactID: testdata.Cathy.ID,
					URNID:     testdata.Cathy.URNID,
				},
			},
			QueueSizes: map[string][]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": {1},
			},
		},
		{
			Description: "0 messages",
			Msgs:        []msgSpec{},
			QueueSizes:  map[string][]int{},
		},
	}

	for _, tc := range tests {
		var contactID models.ContactID
		msgs := make([]*models.Msg, len(tc.Msgs))
		for i, ms := range tc.Msgs {
			msgs[i] = ms.createMsg(t, rt, oa)
			contactID = ms.ContactID
		}

		rc.Do("FLUSHDB")
		msgio.QueueCourierMessages(rc, contactID, msgs)

		testsuite.AssertCourierQueues(t, tc.QueueSizes, "courier queue sizes mismatch in '%s'", tc.Description)
	}

	// check that trying to queue a courier message will panic
	assert.Panics(t, func() {
		ms := msgSpec{
			ChannelID: androidChannel.ID,
			ContactID: testdata.Cathy.ID,
			URNID:     testdata.Cathy.URNID,
		}
		msgio.QueueCourierMessages(rc, testdata.Cathy.ID, []*models.Msg{ms.createMsg(t, rt, oa)})
	})
}
