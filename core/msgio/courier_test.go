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
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rc := testsuite.RC()
	testsuite.Reset()
	models.FlushCache()

	defer rc.Close()

	// create an Andoid channel
	androidChannelID := testdata.InsertChannel(t, db, models.Org1, "A", "Android 1", []string{"tel"}, "SR", map[string]interface{}{"FCM_ID": "FCMID"})

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg|models.RefreshChannels)
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
					ChannelID: models.TwilioChannelID,
					ContactID: models.CathyID,
					URNID:     models.CathyURNID,
				},
				{
					ChannelID: models.TwilioChannelID,
					ContactID: models.CathyID,
					URNID:     models.CathyURNID,
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
					ChannelID: models.TwilioChannelID,
					ContactID: models.CathyID,
					URNID:     models.CathyURNID,
					Failed:    true,
				},
				{
					ChannelID: models.TwilioChannelID,
					ContactID: models.CathyID,
					URNID:     models.CathyURNID,
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
			msgs[i] = ms.createMsg(t, db, oa)
			contactID = ms.ContactID
		}

		rc.Do("FLUSHDB")
		msgio.QueueCourierMessages(rc, contactID, msgs)

		testsuite.AssertCourierQueues(t, tc.QueueSizes, "courier queue sizes mismatch in '%s'", tc.Description)
	}

	// check that trying to queue a courier message will panic
	assert.Panics(t, func() {
		ms := msgSpec{
			ChannelID: androidChannelID,
			ContactID: models.CathyID,
			URNID:     models.CathyURNID,
		}
		msgio.QueueCourierMessages(rc, models.CathyID, []*models.Msg{ms.createMsg(t, db, oa)})
	})

	testsuite.Reset()
}
