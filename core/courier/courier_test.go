package courier_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/courier"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type msgSpec struct {
	ChannelUUID assets.ChannelUUID
	Text        string
	ContactID   models.ContactID
	URN         urns.URN
	URNID       models.URNID
	Failed      bool
}

func createMsg(t *testing.T, m msgSpec) *models.Msg {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	db.MustExec(`UPDATE orgs_org SET is_suspended = $1 WHERE id = $2`, m.Failed, models.Org1)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, models.Org1, models.RefreshOrg)
	require.NoError(t, err)

	channel := oa.ChannelByUUID(m.ChannelUUID)

	flowMsg := flows.NewMsgOut(m.URN, channel.ChannelReference(), m.Text, nil, nil, nil, flows.NilMsgTopic)
	msg, err := models.NewOutgoingMsg(oa.Org(), channel, m.ContactID, flowMsg, time.Now())
	require.NoError(t, err)
	return msg
}

func TestQueueMessages(t *testing.T) {
	db := testsuite.DB()
	rc := testsuite.RC()
	testsuite.Reset()
	models.FlushCache()

	// convert the Twitter channel to be an Android channel
	db.MustExec(`UPDATE channels_channel SET name = 'Android', channel_type = 'A' WHERE id = $1`, models.TwitterChannelID)
	androidChannelUUID := models.TwitterChannelUUID

	tests := []struct {
		Description string
		Msgs        []msgSpec
		QueueSizes  map[string]int
	}{
		{
			Description: "2 queueable messages",
			Msgs: []msgSpec{
				{
					ChannelUUID: models.TwilioChannelUUID,
					Text:        "normal msg 1",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
				},
				{
					ChannelUUID: models.TwilioChannelUUID,
					Text:        "normal msg 2",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
				},
			},
			QueueSizes: map[string]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": 2,
			},
		},
		{
			Description: "1 queueable message and 1 failed",
			Msgs: []msgSpec{
				{
					ChannelUUID: models.TwilioChannelUUID,
					Text:        "normal msg 1",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
					Failed:      true,
				},
				{
					ChannelUUID: models.TwilioChannelUUID,
					Text:        "normal msg 1",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
				},
			},
			QueueSizes: map[string]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": 1,
			},
		},
		{
			Description: "1 queueable message and 1 Android",
			Msgs: []msgSpec{
				{
					ChannelUUID: androidChannelUUID,
					Text:        "normal msg 1",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
				},
				{
					ChannelUUID: models.TwilioChannelUUID,
					Text:        "normal msg 1",
					ContactID:   models.CathyID,
					URN:         urns.URN(fmt.Sprintf("tel:+250700000001?id=%d", models.CathyURNID)),
					URNID:       models.CathyURNID,
				},
			},
			QueueSizes: map[string]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": 1,
			},
		},
		{
			Description: "0 messages",
			Msgs:        []msgSpec{},
			QueueSizes: map[string]int{
				"msgs:74729f45-7f29-4868-9dc4-90e491e3c7d8|10/0": 0,
			},
		},
	}

	for _, tc := range tests {
		msgs := make([]*models.Msg, len(tc.Msgs))
		for i := range tc.Msgs {
			msgs[i] = createMsg(t, tc.Msgs[i])
		}

		rc.Do("FLUSHDB")
		courier.QueueMessages(rc, msgs)

		for queueKey, size := range tc.QueueSizes {
			if size == 0 {
				result, err := rc.Do("ZCARD", queueKey)
				require.NoError(t, err)
				assert.Equal(t, size, int(result.(int64)))
			} else {
				result, err := rc.Do("ZPOPMAX", queueKey)
				require.NoError(t, err)

				results := result.([]interface{})
				assert.Equal(t, 2, len(results)) // result is (item, score)

				batchJSON := results[0].([]byte)
				var batch []map[string]interface{}
				err = json.Unmarshal(batchJSON, &batch)
				require.NoError(t, err)

				assert.Equal(t, size, len(batch))
			}
		}
	}

	testsuite.Reset()
}
