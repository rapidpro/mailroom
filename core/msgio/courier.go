package msgio

import (
	"strconv"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	bulkPriority = 0
	highPriority = 1
)

var queuePushScript = redis.NewScript(6, `
-- KEYS: [QueueType, QueueName, TPS, Priority, Items, EpochSecs]
local queueType, queueName, tps, priority, items, epochSecs = KEYS[1], KEYS[2], tonumber(KEYS[3]), KEYS[4], KEYS[5], KEYS[6]

-- first construct the base key for this queue from the type + name + tps, e.g. "msgs:0a77a158-1dcb-4c06-9aee-e15bdf64653e|10"
local queueKey = queueType .. ":" .. queueName .. "|" .. tps

-- each queue than has two sorted sets for bulk and high priority items, e.g. "msgs:0a77..653e|10/0" vs msgs:0a77..653e|10/1"
local priorityQueueKey = queueKey .. "/" .. priority

-- add the items to the sorted set using the full timestamp (e.g. 1636556789.123456) as the score
redis.call("ZADD", priorityQueueKey, epochSecs, items)

-- if we have a TPS limit, check the transaction counter for this epoch second to see if have already reached it
local curr = -1
if tps > 0 then
  local tpsKey = queueKey .. ":tps:" .. math.floor(epochSecs) -- e.g. "msgs:0a77..4653e|10:tps:1636556789"
  curr = tonumber(redis.call("GET", tpsKey))
end

-- if we haven't hit the limit, add this queue to set of active queues 
if not curr or curr < tps then
  redis.call("ZINCRBY", queueType .. ":active", 0, queueKey)
  return 1
else
  return 0
end
`)

// PushCourierBatch pushes a batch of messages for a single contact and channel onto the appropriate courier queue
func PushCourierBatch(rc redis.Conn, ch *models.Channel, batch []*models.Msg, timestamp string) error {
	priority := bulkPriority
	if batch[0].HighPriority() {
		priority = highPriority
	}
	batchJSON := jsonx.MustMarshal(batch)

	_, err := queuePushScript.Do(rc, "msgs", ch.UUID(), ch.TPS(), priority, batchJSON, timestamp)
	return err
}

// QueueCourierMessages queues messages for a single contact to Courier
func QueueCourierMessages(rc redis.Conn, contactID models.ContactID, msgs []*models.Msg) error {
	if len(msgs) == 0 {
		return nil
	}

	// get the time in seconds since the epoch as a floating point number
	// e.g. 2021-11-10T15:10:49.123456+00:00 => "1636557205.123456"
	now := dates.Now()
	epochSeconds := strconv.FormatFloat(float64(now.UnixNano()/int64(time.Microsecond))/float64(1000000), 'f', 6, 64)

	// we batch msgs by channel uuid
	batch := make([]*models.Msg, 0, len(msgs))
	currentChannel := msgs[0].Channel()

	// commits our batch to redis
	commitBatch := func() error {
		if len(batch) > 0 {
			start := time.Now()
			err := PushCourierBatch(rc, currentChannel, batch, epochSeconds)
			if err != nil {
				return err
			}
			logrus.WithFields(logrus.Fields{
				"msgs":         len(batch),
				"contact_id":   contactID,
				"channel_uuid": currentChannel.UUID(),
				"elapsed":      time.Since(start),
			}).Info("msgs queued to courier")
		}
		return nil
	}

	for _, msg := range msgs {
		// android messages should never get in here
		if msg.Channel() != nil && msg.Channel().Type() == models.ChannelTypeAndroid {
			panic("trying to queue android messages to courier")
		}

		// ignore any message without a channel or already marked as failed (maybe org is suspended)
		if msg.ChannelUUID() == "" || msg.Status() == models.MsgStatusFailed {
			continue
		}

		// nil channel object but have channel UUID? that's an error
		if msg.Channel() == nil {
			return errors.Errorf("msg passed in without channel set")
		}

		// no contact urn id or urn, also an error
		if msg.URN() == urns.NilURN || msg.ContactURNID() == nil {
			return errors.Errorf("msg passed with nil urn: %s", msg.URN())
		}

		// same channel? add to batch
		if msg.Channel() == currentChannel {
			batch = append(batch, msg)
		}

		// different channel? queue it up
		if msg.Channel() != currentChannel {
			err := commitBatch()
			if err != nil {
				return err
			}

			currentChannel = msg.Channel()
			batch = []*models.Msg{msg}
		}
	}

	// any remaining in our batch, queue it up
	return commitBatch()
}
