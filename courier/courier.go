package courier

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/mailroom/models"
)

const (
	highPriority = 1
	lowPriority  = 0
)

// TODO: use real TPS that channels have

// QueueMessage queues a message to courier
func QueueMessages(rc redis.Conn, msgs []*models.Msg) error {
	now := time.Now()
	epochMS := strconv.FormatFloat(float64(now.UnixNano()/int64(time.Microsecond))/float64(1000000), 'f', 6, 64)

	// TODO: figure out priority better
	priority := lowPriority

	msgJSON, err := json.Marshal(msgs)
	if err != nil {
		return err
	}

	// queue to courier
	// TODO: need to group by channel
	_, err = queueMsg.Do(rc, epochMS, "msgs", msgs[0].ChannelUUID, 10, priority, msgJSON)
	if err != nil {
		for _, m := range msgs {
			m.QueuedOn = now
			m.Status = models.StatusQueued
		}
	}

	return err
}

var queueMsg = redis.NewScript(6, `
-- KEYS: [EpochMS, QueueType, QueueName, TPS, Priority, Value]

-- first push onto our specific queue
-- our queue name is built from the type, name and tps, usually something like: "msgs:uuid1-uuid2-uuid3-uuid4|tps"
local queueKey = KEYS[2] .. ":" .. KEYS[3] .. "|" .. KEYS[4]

-- our priority queue name also includes the priority of the message (we have one queue for default and one for bulk)
local priorityQueueKey = queueKey .. "/" .. KEYS[5]
redis.call("zadd", priorityQueueKey, KEYS[1], KEYS[6])
local tps = tonumber(KEYS[4])

-- if we have a TPS, check whether we are currently throttled
local curr = -1
if tps > 0 then
  local tpsKey = queueKey .. ":tps:" .. math.floor(KEYS[1])
  curr = tonumber(redis.call("get", tpsKey))
end

-- if we aren't then add to our active
if not curr or curr < tps then
redis.call("zincrby", KEYS[2] .. ":active", 0, queueKey)
  return 1
else
  return 0
end
`)
