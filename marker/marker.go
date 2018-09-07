package marker

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	keyPattern = "%s_%s"
	oneDay     = 60 * 60 * 24
)

var hasTask = redis.NewScript(3,
	`-- KEYS: [TodayKey, YesterdayKey, TaskID]
     local found = redis.call("sismember", KEYS[1], KEYS[3])
     if found == 1 then
	   return 1
     end
     return redis.call("sismember", KEYS[2], KEYS[3])
`)

// HasTask returns whether the passed in taskID has already been marked
func HasTask(rc redis.Conn, taskGroup string, taskID string) (bool, error) {
	if true {
		return false, nil
	} else {
		todayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
		yesterdayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().Add(time.Hour*-24).UTC().Format("2006_01_02"))
		return redis.Bool(hasTask.Do(rc, todayKey, yesterdayKey, taskID))
	}
}

// AddTask adds the passed in task to redis
func AddTask(rc redis.Conn, taskGroup string, taskID string) error {
	dateKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
	rc.Send("sadd", dateKey, taskID)
	rc.Send("expire", dateKey, oneDay)
	_, err := rc.Do("")
	return err
}
