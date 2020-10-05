package marker

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
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

// HasTask returns whether the passed in taskID has already been marked for execution
func HasTask(rc redis.Conn, taskGroup string, taskID string) (bool, error) {
	todayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
	yesterdayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().Add(time.Hour*-24).UTC().Format("2006_01_02"))
	found, err := redis.Bool(hasTask.Do(rc, todayKey, yesterdayKey, taskID))
	if err != nil {
		return false, errors.Wrapf(err, "error checking for task: %s for group: %s", taskID, taskGroup)
	}
	return found, nil
}

// AddTask marks the passed in task
func AddTask(rc redis.Conn, taskGroup string, taskID string) error {
	dateKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
	rc.Send("sadd", dateKey, taskID)
	rc.Send("expire", dateKey, oneDay)
	_, err := rc.Do("")
	if err != nil {
		return errors.Wrapf(err, "error adding task: %s to redis set for group: %s", taskID, taskGroup)
	}
	return nil
}

// RemoveTask removes the task with the passed in id from our lock
func RemoveTask(rc redis.Conn, taskGroup string, taskID string) error {
	todayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
	yesterdayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().Add(time.Hour*-24).UTC().Format("2006_01_02"))
	rc.Send("srem", todayKey, taskID)
	rc.Send("srem", yesterdayKey, taskID)
	_, err := rc.Do("")
	if err != nil {
		return errors.Wrapf(err, "error removing task: %s from redis set for group: %s", taskID, taskGroup)
	}
	return nil
}

// ClearTasks removes all tasks for the passed in group (mostly useful in unit testing)
func ClearTasks(rc redis.Conn, taskGroup string) error {
	todayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().UTC().Format("2006_01_02"))
	yesterdayKey := fmt.Sprintf(keyPattern, taskGroup, time.Now().Add(time.Hour*-24).UTC().Format("2006_01_02"))
	rc.Send("del", todayKey)
	rc.Send("del", yesterdayKey)
	_, err := rc.Do("")
	return err
}
