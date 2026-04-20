package tasks

import (
	"context"
	"time"

	"github.com/nyaruka/mailroom"
	"github.com/nyaruka/mailroom/runtime"
)

// Cron is a task to be repeated on a schedule
type Cron interface {
	// Next returns the next schedule time
	Next(time.Time) time.Time

	// Run performs the task
	Run(context.Context, *runtime.Runtime) (map[string]any, error)
}

// RegisterCron registers a new cron job
func RegisterCron(name string, allInstances bool, c Cron) {
	mailroom.RegisterCron(name, allInstances, c.Run, c.Next)
}

// CronNext returns the next time we should fire based on the passed in time and interval
func CronNext(last time.Time, interval time.Duration) time.Time {
	if interval >= time.Second && interval < time.Minute {
		return last.Add(interval - ((time.Duration(last.Second()) * time.Second) % interval))
	} else if interval == time.Minute {
		seconds := time.Duration(60-last.Second()) + 1
		return last.Add(seconds * time.Second)
	} else {
		// no special treatment for other things
		return last.Add(interval)
	}
}
