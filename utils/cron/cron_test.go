package cron_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/utils/cron"

	"github.com/stretchr/testify/assert"
)

func TestCron(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)

	align := func() {
		untilNextSecond := time.Nanosecond * time.Duration(1_000_000_000-time.Now().Nanosecond()) // time until next second boundary
		time.Sleep(untilNextSecond)                                                               // wait until after second boundary
	}

	createCronFunc := func(running *bool, fired *int, delays map[int]time.Duration, defaultDelay time.Duration) cron.Function {
		return func() error {
			if *running {
				assert.Fail(t, "more than 1 thread is trying to run our cron job")
			}

			*running = true
			delay := delays[*fired]
			if delay == 0 {
				delay = defaultDelay
			}
			time.Sleep(delay)
			*fired++
			*running = false
			return nil
		}
	}

	fired := 0
	quit := make(chan bool)
	running := false

	align()

	// start a job that takes ~100 ms and runs every 250ms
	cron.Start(quit, rt, "test1", time.Millisecond*250, false, createCronFunc(&running, &fired, map[int]time.Duration{}, time.Millisecond*100))

	// wait a bit, should only have fired three times (initial time + three repeats)
	time.Sleep(time.Millisecond * 875) // time for 3 delays between tasks plus half of another delay
	assert.Equal(t, 4, fired)

	// tell the job to quit and check we don't see more fires
	close(quit)

	time.Sleep(time.Millisecond * 500)
	assert.Equal(t, 4, fired)

	fired = 0
	quit = make(chan bool)
	running = false

	align()

	// simulate the job taking 400ms to run on the second fire, thus skipping the third fire
	cron.Start(quit, rt, "test2", time.Millisecond*250, false, createCronFunc(&running, &fired, map[int]time.Duration{1: time.Millisecond * 400}, time.Millisecond*100))

	time.Sleep(time.Millisecond * 875)
	assert.Equal(t, 3, fired)

	close(quit)

	// simulate two different instances running the same cron
	cfg1 := *rt.Config
	cfg2 := *rt.Config
	cfg1.InstanceName = "instance1"
	cfg2.InstanceName = "instance2"
	rt1 := *rt
	rt1.Config = &cfg1
	rt2 := *rt
	rt2.Config = &cfg2

	fired1 := 0
	fired2 := 0
	quit = make(chan bool)
	running = false

	align()

	cron.Start(quit, &rt1, "test3", time.Millisecond*250, false, createCronFunc(&running, &fired1, map[int]time.Duration{}, time.Millisecond*100))
	cron.Start(quit, &rt2, "test3", time.Millisecond*250, false, createCronFunc(&running, &fired2, map[int]time.Duration{}, time.Millisecond*100))

	// same number of fires as if only a single instance was running it...
	time.Sleep(time.Millisecond * 875)
	assert.Equal(t, 4, fired1+fired2) // can't say which instances will run the 4 fires

	close(quit)

	fired1 = 0
	fired2 = 0
	quit = make(chan bool)
	running1 := false
	running2 := false

	align()

	// unless we start the cron with allInstances = true
	cron.Start(quit, &rt1, "test4", time.Millisecond*250, true, createCronFunc(&running1, &fired1, map[int]time.Duration{}, time.Millisecond*100))
	cron.Start(quit, &rt2, "test4", time.Millisecond*250, true, createCronFunc(&running2, &fired2, map[int]time.Duration{}, time.Millisecond*100))

	// now both instances fire 4 times
	time.Sleep(time.Millisecond * 875)
	assert.Equal(t, 4, fired1)
	assert.Equal(t, 4, fired2)

	close(quit)
}

func TestNextFire(t *testing.T) {
	tcs := []struct {
		last     time.Time
		interval time.Duration
		expected time.Time
	}{
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 44, 0, time.UTC), time.Minute, time.Date(2000, 1, 1, 1, 2, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 1, 100, time.UTC), time.Millisecond * 150, time.Date(2000, 1, 1, 1, 1, 1, 150000100, time.UTC)},
		{time.Date(2000, 1, 1, 2, 6, 1, 0, time.UTC), time.Minute * 10, time.Date(2000, 1, 1, 2, 16, 1, 0, time.UTC)},
		{time.Date(2000, 1, 1, 1, 1, 4, 0, time.UTC), time.Second * 15, time.Date(2000, 1, 1, 1, 1, 15, 0, time.UTC)},
	}

	for _, tc := range tcs {
		actual := cron.NextFire(tc.last, tc.interval)
		assert.Equal(t, tc.expected, actual, "next fire mismatch for %s + %s", tc.last, tc.interval)
	}
}
