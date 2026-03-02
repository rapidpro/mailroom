package runtime_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	_, rt := testsuite.Runtime(t)
	defer testsuite.Reset(t, rt, testsuite.ResetValkey)

	vc := rt.VK.Get()
	defer vc.Close()

	sc := runtime.NewStatsCollector(rt.VK)
	sc.RecordCronTask("make_foos", 10*time.Second)
	sc.RecordCronTask("make_foos", 5*time.Second)
	sc.RecordLLMCall("openai", "gpt-4", 7*time.Second)
	sc.RecordLLMCall("openai", "gpt-4", 3*time.Second)
	sc.RecordLLMCall("anthropic", "claude-3.7", 4*time.Second)

	stats := sc.Extract()
	assert.Equal(t, 2, stats.CronTaskCount["make_foos"])
	assert.Equal(t, 15*time.Second, stats.CronTaskDuration["make_foos"])
	assert.Equal(t, 2, stats.LLMCallCount[runtime.LLMTypeAndModel{Type: "openai", Model: "gpt-4"}])
	assert.Equal(t, 10*time.Second, stats.LLMCallDuration[runtime.LLMTypeAndModel{Type: "openai", Model: "gpt-4"}])
	assert.Equal(t, 1, stats.LLMCallCount[runtime.LLMTypeAndModel{Type: "anthropic", Model: "claude-3.7"}])
	assert.Equal(t, 4*time.Second, stats.LLMCallDuration[runtime.LLMTypeAndModel{Type: "anthropic", Model: "claude-3.7"}])

	datums := stats.ToMetrics(true)
	assert.Len(t, datums, 9)

	datums = stats.ToMetrics(false)
	assert.Len(t, datums, 6)

	// no latencies recorded yet
	latencies, err := runtime.GetCTaskLatencies(rt.VK)
	require.NoError(t, err)
	assert.Len(t, latencies, 0)

	// record latencies for two orgs and two task types
	sc.RecordContactTask("msg_received", 1, 150*time.Millisecond, 150*time.Millisecond, false)
	sc.RecordContactTask("msg_received", 1, 250*time.Millisecond, 250*time.Millisecond, false)
	sc.RecordContactTask("msg_received", 2, 500*time.Millisecond, 500*time.Millisecond, false)
	sc.RecordContactTask("event_received", 2, 100*time.Millisecond, 100*time.Millisecond, false)

	key := fmt.Sprintf("ctask_latency:%s", time.Now().UTC().Format("2006-01-02T15"))

	assertvk.HGetAll(t, vc, key, map[string]string{
		"1/msg_received:n": "2", "1/msg_received:t": "400",
		"2/msg_received:n": "1", "2/msg_received:t": "500",
		"2/event_received:n": "1", "2/event_received:t": "100",
	})

	// get latencies grouped by org, sorted by org total descending
	latencies, err = runtime.GetCTaskLatencies(rt.VK)
	require.NoError(t, err)

	assert.Equal(t, []runtime.OrgCTaskLatency{
		{OrgID: 2, TotalMS: 600, Tasks: []runtime.TaskLatency{
			{Type: "msg_received", Count: 1, TotalMS: 500, AvgMS: 500},
			{Type: "event_received", Count: 1, TotalMS: 100, AvgMS: 100},
		}},
		{OrgID: 1, TotalMS: 400, Tasks: []runtime.TaskLatency{
			{Type: "msg_received", Count: 2, TotalMS: 400, AvgMS: 200},
		}},
	}, latencies)
}
