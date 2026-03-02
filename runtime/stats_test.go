package runtime

import (
	"fmt"
	"testing"
	"time"

	"github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	rp, err := vkutil.NewPool("valkey://valkey:6379/15")
	require.NoError(t, err)

	vc := rp.Get()
	defer vc.Close()

	key := fmt.Sprintf("ctask_latency:msg_received:%s", time.Now().UTC().Format("2006-01-02T15"))
	defer func() {
		vc.Do("DEL", key)
	}()

	sc := NewStatsCollector(rp)
	sc.RecordCronTask("make_foos", 10*time.Second)
	sc.RecordCronTask("make_foos", 5*time.Second)
	sc.RecordLLMCall("openai", "gpt-4", 7*time.Second)
	sc.RecordLLMCall("openai", "gpt-4", 3*time.Second)
	sc.RecordLLMCall("anthropic", "claude-3.7", 4*time.Second)

	stats := sc.Extract()
	assert.Equal(t, 2, stats.CronTaskCount["make_foos"])
	assert.Equal(t, 15*time.Second, stats.CronTaskDuration["make_foos"])
	assert.Equal(t, 2, stats.LLMCallCount[LLMTypeAndModel{"openai", "gpt-4"}])
	assert.Equal(t, 10*time.Second, stats.LLMCallDuration[LLMTypeAndModel{"openai", "gpt-4"}])
	assert.Equal(t, 1, stats.LLMCallCount[LLMTypeAndModel{"anthropic", "claude-3.7"}])
	assert.Equal(t, 4*time.Second, stats.LLMCallDuration[LLMTypeAndModel{"anthropic", "claude-3.7"}])

	datums := stats.ToMetrics(true)
	assert.Len(t, datums, 9)

	datums = stats.ToMetrics(false)
	assert.Len(t, datums, 6)

	// no latencies recorded yet
	latencies, err := GetLatencies(rp, "msg_received")
	require.NoError(t, err)
	assert.Len(t, latencies, 0)

	// record latencies for two orgs via RecordContactTask
	sc.RecordContactTask("msg_received", 1, 150*time.Millisecond, 150*time.Millisecond, false)
	sc.RecordContactTask("msg_received", 1, 250*time.Millisecond, 250*time.Millisecond, false)
	sc.RecordContactTask("msg_received", 2, 500*time.Millisecond, 500*time.Millisecond, false)

	assertvk.HGetAll(t, vc, key, map[string]string{
		"1:n": "2", "1:t": "400",
		"2:n": "1", "2:t": "500",
	})

	// get latencies, should be sorted by avg descending
	latencies, err = GetLatencies(rp, "msg_received")
	require.NoError(t, err)

	assert.Len(t, latencies, 2)
	assert.Equal(t, 2, latencies[0].OrgID)
	assert.Equal(t, int64(1), latencies[0].Count)
	assert.Equal(t, int64(500), latencies[0].TotalMS)
	assert.Equal(t, int64(500), latencies[0].AvgMS)

	assert.Equal(t, 1, latencies[1].OrgID)
	assert.Equal(t, int64(2), latencies[1].Count)
	assert.Equal(t, int64(400), latencies[1].TotalMS)
	assert.Equal(t, int64(200), latencies[1].AvgMS)

	// different task type should be empty
	latencies, err = GetLatencies(rp, "event_received")
	require.NoError(t, err)
	assert.Len(t, latencies, 0)
}
