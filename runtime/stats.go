package runtime

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/aws/cwatch"
)

type LLMTypeAndModel struct {
	Type  string
	Model string
}

type Stats struct {
	ContactTaskCount    map[string]int           // number of contact tasks handled by type
	ContactTaskErrors   map[string]int           // number of contact tasks that errored by type
	ContactTaskDuration map[string]time.Duration // total time spent handling contact tasks
	ContactTaskLatency  map[string]time.Duration // total time spent queuing and handling contact tasks
	RealtimeLockFails   int                      // number of times an attempt to get a contact lock failed

	CronTaskCount    map[string]int           // number of cron tasks run by type
	CronTaskDuration map[string]time.Duration // total time spent running cron tasks

	LLMCallCount    map[LLMTypeAndModel]int           // number of LLM calls run by type
	LLMCallDuration map[LLMTypeAndModel]time.Duration // total time spent making LLM calls

	WebhookCallCount    int           // number of webhook calls
	WebhookCallDuration time.Duration // total time spent handling webhook calls
}

func newStats() *Stats {
	return &Stats{
		ContactTaskCount:    make(map[string]int),
		ContactTaskErrors:   make(map[string]int),
		ContactTaskDuration: make(map[string]time.Duration),
		ContactTaskLatency:  make(map[string]time.Duration),

		CronTaskCount:    make(map[string]int),
		CronTaskDuration: make(map[string]time.Duration),

		LLMCallCount:    make(map[LLMTypeAndModel]int),
		LLMCallDuration: make(map[LLMTypeAndModel]time.Duration),
	}
}

func (s *Stats) ToMetrics(advanced bool) []types.MetricDatum {
	metrics := make([]types.MetricDatum, 0, 20)

	for typ, count := range s.ContactTaskCount {
		// convert task timings to averages
		avgDuration := s.ContactTaskDuration[typ] / time.Duration(count)
		avgLatency := s.ContactTaskLatency[typ] / time.Duration(count)

		metrics = append(metrics,
			cwatch.Datum("HandlerTaskCount", float64(count), types.StandardUnitCount, cwatch.Dimension("TaskType", typ)),
			cwatch.Datum("HandlerTaskErrors", float64(s.ContactTaskErrors[typ]), types.StandardUnitCount, cwatch.Dimension("TaskType", typ)),
			cwatch.Datum("HandlerTaskDuration", float64(avgDuration)/float64(time.Second), types.StandardUnitCount, cwatch.Dimension("TaskType", typ)),
			cwatch.Datum("HandlerTaskLatency", float64(avgLatency)/float64(time.Second), types.StandardUnitCount, cwatch.Dimension("TaskType", typ)),
		)
	}

	for typeAndModel, count := range s.LLMCallCount {
		avgTime := s.LLMCallDuration[typeAndModel] / time.Duration(count)

		metrics = append(metrics,
			cwatch.Datum("LLMCallCount", float64(count), types.StandardUnitCount, cwatch.Dimension("LLMType", typeAndModel.Type), cwatch.Dimension("LLMModel", typeAndModel.Model)),
			cwatch.Datum("LLMCallDuration", float64(avgTime)/float64(time.Second), types.StandardUnitSeconds, cwatch.Dimension("LLMType", typeAndModel.Type), cwatch.Dimension("LLMModel", typeAndModel.Model)),
		)
	}

	var avgWebhookDuration time.Duration
	if s.WebhookCallCount > 0 {
		avgWebhookDuration = s.WebhookCallDuration / time.Duration(s.WebhookCallCount)
	}

	metrics = append(metrics,
		cwatch.Datum("WebhookCallCount", float64(s.WebhookCallCount), types.StandardUnitCount),
		cwatch.Datum("WebhookCallDuration", float64(avgWebhookDuration)/float64(time.Second), types.StandardUnitSeconds),
	)

	if advanced {
		metrics = append(metrics,
			cwatch.Datum("HandlerLockFails", float64(s.RealtimeLockFails), types.StandardUnitCount),
		)

		for name, count := range s.CronTaskCount {
			avgTime := s.CronTaskDuration[name] / time.Duration(count)

			metrics = append(metrics,
				cwatch.Datum("CronTaskCount", float64(count), types.StandardUnitCount, cwatch.Dimension("TaskType", name)),
				cwatch.Datum("CronTaskDuration", float64(avgTime)/float64(time.Second), types.StandardUnitSeconds, cwatch.Dimension("TaskType", name)),
			)
		}
	}

	return metrics
}

// StatsCollector provides threadsafe stats collection
type StatsCollector struct {
	vk    *redis.Pool
	mutex sync.Mutex
	stats *Stats
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector(vk *redis.Pool) *StatsCollector {
	return &StatsCollector{vk: vk, stats: newStats()}
}

func (c *StatsCollector) RecordContactTask(typ string, orgID int, d, l time.Duration, errored bool) {
	c.mutex.Lock()
	c.stats.ContactTaskCount[typ]++
	c.stats.ContactTaskDuration[typ] += d
	c.stats.ContactTaskLatency[typ] += l
	if errored {
		c.stats.ContactTaskErrors[typ]++
	}
	c.mutex.Unlock()

	c.recordCTaskLatency(orgID, typ, l)
}

func (c *StatsCollector) RecordRealtimeLockFail() {
	c.mutex.Lock()
	c.stats.RealtimeLockFails++
	c.mutex.Unlock()
}

func (c *StatsCollector) RecordCronTask(name string, d time.Duration) {
	c.mutex.Lock()
	c.stats.CronTaskCount[name]++
	c.stats.CronTaskDuration[name] += d
	c.mutex.Unlock()
}

func (c *StatsCollector) RecordWebhookCall(d time.Duration) {
	c.mutex.Lock()
	c.stats.WebhookCallCount++
	c.stats.WebhookCallDuration += d
	c.mutex.Unlock()
}

func (c *StatsCollector) RecordLLMCall(typ, model string, d time.Duration) {
	c.mutex.Lock()
	c.stats.LLMCallCount[LLMTypeAndModel{typ, model}]++
	c.stats.LLMCallDuration[LLMTypeAndModel{typ, model}] += d
	c.mutex.Unlock()
}

// Extract returns the stats for the period since the last call
func (c *StatsCollector) Extract() *Stats {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	s := c.stats
	c.stats = newStats()
	return s
}

var recordLatencyScript = redis.NewScript(1, `
local key = KEYS[1]
local field_n = ARGV[1]
local field_t = ARGV[2]
local latency_ms = tonumber(ARGV[3])

redis.call("HINCRBY", key, field_n, 1)
redis.call("HINCRBY", key, field_t, latency_ms)
redis.call("EXPIRE", key, 90000)

return 1
`)

// records a contact task's latency in Valkey, keyed by org and task type (best effort).
func (c *StatsCollector) recordCTaskLatency(orgID int, taskType string, latency time.Duration) {
	if c.vk == nil {
		return
	}

	vc := c.vk.Get()
	defer vc.Close()

	key := fmt.Sprintf("ctask_latency:%s", time.Now().UTC().Format("2006-01-02T15"))
	field := fmt.Sprintf("%d/%s", orgID, taskType)

	if _, err := recordLatencyScript.Do(vc, key, field+":n", field+":t", latency.Milliseconds()); err != nil {
		slog.Error("error recording per-org latency", "error", err)
	}
}

// OrgCTaskLatency holds latency statistics for all contact task types for a single org
type OrgCTaskLatency struct {
	OrgID   int            `json:"org_id"`
	TotalMS int64          `json:"total_ms"`
	Tasks   []TaskLatency  `json:"tasks"`
}

// TaskLatency holds latency statistics for a single contact task type
type TaskLatency struct {
	Type    string `json:"type"`
	Count   int64  `json:"count"`
	TotalMS int64  `json:"total_ms"`
	AvgMS   int64  `json:"avg_ms"`
}

// GetCTaskLatencies returns per-org latency statistics for the current hourly bucket, grouped by
// org and sorted by org total latency descending. Tasks within each org are sorted by total latency
// descending.
func GetCTaskLatencies(rp *redis.Pool) ([]OrgCTaskLatency, error) {
	vc := rp.Get()
	defer vc.Close()

	key := fmt.Sprintf("ctask_latency:%s", time.Now().UTC().Format("2006-01-02T15"))

	values, err := redis.Values(vc.Do("HGETALL", key))
	if err != nil {
		return nil, fmt.Errorf("error getting latency data: %w", err)
	}

	type entryKey struct {
		orgID    int
		taskType string
	}
	type entry struct {
		count   int64
		totalMS int64
	}
	entries := make(map[entryKey]*entry)

	for i := 0; i < len(values); i += 2 {
		field, _ := redis.String(values[i], nil)
		val, _ := redis.Int64(values[i+1], nil)

		// field format is "{orgID}/{taskType}:n" or "{orgID}/{taskType}:t"
		suffixIdx := strings.LastIndex(field, ":")
		if suffixIdx == -1 {
			continue
		}
		prefix := field[:suffixIdx]
		suffix := field[suffixIdx+1:]

		before, after, ok := strings.Cut(prefix, "/")
		if !ok {
			continue
		}

		orgID, err := strconv.Atoi(before)
		if err != nil {
			continue
		}

		ek := entryKey{orgID, after}
		e, ok := entries[ek]
		if !ok {
			e = &entry{}
			entries[ek] = e
		}

		switch suffix {
		case "n":
			e.count = val
		case "t":
			e.totalMS = val
		}
	}

	// group by org
	orgs := make(map[int]*OrgCTaskLatency)
	for ek, e := range entries {
		org, ok := orgs[ek.orgID]
		if !ok {
			org = &OrgCTaskLatency{OrgID: ek.orgID}
			orgs[ek.orgID] = org
		}

		var avgMS int64
		if e.count > 0 {
			avgMS = e.totalMS / e.count
		}

		org.TotalMS += e.totalMS
		org.Tasks = append(org.Tasks, TaskLatency{Type: ek.taskType, Count: e.count, TotalMS: e.totalMS, AvgMS: avgMS})
	}

	result := make([]OrgCTaskLatency, 0, len(orgs))
	for _, org := range orgs {
		sort.Slice(org.Tasks, func(i, j int) bool {
			return org.Tasks[i].TotalMS > org.Tasks[j].TotalMS
		})
		result = append(result, *org)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalMS > result[j].TotalMS
	})

	return result, nil
}
