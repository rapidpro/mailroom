package incidents_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/incidents"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndIncidents(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)

	oa1 := testdata.Org1.Load(rt)
	oa2 := testdata.Org2.Load(rt)

	createWebhookEvents := func(count int, elapsed time.Duration) []*events.WebhookCalledEvent {
		evts := make([]*events.WebhookCalledEvent, count)
		for i := range evts {
			req, _ := http.NewRequest("GET", "http://example.com", nil)
			trace := &httpx.Trace{Request: req, StartTime: dates.Now(), EndTime: dates.Now().Add(elapsed)}
			evts[i] = events.NewWebhookCalled(&flows.WebhookCall{Trace: trace}, flows.CallStatusSuccess, "")
		}
		return evts
	}

	node1 := &models.WebhookNode{UUID: "3c703019-8c92-4d28-9be0-a926a934486b"}
	node1.Record(rt, createWebhookEvents(10, time.Second*30))

	// create incident for org 1 based on node which is still unhealthy
	id1, err := models.IncidentWebhooksUnhealthy(ctx, rt.DB, rt.RP, oa1, []flows.NodeUUID{"3c703019-8c92-4d28-9be0-a926a934486b"})
	require.NoError(t, err)

	node2 := &models.WebhookNode{UUID: "07d69080-475b-4395-aa96-ea6c28ea6cb6"}
	node2.Record(rt, createWebhookEvents(10, time.Second*1))

	// create incident for org 2 based on node which is now healthy
	id2, err := models.IncidentWebhooksUnhealthy(ctx, rt.DB, rt.RP, oa2, []flows.NodeUUID{"07d69080-475b-4395-aa96-ea6c28ea6cb6"})
	require.NoError(t, err)

	cron := &incidents.EndIncidentsCron{}
	res, err := cron.Run(ctx, rt)
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"ended": 1}, res)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM notifications_incident WHERE id = $1 AND ended_on IS NULL`, id1).Returns(1)
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM notifications_incident WHERE id = $1 AND ended_on IS NOT NULL`, id2).Returns(1)

	assertredis.SMembers(t, rt.RP, fmt.Sprintf("incident:%d:nodes", id1), []string{"3c703019-8c92-4d28-9be0-a926a934486b"})
	assertredis.SMembers(t, rt.RP, fmt.Sprintf("incident:%d:nodes", id2), []string{}) // healthy node removed
}
