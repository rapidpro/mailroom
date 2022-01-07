package models_test

import (
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/redisx/assertredis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncidentWebhooksUnhealthy(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa := testdata.Org1.Load(rt)

	id1, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa, []flows.NodeUUID{"5a2e83f1-efa8-40ba-bc0c-8873c525de7d", "aba89043-6f0a-4ccf-ba7f-0e1674b90759"})
	require.NoError(t, err)
	assert.NotEqual(t, 0, id1)

	assertdb.Query(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)
	assertredis.SMembers(t, rp, fmt.Sprintf("incident:%d:nodes", id1), []string{"5a2e83f1-efa8-40ba-bc0c-8873c525de7d", "aba89043-6f0a-4ccf-ba7f-0e1674b90759"})

	// raising same incident doesn't create a new one...
	id2, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa, []flows.NodeUUID{"3b1743cd-bd8b-449e-8e8a-11a3bc479766"})
	require.NoError(t, err)
	assert.Equal(t, id1, id2)

	// but will add new nodes to the incident's node set
	assertdb.Query(t, db, `SELECT count(*) FROM notifications_incident`).Returns(1)
	assertredis.SMembers(t, rp, fmt.Sprintf("incident:%d:nodes", id1), []string{"3b1743cd-bd8b-449e-8e8a-11a3bc479766", "5a2e83f1-efa8-40ba-bc0c-8873c525de7d", "aba89043-6f0a-4ccf-ba7f-0e1674b90759"})

	// when the incident has ended, a new one can be created
	db.MustExec(`UPDATE notifications_incident SET ended_on = NOW()`)

	id3, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa, nil)
	require.NoError(t, err)
	assert.NotEqual(t, id1, id3)

	assertdb.Query(t, db, `SELECT count(*) FROM notifications_incident`).Returns(2)

}

func TestGetOpenIncidents(t *testing.T) {
	ctx, rt, db, rp := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	oa1 := testdata.Org1.Load(rt)
	oa2 := testdata.Org2.Load(rt)

	// create incident for org 1
	id1, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa1, nil)
	require.NoError(t, err)

	incidents, err := models.GetOpenIncidents(ctx, db, []models.IncidentType{models.IncidentTypeWebhooksUnhealthy})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))
	assert.Equal(t, id1, incidents[0].ID)
	assert.Equal(t, models.IncidentTypeWebhooksUnhealthy, incidents[0].Type)

	// but then end it
	err = incidents[0].End(ctx, db)
	require.NoError(t, err)

	// and create another one...
	id2, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa1, nil)
	require.NoError(t, err)

	// create an incident for org 2
	id3, err := models.IncidentWebhooksUnhealthy(ctx, db, rp, oa2, nil)
	require.NoError(t, err)

	incidents, err = models.GetOpenIncidents(ctx, db, []models.IncidentType{models.IncidentTypeWebhooksUnhealthy})
	require.NoError(t, err)

	assert.Equal(t, 2, len(incidents))

	sort.Slice(incidents, func(i, j int) bool { return incidents[i].ID < incidents[j].ID }) // db results aren't ordered

	assert.Equal(t, id2, incidents[0].ID)
	assert.Equal(t, id3, incidents[1].ID)
}

func TestWebhookNode(t *testing.T) {
	_, rt, _, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetRedis)

	node := &models.WebhookNode{UUID: "3c703019-8c92-4d28-9be0-a926a934486b"}
	healthy, err := node.Healthy(rt)
	assert.NoError(t, err)
	assert.True(t, healthy)

	createWebhookEvents := func(count int, elapsed time.Duration) []*events.WebhookCalledEvent {
		evts := make([]*events.WebhookCalledEvent, count)
		for i := range evts {
			req, _ := http.NewRequest("GET", "http://example.com", nil)
			trace := &httpx.Trace{Request: req, StartTime: dates.Now(), EndTime: dates.Now().Add(elapsed)}
			evts[i] = events.NewWebhookCalled(&flows.WebhookCall{Trace: trace}, flows.CallStatusSuccess, "")
		}
		return evts
	}

	// record 10 healthy calls
	err = node.Record(rt, createWebhookEvents(10, time.Second*1))
	assert.NoError(t, err)

	healthy, err = node.Healthy(rt)
	assert.NoError(t, err)
	assert.True(t, healthy)

	// record 5 unhealthy calls
	err = node.Record(rt, createWebhookEvents(5, time.Second*30))
	assert.NoError(t, err)

	healthy, err = node.Healthy(rt)
	assert.NoError(t, err)
	assert.True(t, healthy)

	// record another 5 unhealthy calls
	err = node.Record(rt, createWebhookEvents(5, time.Second*30))
	assert.NoError(t, err)

	healthy, err = node.Healthy(rt)
	assert.NoError(t, err)
	assert.False(t, healthy)
}
