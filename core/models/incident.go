package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null"
	"github.com/nyaruka/redisx"
	"github.com/pkg/errors"
)

// IncidentID is our type for incident ids
type IncidentID null.Int

const NilIncidentID = IncidentID(0)

// MarshalJSON marshals into JSON. 0 values will become null
func (i IncidentID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *IncidentID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i IncidentID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *IncidentID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

type IncidentType string

const (
	IncidentTypeOrgFlagged        IncidentType = "org:flagged"
	IncidentTypeWebhooksUnhealthy IncidentType = "webhooks:unhealthy"
)

type Incident struct {
	ID        IncidentID   `db:"id"`
	OrgID     OrgID        `db:"org_id"`
	Type      IncidentType `db:"incident_type"`
	Scope     string       `db:"scope"`
	StartedOn time.Time    `db:"started_on"`
	EndedOn   *time.Time   `db:"ended_on"`
	ChannelID ChannelID    `db:"channel_id"`
}

// End ends this incident
func (i *Incident) End(ctx context.Context, db Queryer) error {
	now := time.Now()
	i.EndedOn = &now
	_, err := db.ExecContext(ctx, `UPDATE notifications_incident SET ended_on = $2 WHERE id = $1`, i.ID, i.EndedOn)
	return errors.Wrap(err, "error updating incident ended_on")
}

// IncidentWebhooksUnhealthy ensures there is an open unhealthy webhooks incident for the given org
func IncidentWebhooksUnhealthy(ctx context.Context, db Queryer, rp *redis.Pool, oa *OrgAssets, nodes []flows.NodeUUID) (IncidentID, error) {
	id, err := getOrCreateIncident(ctx, db, oa, &Incident{
		OrgID:     oa.OrgID(),
		Type:      IncidentTypeWebhooksUnhealthy,
		StartedOn: dates.Now(),
		Scope:     "",
	})
	if err != nil {
		return NilIncidentID, err
	}

	if len(nodes) > 0 {
		rc := rp.Get()
		defer rc.Close()

		nodesKey := fmt.Sprintf("incident:%d:nodes", id)
		rc.Send("MULTI")
		rc.Send("SADD", redis.Args{}.Add(nodesKey).AddFlat(nodes)...)
		rc.Send("EXPIRE", nodesKey, 60*30) // 30 minutes
		_, err = rc.Do("EXEC")
		if err != nil {
			return NilIncidentID, errors.Wrap(err, "error adding node uuids to incident")
		}
	}

	return id, nil
}

const insertIncidentSQL = `
INSERT INTO notifications_incident(org_id, incident_type, scope, started_on, channel_id) VALUES($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING RETURNING id`

func getOrCreateIncident(ctx context.Context, db Queryer, oa *OrgAssets, incident *Incident) (IncidentID, error) {
	var incidentID IncidentID
	err := db.GetContext(ctx, &incidentID, insertIncidentSQL, incident.OrgID, incident.Type, incident.Scope, incident.StartedOn, incident.ChannelID)
	if err != nil && err != sql.ErrNoRows {
		return NilIncidentID, errors.Wrap(err, "error inserting incident")
	}

	// if we got back an id, a new incident was actually created
	if incidentID != NilIncidentID {
		incident.ID = incidentID

		if err := NotifyIncidentStarted(ctx, db, oa, incident); err != nil {
			return NilIncidentID, errors.Wrap(err, "error creating notifications for new incident")
		}
	} else {
		err := db.GetContext(ctx, &incidentID, `SELECT id FROM notifications_incident WHERE org_id = $1 AND incident_type = $2 AND scope = $3`, incident.OrgID, incident.Type, incident.Scope)
		if err != nil {
			return NilIncidentID, errors.Wrap(err, "error looking up existing incident")
		}
	}

	return incidentID, nil
}

const selectOpenIncidentsSQL = `
SELECT id, org_id, incident_type, scope, started_on, ended_on, channel_id
FROM notifications_incident
WHERE ended_on IS NULL AND incident_type = ANY($1)`

func GetOpenIncidents(ctx context.Context, db Queryer, types []IncidentType) ([]*Incident, error) {
	rows, err := db.QueryxContext(ctx, selectOpenIncidentsSQL, pq.Array(types))
	if err != nil {
		return nil, errors.Wrap(err, "error querying open incidents")
	}
	defer rows.Close()

	incidents := make([]*Incident, 0, 10)
	for rows.Next() {
		obj := &Incident{}
		err := rows.StructScan(obj)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning incident")
		}

		incidents = append(incidents, obj)
	}

	return incidents, nil
}

// WebhookNode is a utility to help determine the health of an individual webhook node
type WebhookNode struct {
	UUID flows.NodeUUID
}

func (n *WebhookNode) Record(rt *runtime.Runtime, events []*events.WebhookCalledEvent) error {
	numHealthy, numUnhealthy := 0, 0
	for _, e := range events {
		if e.ElapsedMS <= rt.Config.WebhooksHealthyResponseLimit {
			numHealthy++
		} else {
			numUnhealthy++
		}
	}

	rc := rt.RP.Get()
	defer rc.Close()

	healthySeries, unhealthySeries := n.series()

	if numHealthy > 0 {
		if err := healthySeries.Record(rc, string(n.UUID), int64(numHealthy)); err != nil {
			return errors.Wrap(err, "error recording healthy calls")
		}
	}
	if numUnhealthy > 0 {
		if err := unhealthySeries.Record(rc, string(n.UUID), int64(numUnhealthy)); err != nil {
			return errors.Wrap(err, "error recording unhealthy calls")
		}
	}

	return nil
}

func (n *WebhookNode) Healthy(rt *runtime.Runtime) (bool, error) {
	rc := rt.RP.Get()
	defer rc.Close()

	healthySeries, unhealthySeries := n.series()
	healthy, err := healthySeries.Total(rc, string(n.UUID))
	if err != nil {
		return false, errors.Wrap(err, "error getting healthy series total")
	}
	unhealthy, err := unhealthySeries.Total(rc, string(n.UUID))
	if err != nil {
		return false, errors.Wrap(err, "error getting healthy series total")
	}

	// node is healthy if number of unhealthy calls is less than 10 or unhealthy percentage is < 25%
	return unhealthy < 10 || (100*unhealthy/(healthy+unhealthy)) < 25, nil
}

func (n *WebhookNode) series() (*redisx.IntervalSeries, *redisx.IntervalSeries) {
	return redisx.NewIntervalSeries("webhooks:healthy", time.Minute*5, 4), redisx.NewIntervalSeries("webhooks:unhealthy", time.Minute*5, 4)
}
