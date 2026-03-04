package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/nyaruka/vkutil"
	"github.com/vinovest/sqlx"
)

// IncidentID is our type for incident ids
type IncidentID int64

const NilIncidentID = IncidentID(0)

func (i *IncidentID) Scan(value any) error         { return null.ScanInt(value, i) }
func (i IncidentID) Value() (driver.Value, error)  { return null.IntValue(i) }
func (i *IncidentID) UnmarshalJSON(b []byte) error { return null.UnmarshalInt(b, i) }
func (i IncidentID) MarshalJSON() ([]byte, error)  { return null.MarshalInt(i) }

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
func (i *Incident) End(ctx context.Context, db DBorTx) error {
	now := time.Now()
	i.EndedOn = &now
	_, err := db.ExecContext(ctx, `UPDATE notifications_incident SET ended_on = $2 WHERE id = $1`, i.ID, i.EndedOn)
	if err != nil {
		return fmt.Errorf("error updating incident ended_on: %w", err)
	}
	return nil
}

// IncidentWebhooksUnhealthy ensures there is an open unhealthy webhooks incident for the given org
func IncidentWebhooksUnhealthy(ctx context.Context, db DBorTx, rp *valkey.Pool, oa *OrgAssets, nodes []flows.NodeUUID) (IncidentID, error) {
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
		vc := rp.Get()
		defer vc.Close()

		nodesKey := fmt.Sprintf("incident:%d:nodes", id)
		vc.Send("MULTI")
		vc.Send("SADD", valkey.Args{}.Add(nodesKey).AddFlat(nodes)...)
		vc.Send("EXPIRE", nodesKey, 60*30) // 30 minutes
		_, err = vc.Do("EXEC")
		if err != nil {
			return NilIncidentID, fmt.Errorf("error adding node uuids to incident: %w", err)
		}
	}

	return id, nil
}

const sqlInsertIncident = `
INSERT INTO notifications_incident(org_id, incident_type, scope, started_on, channel_id) 
     VALUES($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING 
  RETURNING id`

func getOrCreateIncident(ctx context.Context, db DBorTx, oa *OrgAssets, incident *Incident) (IncidentID, error) {
	var incidentID IncidentID
	err := db.GetContext(ctx, &incidentID, sqlInsertIncident, incident.OrgID, incident.Type, incident.Scope, incident.StartedOn, incident.ChannelID)
	if err != nil && err != sql.ErrNoRows {
		return NilIncidentID, fmt.Errorf("error inserting incident: %w", err)
	}

	// if we got back an id, a new incident was actually created
	if incidentID != NilIncidentID {
		incident.ID = incidentID

		if err := NotifyIncidentStarted(ctx, db, oa, incident); err != nil {
			return NilIncidentID, fmt.Errorf("error creating notifications for new incident: %w", err)
		}
	} else {
		err := db.GetContext(ctx, &incidentID, `SELECT id FROM notifications_incident WHERE org_id = $1 AND incident_type = $2 AND scope = $3 AND ended_on IS NULL`, incident.OrgID, incident.Type, incident.Scope)
		if err != nil {
			return NilIncidentID, fmt.Errorf("error looking up existing incident: %w", err)
		}
	}

	return incidentID, nil
}

const sqlSelectOpenIncidents = `
SELECT id, org_id, incident_type, scope, started_on, ended_on, channel_id
  FROM notifications_incident
 WHERE ended_on IS NULL AND incident_type = ANY($1)`

func GetOpenIncidents(ctx context.Context, db *sqlx.DB, types []IncidentType) ([]*Incident, error) {
	rows, err := db.QueryxContext(ctx, sqlSelectOpenIncidents, pq.Array(types))
	if err != nil {
		return nil, fmt.Errorf("error querying open incidents: %w", err)
	}
	defer rows.Close()

	incidents := make([]*Incident, 0, 10)
	for rows.Next() {
		obj := &Incident{}
		err := rows.StructScan(obj)
		if err != nil {
			return nil, fmt.Errorf("error scanning incident: %w", err)
		}

		incidents = append(incidents, obj)
	}

	return incidents, nil
}

// WebhookNode is a utility to help determine the health of an individual webhook node
type WebhookNode struct {
	UUID flows.NodeUUID
}

func (n *WebhookNode) Record(ctx context.Context, rt *runtime.Runtime, events []*events.WebhookCalled) error {
	numHealthy, numUnhealthy := 0, 0
	for _, e := range events {
		if e.ElapsedMS <= rt.Config.WebhooksHealthyResponseLimit {
			numHealthy++
		} else {
			numUnhealthy++
		}
	}

	vc := rt.VK.Get()
	defer vc.Close()

	healthySeries, unhealthySeries := n.series()

	if numHealthy > 0 {
		if err := healthySeries.Record(ctx, vc, string(n.UUID), int64(numHealthy)); err != nil {
			return fmt.Errorf("error recording healthy calls: %w", err)
		}
	}
	if numUnhealthy > 0 {
		if err := unhealthySeries.Record(ctx, vc, string(n.UUID), int64(numUnhealthy)); err != nil {
			return fmt.Errorf("error recording unhealthy calls: %w", err)
		}
	}

	return nil
}

func (n *WebhookNode) Healthy(ctx context.Context, rt *runtime.Runtime) (bool, error) {
	vc := rt.VK.Get()
	defer vc.Close()

	healthySeries, unhealthySeries := n.series()
	healthy, err := healthySeries.Total(ctx, vc, string(n.UUID))
	if err != nil {
		return false, fmt.Errorf("error getting healthy series total: %w", err)
	}
	unhealthy, err := unhealthySeries.Total(ctx, vc, string(n.UUID))
	if err != nil {
		return false, fmt.Errorf("error getting healthy series total: %w", err)
	}

	// node is healthy if number of unhealthy calls is less than 10 or unhealthy percentage is < 25%
	return unhealthy < 10 || (100*unhealthy/(healthy+unhealthy)) < 25, nil
}

func (n *WebhookNode) series() (*vkutil.IntervalSeries, *vkutil.IntervalSeries) {
	return vkutil.NewIntervalSeries("webhooks:healthy", time.Minute*5, 4), vkutil.NewIntervalSeries("webhooks:unhealthy", time.Minute*5, 4)
}
