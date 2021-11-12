package models

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/null"
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
	IncidentTypeFlowWebhooks IncidentType = "flow:webhooks"
)

type Incident struct {
	ID        IncidentID   `db:"id"`
	OrgID     OrgID        `db:"org_id"`
	Type      IncidentType `db:"incident_type"`
	Scope     string       `db:"scope"`
	StartedOn time.Time    `db:"started_on"`
	EndedOn   *time.Time   `db:"ended_on"`

	FlowID FlowID `db:"flow_id"`
}

func GetOrCreateFlowWebhooksIncident(ctx context.Context, db Queryer, oa *OrgAssets, flow *Flow) error {
	return getOrCreateIncident(ctx, db, oa, &Incident{
		OrgID:     oa.OrgID(),
		Type:      IncidentTypeFlowWebhooks,
		StartedOn: dates.Now(),
		Scope:     string(flow.UUID()),
		FlowID:    flow.ID(),
	})
}

const insertIncidentSQL = `
INSERT INTO notifications_incident(org_id, incident_type, scope, started_on) VALUES($1, $2, $3, $4)
ON CONFLICT DO NOTHING RETURNING id`

func getOrCreateIncident(ctx context.Context, db Queryer, oa *OrgAssets, incident *Incident) error {
	var incidentID IncidentID
	err := db.GetContext(ctx, &incidentID, insertIncidentSQL, incident.OrgID, incident.Type, incident.Scope, incident.StartedOn)
	if err != nil {
		return errors.Wrap(err, "error inserting incident")
	}

	// if we got back an id, a new incident was actually created
	if incidentID != NilIncidentID {
		incident.ID = incidentID

		if err := NotifyIncidentStarted(ctx, db, oa, incident); err != nil {
			return errors.Wrap(err, "error creating notifications for new incident")
		}
	}

	return nil
}
