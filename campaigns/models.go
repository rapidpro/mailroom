package campaigns

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/models"
)

// EventFire represents a single campaign event fire for an event and contact
type EventFire struct {
	FireID       int               `db:"fire_id"`
	Scheduled    time.Time         `db:"scheduled"`
	Fired        *time.Time        `db:"fired"`
	ContactID    models.ContactID  `db:"contact_id"`
	ContactUUID  flows.ContactUUID `db:"contact_uuid"`
	EventID      int64             `db:"event_id"`
	EventUUID    string            `db:"event_uuid"`
	CampaignID   int64             `db:"campaign_id"`
	CampaignUUID string            `db:"campaign_uuid"`
	CampaignName string            `db:"campaign_name"`
	OrgID        models.OrgID      `db:"org_id"`
	FlowUUID     flows.FlowUUID    `db:"flow_uuid"`
}

const loadEventFireSQL = `
SELECT 
	ef.id as fire_id, 
	ef.scheduled as scheduled, 
	ef.fired as fired, 
	c.id as contact_id, 
	c.uuid as contact_uuid,
	ce.id as event_id,
	ce.uuid as event_uuid,
	ca.id as campaign_id,
	ca.uuid as campaign_uuid,
	ca.name as campaign_name,
	ca.org_id as org_id,
	f.uuid as flow_uuid
FROM 
	campaigns_eventfire ef,
	campaigns_campaignevent ce,
	campaigns_campaign ca,
	flows_flow f,
	contacts_contact c
WHERE 
	ef.id = $1 AND
	ef.contact_id = c.id AND
	ce.id = ef.event_id AND 
	ca.id = ce.campaign_id AND
	f.id = ce.flow_id
`

func loadEventFire(ctx context.Context, db *sqlx.DB, id int64) (*EventFire, error) {
	fire := EventFire{}
	ctx, cancel := context.WithTimeout(ctx, time.Second*15)
	defer cancel()

	err := db.GetContext(ctx, &fire, loadEventFireSQL, id)
	return &fire, err
}
