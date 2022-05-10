package testdata

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/core/models"
)

type Campaign struct {
	ID   models.CampaignID
	UUID models.CampaignUUID
}

type CampaignEvent struct {
	ID   models.CampaignEventID
	UUID models.CampaignEventUUID
}

func InsertCampaign(db *sqlx.DB, org *Org, name string, group *Group) *Campaign {
	uuid := models.CampaignUUID(uuids.New())
	var id models.CampaignID
	must(db.Get(&id,
		`INSERT INTO campaigns_campaign(uuid, org_id, name, group_id, is_archived, is_system, is_active, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, $2, $3, $4, FALSE, FALSE, TRUE, NOW(), NOW(), 1, 1) RETURNING id`, uuid, org.ID, name, group.ID,
	))
	return &Campaign{id, uuid}
}

func InsertCampaignFlowEvent(db *sqlx.DB, campaign *Campaign, flow *Flow, relativeTo *Field, offset int, unit string) *CampaignEvent {
	uuid := models.CampaignEventUUID(uuids.New())
	var id models.CampaignEventID
	must(db.Get(&id,
		`INSERT INTO campaigns_campaignevent(
			uuid, campaign_id, event_type, flow_id, relative_to_id, "offset", unit, delivery_hour, start_mode,
			is_active, created_on, modified_on, created_by_id, modified_by_id
		) VALUES(
			$1, $2, 'F', $3, $4, $5, $6, -1, 'I',
			TRUE, NOW(), NOW(), 1, 1
		) RETURNING id`,
		uuid, campaign.ID, flow.ID, relativeTo.ID, offset, unit,
	))
	return &CampaignEvent{id, uuid}
}

func InsertEventFire(db *sqlx.DB, contact *Contact, event *CampaignEvent, scheduled time.Time) models.FireID {
	var id models.FireID
	must(db.Get(&id,
		`INSERT INTO campaigns_eventfire(contact_id, event_id, scheduled) VALUES ($1, $2, $3) RETURNING id;`, contact.ID, event.ID, scheduled,
	))
	return id
}
