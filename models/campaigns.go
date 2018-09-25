package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/utils"
)

// CampaignID is our type for campaign ids
type CampaignID int

// CampaignEventID is our type for campaign event ids
type CampaignEventID int

// CampaignUUID is our type for campaign UUIDs
type CampaignUUID utils.UUID

// CampaignEventUUID is our type for campaign event UUIDs
type CampaignEventUUID utils.UUID

// OffsetUnit defines what time unit our offset is in
type OffsetUnit string

const (
	// OffsetMinute means our offset is in minutes
	OffsetMinute = OffsetUnit("M")

	// OffsetHour means our offset is in hours
	OffsetHour = OffsetUnit("H")

	// OffsetDay means our offset is in days
	OffsetDay = OffsetUnit("D")

	// OffsetWeek means our offset is in weeks
	OffsetWeek = OffsetUnit("W")

	// NilDeliveryHour is our constant for not having a set delivery hour
	NilDeliveryHour = -1
)

// Campaign is our struct for a campaign and all its events
type Campaign struct {
	c struct {
		ID        CampaignID       `json:"id"`
		UUID      CampaignUUID     `json:"uuid"`
		Name      string           `json:"name"`
		GroupID   GroupID          `json:"group_id"`
		GroupUUID assets.GroupUUID `json:"group_uuid"`
		GroupName string           `json:"group_name"`
		Events    []*CampaignEvent `json:"events"`
	}
}

// ID return the database id of this campaign
func (c *Campaign) ID() CampaignID { return c.c.ID }

// UUID returns the UUID of this campaign
func (c *Campaign) UUID() CampaignUUID { return c.c.UUID }

// Name returns the name of this campaign
func (c *Campaign) Name() string { return c.c.Name }

// GroupID returns the id of the group this campaign works against
func (c *Campaign) GroupID() GroupID { return c.c.GroupID }

// GroupUUID returns the uuid of the group this campaign works against
func (c *Campaign) GroupUUID() assets.GroupUUID { return c.c.GroupUUID }

// Events returns the list of events for this campaign
func (c *Campaign) Events() []*CampaignEvent { return c.c.Events }

// CampaignEvent is our struct for an individual campaign event
type CampaignEvent struct {
	e struct {
		ID            CampaignEventID   `json:"id"`
		UUID          CampaignEventUUID `json:"uuid"`
		EventType     string            `json:"event_type"`
		RelativeToID  FieldID           `json:"relative_to_id"`
		RelativeToKey string            `json:"relative_to_key"`
		Offset        int               `json:"offset"`
		Unit          OffsetUnit        `json:"unit"`
		DeliveryHour  int               `json:"delivery_hour"`
		FlowID        FlowID            `json:"flow_id"`
	}

	campaign *Campaign
}

func (e *CampaignEvent) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &e.e)
}

// ScheduleForTime calculates the next fire (if any) for the passed in contact for this CampaignEvent
func (e *CampaignEvent) ScheduleForTime(tz *time.Location, now time.Time, start time.Time) (*time.Time, error) {
	// convert to our timezone
	start = start.In(tz)

	// round to next minute, floored at 0 s/ns if we aren't already at 0
	scheduled := start
	if start.Second() > 0 || start.Nanosecond() > 0 {
		scheduled = start.Add(time.Second * 59).Round(time.Minute)
	}

	// create our offset
	switch e.Unit() {
	case OffsetMinute:
		scheduled = scheduled.Add(time.Minute * time.Duration(e.Offset()))
	case OffsetHour:
		scheduled = scheduled.Add(time.Hour * time.Duration(e.Offset()))
	case OffsetDay:
		scheduled = scheduled.AddDate(0, 0, e.Offset())
	case OffsetWeek:
		scheduled = scheduled.AddDate(0, 0, e.Offset()*7)
	default:
		return nil, errors.Errorf("unknown offset unit: %s", e.Unit())
	}

	// now set our delivery hour if set
	if e.DeliveryHour() != NilDeliveryHour {
		scheduled = time.Date(scheduled.Year(), scheduled.Month(), scheduled.Day(), e.DeliveryHour(), 0, 0, 0, tz)
	}

	// if this is in the past, this is a no op
	if scheduled.Before(now) {
		return nil, nil
	}

	return &scheduled, nil
}

// ID returns the database id for this campaign event
func (e *CampaignEvent) ID() CampaignEventID { return e.e.ID }

// UUID returns the UUID of this campaign event
func (e *CampaignEvent) UUID() CampaignEventUUID { return e.e.UUID }

// RelativeToID returns the ID of the field this event is relative to
func (e *CampaignEvent) RelativeToID() FieldID { return e.e.RelativeToID }

// RelativeToKey returns the key of the field this event is relative to
func (e *CampaignEvent) RelativeToKey() string { return e.e.RelativeToKey }

// Offset returns the offset for thi campaign event
func (e *CampaignEvent) Offset() int { return e.e.Offset }

// Unit returns the unit for this campaign event
func (e *CampaignEvent) Unit() OffsetUnit { return e.e.Unit }

// DeliveryHour returns the hour this event should send at, if any
func (e *CampaignEvent) DeliveryHour() int { return e.e.DeliveryHour }

// Campaign returns the campaign this event is part of
func (e *CampaignEvent) Campaign() *Campaign { return e.campaign }

// loadCampaigns loads all the campaigns for the passed in org
func loadCampaigns(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Campaign, error) {
	rows, err := db.Queryx(selectCampaignsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying campaigns for org: %d", orgID)
	}
	defer rows.Close()

	campaigns := make([]*Campaign, 0, 2)
	for rows.Next() {
		campaign := &Campaign{}
		err := readJSONRow(rows, &campaign.c)
		if err != nil {
			return nil, errors.Annotatef(err, "error unmarshalling campaign")
		}

		campaigns = append(campaigns, campaign)
	}

	// populate the campaign pointer for each event
	for _, c := range campaigns {
		for _, e := range c.Events() {
			e.campaign = c
		}
	}

	return campaigns, nil
}

const selectCampaignsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	c.id as id,
	c.uuid as uuid,
	c.name as name,
	cc.name as group_name,
	cc.uuid as group_uuid,
	c.group_id,
	(SELECT ARRAY_AGG(evs) FROM (
		SELECT
			e.id as id,
            e.uuid as uuid,
            e.event_type as event_type,
			e.relative_to_id as relative_to_id,
			f.key as relative_to_key,
            e.offset as offset,
			e.unit as unit,
			e.delivery_hour as delivery_hour,
			e.flow_id as flow_id
		FROM 
			campaigns_campaignevent e
			JOIN contacts_contactfield f on e.relative_to_id = f.id
		WHERE 
			e.campaign_id = c.id AND
			e.is_active = TRUE AND
			f.is_active = TRUE
		ORDER BY
			e.relative_to_id,
			e.offset
    ) evs) as events
FROM 
	campaigns_campaign c
	JOIN contacts_contactgroup cc on c.group_id = cc.id
WHERE 
	c.org_id = $1 AND
	c.is_active = TRUE AND
	c.is_archived = FALSE
) r;
`

func MarkCampaignEventFired(ctx context.Context, db *sqlx.DB, fireID int, fired time.Time) error {
	_, err := db.ExecContext(ctx, markEventFired, fireID, fired)
	return err
}

const markEventFired = `
UPDATE 
	campaigns_eventfire
SET 
	fired = $2
WHERE
	id = $1
`
