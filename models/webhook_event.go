package models

import (
	"context"
	"time"
)

type WebhookEventID int64

type EventType string

type EventStatus string

const (
	EventTypeFlow = EventType("flow")

	EventStatusComplete = EventStatus("C")
)

// WebhookEvent represents an event that was created, mostly used for resthooks
type WebhookEvent struct {
	e struct {
		ID         WebhookEventID `db:"id"`
		EventType  EventType      `db:"event"`
		Status     EventStatus    `db:"status"`
		Data       string         `db:"data"`
		ResthookID ResthookID     `db:"resthook_id"`
		OrgID      OrgID          `db:"org_id"`
		CreatedOn  time.Time      `db:"created_on"`
	}
}

func (e *WebhookEvent) ID() WebhookEventID { return e.e.ID }

// NewWebhookEvent creates a new webhook event
func NewWebhookEvent(orgID OrgID, resthookID ResthookID, data string, createdOn time.Time) *WebhookEvent {
	event := &WebhookEvent{}
	e := &event.e

	e.EventType = EventTypeFlow
	e.Status = EventStatusComplete
	e.Data = data
	e.OrgID = orgID
	e.ResthookID = resthookID
	e.CreatedOn = createdOn

	return event
}

const insertWebhookEventsSQL = `
INSERT INTO	api_webhookevent( event,  status,  data,  resthook_id,  org_id,  created_on, try_count, action)
					  VALUES(:event, :status, :data, :resthook_id, :org_id, :created_on, 1,         'POST')
RETURNING id
`

// InsertWebhookEvents inserts the passed in webhook events, assigning them ids
func InsertWebhookEvents(ctx context.Context, db Queryer, events []*WebhookEvent) error {
	is := make([]interface{}, len(events))
	for i := range events {
		is[i] = &events[i].e
	}

	return BulkSQL(ctx, "inserted webhook events", db, insertWebhookEventsSQL, is)
}
