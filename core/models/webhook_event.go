package models

import (
	"context"
	"time"
)

type WebhookEventID int64

// WebhookEvent represents an event that was created, mostly used for resthooks
type WebhookEvent struct {
	e struct {
		ID         WebhookEventID `db:"id"`
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

	e.Data = data
	e.OrgID = orgID
	e.ResthookID = resthookID
	e.CreatedOn = createdOn

	return event
}

const sqlInsertWebhookEvents = `
INSERT INTO api_webhookevent(data, resthook_id, org_id, created_on, action)
     VALUES(:data, :resthook_id, :org_id, :created_on, 'POST')
  RETURNING id`

// InsertWebhookEvents inserts the passed in webhook events, assigning them ids
func InsertWebhookEvents(ctx context.Context, db Queryer, events []*WebhookEvent) error {
	if len(events) == 0 {
		return nil
	}

	is := make([]interface{}, len(events))
	for i := range events {
		is[i] = &events[i].e
	}

	return BulkQuery(ctx, "inserted webhook events", db, sqlInsertWebhookEvents, is)
}
