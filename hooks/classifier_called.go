package hooks

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHook(events.TypeClassifierCalled, handleClassifierCalled)
}

// InsertHTTPLogsHook is our hook for inserting classifier logs
type InsertHTTPLogsHook struct{}

var insertHTTPLogsHook = &InsertHTTPLogsHook{}

// Apply inserts all the classifier logs that were created
func (h *InsertHTTPLogsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, sessions map[*models.Session][]interface{}) error {
	// gather all our logs
	logs := make([]*models.HTTPLog, 0, len(sessions))
	for _, ls := range sessions {
		for _, l := range ls {
			logs = append(logs, l.(*models.HTTPLog))
		}
	}

	err := models.InsertHTTPLogs(ctx, tx, logs)
	if err != nil {
		return errors.Wrapf(err, "error inserting http logs")
	}

	return nil
}

// handleClassifierCalled is called for each classifier called event
func handleClassifierCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, session *models.Session, e flows.Event) error {
	event := e.(*events.ClassifierCalledEvent)
	logrus.WithFields(logrus.Fields{
		"contact_uuid":    session.ContactUUID(),
		"session_id":      session.ID(),
		"url":             event.URL,
		"status":          event.Status,
		"elapsed_ms":      event.ElapsedMS,
		"classifier_name": event.Classifier.Name,
		"classifier_uuid": event.Classifier.UUID,
	}).Debug("classifier called")

	// if this is a connection error, use that as our response
	response := event.Response
	if event.Status == flows.CallStatusConnectionError {
		response = "connection error"
	}

	classifier := org.ClassifierByUUID(event.Classifier.UUID)
	if classifier == nil {
		return errors.Errorf("unable to find classifier with UUID: %s", event.Classifier.UUID)
	}

	// create a log for this call
	log := models.NewClassifierCalledLog(
		org.OrgID(),
		classifier.ID(),
		event.URL,
		event.Request,
		response,
		event.Status != flows.CallStatusSuccess,
		time.Duration(event.ElapsedMS)*time.Millisecond,
		event.CreatedOn(),
	)

	session.AddPreCommitEvent(insertHTTPLogsHook, log)

	return nil
}
