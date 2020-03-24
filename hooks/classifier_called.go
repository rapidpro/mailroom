package hooks

import (
	"context"
	"time"

	"github.com/greatnonprofits-nfp/goflow/flows"
	"github.com/greatnonprofits-nfp/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeClassifierCalled, handleClassifierCalled)
}

// InsertHTTPLogsHook is our hook for inserting classifier logs
type InsertHTTPLogsHook struct{}

var insertHTTPLogsHook = &InsertHTTPLogsHook{}

// Apply inserts all the classifier logs that were created
func (h *InsertHTTPLogsHook) Apply(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scenes map[*models.Scene][]interface{}) error {
	// gather all our logs
	logs := make([]*models.HTTPLog, 0, len(scenes))
	for _, ls := range scenes {
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
func handleClassifierCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ClassifierCalledEvent)

	classifier := org.ClassifierByUUID(event.Classifier.UUID)
	if classifier == nil {
		return errors.Errorf("unable to find classifier with UUID: %s", event.Classifier.UUID)
	}

	// create a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		logrus.WithFields(logrus.Fields{
			"contact_uuid":    scene.ContactUUID(),
			"session_id":      scene.SessionID(),
			"url":             httpLog.URL,
			"status":          httpLog.Status,
			"elapsed_ms":      httpLog.ElapsedMS,
			"classifier_name": event.Classifier.Name,
			"classifier_uuid": event.Classifier.UUID,
		}).Debug("classifier called")

		log := models.NewClassifierCalledLog(
			org.OrgID(),
			classifier.ID(),
			httpLog.URL,
			httpLog.Request,
			httpLog.Response,
			httpLog.Status != flows.CallStatusSuccess,
			time.Duration(httpLog.ElapsedMS)*time.Millisecond,
			httpLog.CreatedOn,
		)

		scene.AppendToEventPreCommitHook(insertHTTPLogsHook, log)
	}

	return nil
}
