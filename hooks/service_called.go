package hooks

import (
	"context"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	models.RegisterEventHandler(events.TypeServiceCalled, handleServiceCalled)
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

// handleServiceCalled is called for each service called event
func handleServiceCalled(ctx context.Context, tx *sqlx.Tx, rp *redis.Pool, org *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ServiceCalledEvent)
	var classifier *models.Classifier
	var ticketer *models.Ticketer

	if event.Service == "classifier" {
		classifier = org.ClassifierByUUID(event.Classifier.UUID)
		if classifier == nil {
			return errors.Errorf("unable to find classifier with UUID: %s", event.Classifier.UUID)
		}
	} else if event.Service == "ticketer" {
		ticketer = org.TicketerByUUID(event.Ticketer.UUID)
		if ticketer == nil {
			return errors.Errorf("unable to find ticketer with UUID: %s", event.Ticketer.UUID)
		}
	}

	// create a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		logrus.WithFields(logrus.Fields{
			"contact_uuid": scene.ContactUUID(),
			"session_id":   scene.SessionID(),
			"url":          httpLog.URL,
			"status":       httpLog.Status,
			"elapsed_ms":   httpLog.ElapsedMS,
		}).Debug("service called")

		var log *models.HTTPLog

		if event.Service == "classifier" {
			log = models.NewClassifierCalledLog(
				org.OrgID(),
				classifier.ID(),
				httpLog.URL,
				httpLog.Request,
				httpLog.Response,
				httpLog.Status != flows.CallStatusSuccess,
				time.Duration(httpLog.ElapsedMS)*time.Millisecond,
				httpLog.CreatedOn,
			)
		} else if event.Service == "ticketer" {
			log = models.NewTicketerCalledLog(
				org.OrgID(),
				ticketer.ID(),
				httpLog.URL,
				httpLog.Request,
				httpLog.Response,
				httpLog.Status != flows.CallStatusSuccess,
				time.Duration(httpLog.ElapsedMS)*time.Millisecond,
				httpLog.CreatedOn,
			)
		}

		scene.AppendToEventPreCommitHook(insertHTTPLogsHook, log)
	}

	return nil
}
