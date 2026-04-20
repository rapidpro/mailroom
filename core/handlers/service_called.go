package handlers

import (
	"context"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/core/hooks"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

func init() {
	models.RegisterEventHandler(events.TypeServiceCalled, handleServiceCalled)
}

// handleServiceCalled is called for each service called event
func handleServiceCalled(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scene *models.Scene, e flows.Event) error {
	event := e.(*events.ServiceCalledEvent)

	slog.Debug("service called", "contact", scene.ContactUUID(), "session", scene.SessionID(), "service", event.Service)

	var classifier *models.Classifier

	if event.Service == "classifier" {
		classifier = oa.ClassifierByUUID(event.Classifier.UUID)
		if classifier == nil {
			return errors.Errorf("unable to find classifier with UUID: %s", event.Classifier.UUID)
		}
	}

	// create a log for each HTTP call
	for _, httpLog := range event.HTTPLogs {
		slog.Debug("http requested", "contact", scene.ContactUUID(), "session", scene.SessionID(), "url", httpLog.URL, "status", httpLog.Status, "elapsed_ms", httpLog.ElapsedMS)

		var log *models.HTTPLog

		if event.Service == "classifier" {
			log = models.NewClassifierCalledLog(
				oa.OrgID(),
				classifier.ID(),
				httpLog.URL,
				httpLog.StatusCode,
				httpLog.Request,
				httpLog.Response,
				httpLog.Status != flows.CallStatusSuccess,
				time.Duration(httpLog.ElapsedMS)*time.Millisecond,
				httpLog.Retries,
				httpLog.CreatedOn,
			)
		}
		scene.AppendToEventPreCommitHook(hooks.InsertHTTPLogsHook, log)
	}

	return nil
}
