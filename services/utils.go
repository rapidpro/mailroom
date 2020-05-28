package services

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

// LoggingJSONHandler is a JSON web handler which logs HTTP logs
type LoggingJSONHandler func(ctx context.Context, s *web.Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error)

// WithHTTPLogs wraps a handler to create a handler which can record and save HTTP logs
func WithHTTPLogs(handler LoggingJSONHandler) web.JSONHandler {
	return func(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
		logger := &models.HTTPLogger{}

		response, status, err := handler(ctx, s, r, logger)

		if err := logger.Insert(ctx, s.DB); err != nil {
			return nil, http.StatusInternalServerError, errors.Wrap(err, "error writing HTTP logs")
		}

		return response, status, err
	}
}
