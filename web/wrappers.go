package web

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/pkg/errors"
)

// RequireUserToken wraps a JSON handler to require passing of an API token via the authorization header
func RequireUserToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
		token := r.Header.Get("authorization")
		if !strings.HasPrefix(token, "Token ") {
			return errors.New("missing authorization header"), http.StatusUnauthorized, nil
		}

		// pull out the actual token
		token = token[6:]

		// try to look it up
		rows, err := s.DB.QueryContext(s.CTX, `
		SELECT 
			user_id, 
			org_id
		FROM
			api_apitoken t
			JOIN orgs_org o ON t.org_id = o.id
			JOIN auth_group g ON t.role_id = g.id
			JOIN auth_user u ON t.user_id = u.id
		WHERE
			key = $1 AND
			g.name IN ('Administrators', 'Editors', 'Surveyors') AND
			t.is_active = TRUE AND
			o.is_active = TRUE AND
			u.is_active = TRUE
		`, token)
		if err != nil {
			return errors.Wrapf(err, "error looking up authorization header"), http.StatusUnauthorized, nil
		}
		defer rows.Close()

		if !rows.Next() {
			return errors.Errorf("invalid authorization header"), http.StatusUnauthorized, nil
		}

		var userID int64
		var orgID models.OrgID
		err = rows.Scan(&userID, &orgID)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error scanning auth row")
		}

		// we are authenticated set our user id ang org id on our context and call our sub handler
		ctx = context.WithValue(ctx, UserIDKey, userID)
		ctx = context.WithValue(ctx, OrgIDKey, orgID)
		return handler(ctx, s, r)
	}
}

// RequireAuthToken wraps a handler to require that our request to have our global authorization header
func RequireAuthToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
		auth := r.Header.Get("authorization")
		if s.Config.AuthToken != "" && fmt.Sprintf("Token %s", s.Config.AuthToken) != auth {
			return fmt.Errorf("invalid or missing authorization header, denying"), http.StatusUnauthorized, nil
		}

		// we are authenticated, call our chain
		return handler(ctx, s, r)
	}
}

// LoggingJSONHandler is a JSON web handler which logs HTTP logs
type LoggingJSONHandler func(ctx context.Context, s *Server, r *http.Request, l *models.HTTPLogger) (interface{}, int, error)

// WithHTTPLogs wraps a handler to create a handler which can record and save HTTP logs
func WithHTTPLogs(handler LoggingJSONHandler) JSONHandler {
	return func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
		logger := &models.HTTPLogger{}

		response, status, err := handler(ctx, s, r, logger)

		if err := logger.Insert(ctx, s.DB); err != nil {
			return nil, http.StatusInternalServerError, errors.Wrap(err, "error writing HTTP logs")
		}

		return response, status, err
	}
}
