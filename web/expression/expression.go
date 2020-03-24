package expression

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/flows/definition/legacy/expressions"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/expression/migrate", web.RequireAuthToken(handleMigrate))
}

// Migrates a legacy expression to the new flow definition specification
//
//   {
//     "expression": "@contact.age"
//   }
//
type migrateRequest struct {
	Expression string `json:"expression" validate:"required"`
}

type migrateResponse struct {
	Migrated string `json:"migrated"`
}

func handleMigrate(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &migrateRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	migrated, err := expressions.MigrateTemplate(request.Expression, nil)
	if err != nil {
		return errors.Wrapf(err, "unable to migrate expression"), http.StatusUnprocessableEntity, nil
	}

	return &migrateResponse{migrated}, http.StatusOK, nil
}
