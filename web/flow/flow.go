package flow

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/legacy"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/flow/migrate", web.RequireAuthToken(handleMigrate))
	web.RegisterJSONRoute(http.MethodPost, "/mr/flow/validate", web.RequireAuthToken(handleValidate))
}

// Migrates a legacy flow to the new flow definition specification
//
//   {
//     "flow": {"uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "action_sets": [], ...},
//     "include_ui": false
//   }
//
type migrateRequest struct {
	Flow      json.RawMessage `json:"flow"            validate:"required"`
	IncludeUI *bool           `json:"include_ui"`
}

func handleMigrate(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &migrateRequest{}
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, web.MaxRequestBytes))
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if err := r.Body.Close(); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	if err := utils.UnmarshalAndValidate(body, request); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error unmarshalling request")
	}

	legacyFlow, err := legacy.ReadLegacyFlow(request.Flow)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error reading legacy flow")
	}

	includeUI := request.IncludeUI == nil || *request.IncludeUI

	flow, err := legacyFlow.Migrate(includeUI, "https://"+config.Mailroom.AttachmentDomain)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error migrating legacy flow")
	}

	return flow, http.StatusOK, nil
}

// Validates a flow. If validation fails, we return the error. If it succeeds, we return
// the valid definition which will now include extracted dependencies. The provided flow
// definition can be in either legacy or new format, but the returned definition will
// always be in the new format. `org_id` is optional and determines whether we load and
// pass assets to the flow validation to find missing assets.
//
// Note that a invalid request to this endpoint will return a 400 status code, but that a
// valid request with a flow that fails validation will return a 422 status code.
//
//   {
//     "org_id": 1,
//     "flow": { "uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "nodes": [...]}
//   }
//
type validateRequest struct {
	OrgID models.OrgID    `json:"org_id"`
	Flow  json.RawMessage `json:"flow"   validate:"required"`
}

func handleValidate(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &validateRequest{}
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, web.MaxRequestBytes))
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if err := r.Body.Close(); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	if err := utils.UnmarshalAndValidate(body, request); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error unmarshalling request")
	}

	var flowDef = request.Flow
	var sa flows.SessionAssets

	// migrate definition if it is in legacy format
	if legacy.IsLegacyDefinition(flowDef) {
		flowDef, err = legacy.MigrateLegacyDefinition(flowDef, "https://"+config.Mailroom.AttachmentDomain)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
	}

	// try to read the flow definition which will fail if it's invalid
	flow, err := definition.ReadFlow(flowDef)
	if err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}

	// if we have an org ID, build a session assets for it
	if request.OrgID != models.NilOrgID {
		org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}

		sa, err = models.NewSessionAssets(org)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable get session assets")
		}
	}

	// inspect the flow to get dependecies, results etc
	if err := flow.Inspect(sa); err != nil {
		return nil, http.StatusUnprocessableEntity, err
	}

	return flow, http.StatusOK, nil
}
