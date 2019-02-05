package simulation

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/legacy"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/flow/migrate", web.RequireAuthToken(handleMigrate))
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/start", web.RequireAuthToken(handleStart))
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/resume", web.RequireAuthToken(handleResume))
}

var (
	httpClient = utils.NewHTTPClient("mailroom")
)

type flowDefinition struct {
	UUID             assets.FlowUUID `json:"uuid"                validate:"required"`
	Definition       json.RawMessage `json:"definition"`
	LegacyDefinition json.RawMessage `json:"legacy_definition"`
}

type sessionRequest struct {
	OrgID models.OrgID     `json:"org_id"  validate:"required"`
	Flows []flowDefinition `json:"flows"`
}

type sessionResponse struct {
	Session flows.Session `json:"session"`
	Events  []flows.Event `json:"events"`
}

// Starts a new engine session
//
//   {
//     "org_id": 1,
//     "flows": [{
//	      "uuid": uuidv4,
//        "definition": "goflow definition",
//        "legacy_definition": "legacy definition",
//     },.. ],
//     "trigger": {...}
//   }
//
type startRequest struct {
	sessionRequest
	Trigger json.RawMessage `json:"trigger" validate:"required"`
}

// handles a request to /start
func handleStart(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &startRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "request failed validation")
	}

	// grab our org
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load org assets")
	}

	// for each of our passed in definitions
	for _, flow := range request.Flows {
		// populate our flow in our org from our request
		err = populateFlow(org, flow.UUID, flow.Definition, flow.LegacyDefinition)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
	}

	// build our session
	sa, err := models.NewSessionAssets(org)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable get session assets")
	}

	session := engine.NewSession(sa, engine.NewDefaultConfig(), httpClient)

	// read our trigger
	trigger, err := triggers.ReadTrigger(sa, request.Trigger, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to read trigger")
	}

	// start our flow
	start := time.Now()
	sprint, err := session.Start(trigger)
	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", request.OrgID).Debug("start simulation complete")
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting session")
	}

	return &sessionResponse{Session: session, Events: sprint.Events()}, http.StatusOK, nil
}

// Resumes an existing engine session
//
//   {
//	   "org_id": 1,
//     "flows": [{
//	      "uuid": uuidv4,
//        "definition": "goflow definition",
//        "legacy_definition": "legacy definition",
//     },.. ],
//     "session": {"uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "runs": [...], ...},
//     "resume": {...}
//   }
//
type resumeRequest struct {
	sessionRequest

	Session json.RawMessage `json:"session" validate:"required"`
	Resume  json.RawMessage `json:"resume" validate:"required"`
}

func handleResume(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &resumeRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, err
	}

	// grab our org
	org, err := models.NewOrgAssets(s.CTX, s.DB, request.OrgID, nil)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// for each of our passed in definitions
	for _, flow := range request.Flows {
		// populate our flow in our org from our request
		err = populateFlow(org, flow.UUID, flow.Definition, flow.LegacyDefinition)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
	}

	// build our session
	sa, err := models.NewSessionAssets(org)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	session, err := engine.ReadSession(sa, engine.NewDefaultConfig(), httpClient, request.Session, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// read our resume
	resume, err := resumes.ReadResume(sa, request.Resume, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// resume our session
	sprint, err := session.Resume(resume)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return &sessionResponse{Session: session, Events: sprint.Events()}, http.StatusOK, nil
}

// populateFlow takes care of setting the definition for the flow with the passed in UUID according to the passed in definitions
func populateFlow(org *models.OrgAssets, uuid assets.FlowUUID, flowDef json.RawMessage, legacyFlowDef json.RawMessage) error {
	f, err := org.Flow(uuid)
	if err != nil {
		return errors.Wrapf(err, "unable to find flow with uuid: %s", uuid)
	}

	flow := f.(*models.Flow)
	if flowDef != nil {
		flow.SetDefinition(flowDef)
		return nil
	}

	if legacyFlowDef != nil {
		err = flow.SetLegacyDefinition(legacyFlowDef)
		if err != nil {
			return errors.Wrapf(err, "unable to populate flow: %s invalid definition", uuid)
		}
		return nil
	}

	return errors.Errorf("missing definition or legacy_definition for flow: %s", uuid)
}

// Migrates a legacy flow to the new flow definition specification
//
//   {
//     "flow": {"uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "action_sets": [], ...},
//     "include_ui": false
//   }
//
type migrateRequest struct {
	Flow          json.RawMessage `json:"flow"`
	CollapseExits *bool           `json:"collapse_exits"`
	IncludeUI     *bool           `json:"include_ui"`
}

func handleMigrate(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	migrate := migrateRequest{}
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	if err := r.Body.Close(); err != nil {
		return nil, http.StatusInternalServerError, err
	}

	if err := json.Unmarshal(body, &migrate); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error unmarshalling definition")
	}

	if migrate.Flow == nil {
		return nil, http.StatusBadRequest, errors.Errorf("missing flow element")
	}

	legacyFlow, err := legacy.ReadLegacyFlow(migrate.Flow)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error reading legacy flow")
	}

	collapseExits := migrate.CollapseExits == nil || *migrate.CollapseExits
	includeUI := migrate.IncludeUI == nil || *migrate.IncludeUI

	flow, err := legacyFlow.Migrate(collapseExits, includeUI)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error migrating legacy flow")
	}

	return flow, http.StatusOK, nil
}
