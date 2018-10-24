package web

import (
	"encoding/json"
	"net/http"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

const (
	maxRequestBytes int64 = 1048576
)

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
	LegacyFlow json.RawMessage `json:"legacy_flow"`
	Flow       json.RawMessage `json:"flow"`
	Trigger    json.RawMessage `json:"trigger" validate:"required"`
}

// handles a request to /start
func (s *Server) handleStart(r *http.Request) (interface{}, error) {
	request := &startRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, maxRequestBytes); err != nil {
		return nil, errors.Wrapf(err, "request failed validation")
	}

	// grab our org
	org, err := models.GetOrgAssets(s.ctx, s.db, request.OrgID)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load org assets")
	}

	// for each of our passed in definitions
	for _, flow := range request.Flows {
		// populate our flow in our org from our request
		err = populateFlow(org, flow.UUID, flow.Definition, flow.LegacyDefinition)
		if err != nil {
			return nil, err
		}
	}

	// build our session
	assets, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, errors.Wrapf(err, "unable get session assets")
	}

	session := engine.NewSession(assets, engine.NewDefaultConfig(), httpClient)

	// read our trigger
	trigger, err := triggers.ReadTrigger(session, request.Trigger)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read trigger")
	}

	// start our flow
	newEvents, err := session.Start(trigger)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting session")
	}

	return &sessionResponse{Session: session, Events: newEvents}, nil
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

func (s *Server) handleResume(r *http.Request) (interface{}, error) {
	request := &resumeRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, maxRequestBytes); err != nil {
		return nil, err
	}

	// grab our org
	org, err := models.GetOrgAssets(s.ctx, s.db, request.OrgID)
	if err != nil {
		return nil, err
	}

	// for each of our passed in definitions
	for _, flow := range request.Flows {
		// populate our flow in our org from our request
		err = populateFlow(org, flow.UUID, flow.Definition, flow.LegacyDefinition)
		if err != nil {
			return nil, err
		}
	}

	// build our session
	assets, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, err
	}

	session, err := engine.ReadSession(assets, engine.NewDefaultConfig(), httpClient, request.Session)
	if err != nil {
		return nil, err
	}

	// read our resume
	resume, err := resumes.ReadResume(session, request.Resume)
	if err != nil {
		return nil, err
	}

	// resume our session
	newEvents, err := session.Resume(resume)
	if err != nil {
		return nil, err
	}

	return &sessionResponse{Session: session, Events: newEvents}, nil
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

func (s *Server) handleIndex(r *http.Request) (interface{}, error) {
	response := map[string]string{
		"component": "mailroom",
		"version":   s.config.Version,
	}
	return response, nil
}

func (s *Server) handle404(r *http.Request) (interface{}, error) {
	return nil, errors.Errorf("not found: %s", r.URL.String())
}

func (s *Server) handle405(r *http.Request) (interface{}, error) {
	return nil, errors.Errorf("illegal method: %s", r.Method)
}
