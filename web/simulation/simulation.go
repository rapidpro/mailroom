package simulation

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/legacy"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/goflow"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/flow/migrate", web.RequireAuthToken(handleMigrate))
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/start", web.RequireAuthToken(handleStart))
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/resume", web.RequireAuthToken(handleResume))
}

type flowDefinition struct {
	UUID             assets.FlowUUID `json:"uuid"                validate:"required"`
	Definition       json.RawMessage `json:"definition"`
	LegacyDefinition json.RawMessage `json:"legacy_definition"`
}

type sessionRequest struct {
	OrgID  models.OrgID     `json:"org_id"  validate:"required"`
	Flows  []flowDefinition `json:"flows"`
	Assets struct {
		Channels []*types.Channel `json:"channels"`
	} `json:"assets"`
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
//        "uuid": uuidv4,
//        "definition": "goflow definition",
//        "legacy_definition": "legacy definition",
//     },.. ],
//     "trigger": {...},
//     "assets": {...}
//   }
//
type startRequest struct {
	sessionRequest
	Trigger json.RawMessage `json:"trigger" validate:"required"`
}

// handleSimulationEvents takes care of updating our db with any events needed during simulation
func handleSimulationEvents(ctx context.Context, db models.Queryer, org *models.OrgAssets, es []flows.Event) error {
	// nicpottier: this could be refactored into something more similar to how we handle normal events (ie hooks) if
	// we see ourselves taking actions for more than just webhook events
	wes := make([]*models.WebhookEvent, 0)
	for _, e := range es {
		if e.Type() == events.TypeResthookCalled {
			rec := e.(*events.ResthookCalledEvent)
			resthook := org.ResthookBySlug(rec.Resthook)
			if resthook != nil {
				we := models.NewWebhookEvent(org.OrgID(), resthook.ID(), string(rec.Payload), rec.CreatedOn())
				wes = append(wes, we)
			}
		}
	}

	// noop in the case of no events
	return models.InsertWebhookEvents(ctx, db, wes)
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

	// populate any test channels
	for _, channel := range request.Assets.Channels {
		org.AddTestChannel(channel)
	}

	// build our session
	sa, err := models.NewSessionAssets(org)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable get session assets")
	}

	// read our trigger
	trigger, err := triggers.ReadTrigger(sa, request.Trigger, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to read trigger")
	}

	return triggerFlow(ctx, s.DB, org, sa, trigger)
}

// triggerFlow creates a new session with the passed in trigger, returning our standard response
func triggerFlow(ctx context.Context, db *sqlx.DB, org *models.OrgAssets, sa flows.SessionAssets, trigger flows.Trigger) (interface{}, int, error) {
	session := goflow.Engine().NewSession(sa)

	// start our flow
	sprint, err := session.Start(trigger)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting session")
	}

	err = handleSimulationEvents(ctx, db, org, sprint.Events())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error handling simulation events")
	}

	return &sessionResponse{Session: session, Events: sprint.Events()}, http.StatusOK, nil
}

// Resumes an existing engine session
//
//   {
//     "org_id": 1,
//     "flows": [{
//        "uuid": uuidv4,
//        "definition": "goflow definition",
//        "legacy_definition": "legacy definition",
//     },.. ],
//     "session": {"uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "runs": [...], ...},
//     "resume": {...},
//     "assets": {...}
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

	// populate any test channels
	for _, channel := range request.Assets.Channels {
		org.AddTestChannel(channel)
	}

	// build our session
	sa, err := models.NewSessionAssets(org)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	session, err := goflow.Engine().ReadSession(sa, request.Session, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// read our resume
	resume, err := resumes.ReadResume(sa, request.Resume, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// if this is a msg resume we want to check whether it might be caught by a trigger
	if resume.Type() == resumes.TypeMsg {
		msgResume := resume.(*resumes.MsgResume)
		trigger := models.FindMatchingMsgTrigger(org, msgResume.Contact(), msgResume.Msg().Text())
		if trigger != nil {
			var flow *models.Flow
			for _, r := range session.Runs() {
				if r.Status() == flows.RunStatusWaiting {
					f, _ := org.Flow(r.Flow().UUID())
					if f != nil {
						flow = f.(*models.Flow)
					}
					break
				}
			}

			// we don't have a current flow or the current flow doesn't ignore triggers
			if flow == nil || (!flow.IgnoreTriggers() && trigger.TriggerType() == models.KeywordTriggerType) {
				triggeredFlow, err := org.FlowByID(trigger.FlowID())
				if err != nil {
					return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load triggered flow")
				}

				if triggeredFlow != nil && !triggeredFlow.IsArchived() {
					trigger := triggers.NewMsgTrigger(org.Env(), triggeredFlow.FlowReference(), resume.Contact(), msgResume.Msg(), trigger.Match())
					return triggerFlow(ctx, s.DB, org, sa, trigger)
				}
			}
		}
	}

	// resume our session
	sprint, err := session.Resume(resume)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	err = handleSimulationEvents(ctx, s.DB, org, sprint.Events())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error handling simulation events")
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
