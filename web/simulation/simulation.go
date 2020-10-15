package simulation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static/types"
	"github.com/nyaruka/goflow/excellent/tools"
	xtypes "github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/start", web.RequireAuthToken(handleStart))
	web.RegisterJSONRoute(http.MethodPost, "/mr/sim/resume", web.RequireAuthToken(handleResume))
}

type flowDefinition struct {
	UUID       assets.FlowUUID `json:"uuid"       validate:"required"`
	Definition json.RawMessage `json:"definition" validate:"required"`
}

type sessionRequest struct {
	OrgID  models.OrgID     `json:"org_id"  validate:"required"`
	Flows  []flowDefinition `json:"flows"`
	Assets struct {
		Channels []*types.Channel `json:"channels"`
	} `json:"assets"`
}

func (r *sessionRequest) flows() map[assets.FlowUUID]json.RawMessage {
	flows := make(map[assets.FlowUUID]json.RawMessage, len(r.Flows))
	for _, fd := range r.Flows {
		flows[fd.UUID] = fd.Definition
	}
	return flows
}

func (r *sessionRequest) channels() []assets.Channel {
	chs := make([]assets.Channel, len(r.Assets.Channels))
	for i := range r.Assets.Channels {
		chs[i] = r.Assets.Channels[i]
	}
	return chs
}

type simulationResponse struct {
	Session flows.Session   `json:"session"`
	Events  []flows.Event   `json:"events"`
	Context *xtypes.XObject `json:"context,omitempty"`
}

func newSimulationResponse(session flows.Session, sprint flows.Sprint) *simulationResponse {
	var context *xtypes.XObject
	if session != nil {
		context = session.CurrentContext()

		// include object defaults which are not marshaled by default
		if context != nil {
			tools.ContextWalkObjects(context, func(o *xtypes.XObject) {
				o.SetMarshalDefault(true)
			})
		}
	}
	return &simulationResponse{Session: session, Events: sprint.Events(), Context: context}
}

// Starts a new engine session
//
//   {
//     "org_id": 1,
//     "flows": [{
//        "uuid": uuidv4,
//        "definition": {...},
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
func handleSimulationEvents(ctx context.Context, db models.Queryer, oa *models.OrgAssets, es []flows.Event) error {
	// nicpottier: this could be refactored into something more similar to how we handle normal events (ie hooks) if
	// we see ourselves taking actions for more than just webhook events
	wes := make([]*models.WebhookEvent, 0)
	for _, e := range es {
		if e.Type() == events.TypeResthookCalled {
			rec := e.(*events.ResthookCalledEvent)
			resthook := oa.ResthookBySlug(rec.Resthook)
			if resthook != nil {
				we := models.NewWebhookEvent(oa.OrgID(), resthook.ID(), string(rec.Payload), rec.CreatedOn())
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

	// grab our org assets
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load org assets")
	}

	// create clone of assets for simulation
	oa, err = oa.CloneForSimulation(s.CTX, s.DB, request.flows(), request.channels())
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to clone org")
	}

	// read our trigger
	trigger, err := triggers.ReadTrigger(oa.SessionAssets(), request.Trigger, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to read trigger")
	}

	return triggerFlow(ctx, s.DB, oa, trigger)
}

// triggerFlow creates a new session with the passed in trigger, returning our standard response
func triggerFlow(ctx context.Context, db *sqlx.DB, oa *models.OrgAssets, trigger flows.Trigger) (interface{}, int, error) {
	// start our flow session
	session, sprint, err := goflow.Simulator().NewSession(oa.SessionAssets(), trigger)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting session")
	}

	err = handleSimulationEvents(ctx, db, oa, sprint.Events())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error handling simulation events")
	}

	return newSimulationResponse(session, sprint), http.StatusOK, nil
}

// Resumes an existing engine session
//
//   {
//     "org_id": 1,
//     "flows": [{
//        "uuid": uuidv4,
//        "definition": {...},
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

	// grab our org assets
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// create clone of assets for simulation
	oa, err = oa.CloneForSimulation(s.CTX, s.DB, request.flows(), request.channels())
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	session, err := goflow.Simulator().ReadSession(oa.SessionAssets(), request.Session, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// read our resume
	resume, err := resumes.ReadResume(oa.SessionAssets(), request.Resume, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// if this is a msg resume we want to check whether it might be caught by a trigger
	if resume.Type() == resumes.TypeMsg {
		msgResume := resume.(*resumes.MsgResume)
		trigger := models.FindMatchingMsgTrigger(oa, msgResume.Contact(), msgResume.Msg().Text())
		if trigger != nil {
			var flow *models.Flow
			for _, r := range session.Runs() {
				if r.Status() == flows.RunStatusWaiting {
					f, _ := oa.Flow(r.FlowReference().UUID)
					if f != nil {
						flow = f.(*models.Flow)
					}
					break
				}
			}

			// we don't have a current flow or the current flow doesn't ignore triggers
			if flow == nil || (!flow.IgnoreTriggers() && trigger.TriggerType() == models.KeywordTriggerType) {
				triggeredFlow, err := oa.FlowByID(trigger.FlowID())
				if err != nil && err != models.ErrNotFound {
					return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load triggered flow")
				}

				if triggeredFlow != nil {
					trigger := triggers.NewBuilder(oa.Env(), triggeredFlow.FlowReference(), resume.Contact()).Msg(msgResume.Msg()).WithMatch(trigger.Match()).Build()
					return triggerFlow(ctx, s.DB, oa, trigger)
				}
			}
		}
	}

	// if our session is already complete, then this is a no-op, return the session unchanged
	if session.Status() != flows.SessionStatusWaiting {
		return &simulationResponse{Session: session, Events: nil}, http.StatusOK, nil
	}

	// resume our session
	sprint, err := session.Resume(resume)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	err = handleSimulationEvents(ctx, s.DB, oa, sprint.Events())
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error handling simulation events")
	}

	return newSimulationResponse(session, sprint), http.StatusOK, nil
}
