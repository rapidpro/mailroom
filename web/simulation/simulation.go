package simulation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/assets/static"
	"github.com/nyaruka/goflow/excellent/tools"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

var testChannel = assets.NewChannelReference("440099cf-200c-4d45-a8e7-4a564f4a0e8b", "Test Channel")
var testURN = urns.URN("tel:+12065551212")

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/sim/start", web.RequireAuthToken(web.JSONPayload(handleStart)))
	web.RegisterRoute(http.MethodPost, "/mr/sim/resume", web.RequireAuthToken(web.JSONPayload(handleResume)))
}

type flowDefinition struct {
	UUID       assets.FlowUUID `json:"uuid"       validate:"required"`
	Definition json.RawMessage `json:"definition" validate:"required"`
}

type sessionRequest struct {
	OrgID  models.OrgID     `json:"org_id"  validate:"required"`
	Flows  []flowDefinition `json:"flows"`
	Assets struct {
		Channels []*static.Channel `json:"channels"`
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
	Session  flows.Session   `json:"session"`
	Events   []flows.Event   `json:"events"`
	Segments []flows.Segment `json:"segments"`
	Context  *types.XObject  `json:"context,omitempty"`
}

func newSimulationResponse(session flows.Session, sprint flows.Sprint) *simulationResponse {
	var context *types.XObject
	if session != nil {
		context = session.CurrentContext()

		// include object defaults which are not marshaled by default
		if context != nil {
			tools.ContextWalkObjects(context, func(o *types.XObject) {
				o.SetMarshalDefault(true)
			})
		}
	}
	return &simulationResponse{Session: session, Events: sprint.Events(), Segments: sprint.Segments(), Context: context}
}

// Starts a new engine session
//
//	{
//	  "org_id": 1,
//	  "flows": [{
//	     "uuid": uuidv4,
//	     "definition": {...},
//	  },.. ],
//	  "trigger": {...},
//	  "assets": {...}
//	}
type startRequest struct {
	sessionRequest
	Trigger json.RawMessage `json:"trigger" validate:"required"`
}

// handleSimulationEvents takes care of updating our db with any events needed during simulation
func handleSimulationEvents(ctx context.Context, db models.DBorTx, oa *models.OrgAssets, es []flows.Event) error {
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
func handleStart(ctx context.Context, rt *runtime.Runtime, r *startRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load org assets")
	}

	// create clone of assets for simulation
	oa, err = oa.CloneForSimulation(ctx, rt, r.flows(), r.channels())
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to clone org")
	}

	// read our trigger
	trigger, err := triggers.ReadTrigger(oa.SessionAssets(), r.Trigger, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to read trigger")
	}

	return triggerFlow(ctx, rt, oa, trigger)
}

// triggerFlow creates a new session with the passed in trigger, returning our standard response
func triggerFlow(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, trigger flows.Trigger) (any, int, error) {
	// start our flow session
	session, sprint, err := goflow.Simulator(rt.Config).NewSession(oa.SessionAssets(), trigger)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error starting session")
	}

	err = handleSimulationEvents(ctx, rt.DB, oa, sprint.Events())
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error handling simulation events")
	}

	return newSimulationResponse(session, sprint), http.StatusOK, nil
}

// Resumes an existing engine session
//
//	{
//	  "org_id": 1,
//	  "flows": [{
//	     "uuid": uuidv4,
//	     "definition": {...},
//	  },.. ],
//	  "session": {"uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "runs": [...], ...},
//	  "resume": {...},
//	  "assets": {...}
//	}
type resumeRequest struct {
	sessionRequest

	Session json.RawMessage `json:"session" validate:"required"`
	Resume  json.RawMessage `json:"resume" validate:"required"`
}

func handleResume(ctx context.Context, rt *runtime.Runtime, r *resumeRequest) (any, int, error) {
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// create clone of assets for simulation
	oa, err = oa.CloneForSimulation(ctx, rt, r.flows(), r.channels())
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	session, err := goflow.Simulator(rt.Config).ReadSession(oa.SessionAssets(), r.Session, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// read our resume
	resume, err := resumes.ReadResume(oa.SessionAssets(), r.Resume, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// if this is a msg resume we want to check whether it might be caught by a trigger
	if resume.Type() == resumes.TypeMsg {
		msgResume := resume.(*resumes.MsgResume)
		trigger, keyword := models.FindMatchingMsgTrigger(oa, nil, msgResume.Contact(), msgResume.Msg().Text())
		if trigger != nil {
			var flow *models.Flow
			for _, r := range session.Runs() {
				if r.Status() == flows.RunStatusWaiting {
					f, _ := oa.FlowByUUID(r.FlowReference().UUID)
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
					return nil, 0, errors.Wrapf(err, "unable to load triggered flow")
				}

				if triggeredFlow != nil {
					tb := triggers.NewBuilder(oa.Env(), triggeredFlow.Reference(), resume.Contact())

					var sessionTrigger flows.Trigger
					if triggeredFlow.FlowType() == models.FlowTypeVoice {
						// TODO this should trigger a msg trigger with a call but first we need to rework
						// non-simulation IVR triggers to use that so that this is consistent.
						sessionTrigger = tb.Manual().WithCall(testChannel, testURN).Build()
					} else {
						mtb := tb.Msg(msgResume.Msg())
						if keyword != "" {
							mtb = mtb.WithMatch(&triggers.KeywordMatch{Type: trigger.KeywordMatchType(), Keyword: keyword})
						}
						sessionTrigger = mtb.Build()
					}

					return triggerFlow(ctx, rt, oa, sessionTrigger)
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
		return nil, 0, err
	}

	err = handleSimulationEvents(ctx, rt.DB, oa, sprint.Events())
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error handling simulation events")
	}

	return newSimulationResponse(session, sprint), http.StatusOK, nil
}
