package web

import (
	"context"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/runner"
	"github.com/pkg/errors"
)

type StartRequest struct {
	OrgID     models.OrgID     `form:"org"     validate:"required"`
	ChannelID models.ChannelID `form:"channel" validate:"required"`
	StartID   models.StartID   `form:"start"   validate:"required"`
	ContactID flows.ContactID  `form:"contact" validate:"required"`
}

func (s *Server) handleIVRStart(ctx context.Context, r *http.Request) (interface{}, int, error) {
	request := &StartRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, maxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "request failed validation")
	}

	// load our org
	org, err := models.GetOrgAssets(ctx, s.db, request.OrgID)
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error loading org assets")
	}

	// and our channel
	channel := org.ChannelByID(request.ChannelID)
	if channel == nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "no active channel with id: %d", request.ChannelID)
	}

	// get the right kind of client
	client, err := ivr.GetClient(channel)
	if client == nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load client for channel: %d", request.ChannelID)
	}

	// get the flow for our start
	flowID, err := models.FlowIDForStart(ctx, s.db, request.OrgID, request.StartID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load start: %d", request.StartID)
	}

	flow, err := org.FlowByID(flowID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load flow: %d", request.StartID)
	}

	// build our session assets
	sessionAssets, err := models.GetSessionAssets(org)
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error starting flow, unable to load assets")
	}

	contacts, err := models.LoadContacts(ctx, s.db, org, []flows.ContactID{request.ContactID})
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error loading contacts")
	}
	if len(contacts) == 0 {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "no contact width id: %d", request.ContactID)
	}

	contact, err := contacts[0].FlowContact(org, sessionAssets)
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error creating flow contact")
	}

	// our builder for the triggers that will be created for contacts
	flowRef := assets.NewFlowReference(flow.UUID(), flow.Name())
	trigger := triggers.NewManualTrigger(org.Env(), flowRef, contact, nil, time.Now())

	sessions, err := runner.StartFlowForContacts(ctx, s.db, s.rp, org, sessionAssets, []flows.Trigger{trigger}, nil, true)
	if err != nil {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error starting flow")
	}

	if len(sessions) == 0 {
		return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "no session created")
	}

	return "success", http.StatusOK, nil
}
