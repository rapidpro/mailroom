package web

import (
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/ivr"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

type StartRequest struct {
	OrgID     models.OrgID     `form:"org"     validate:"required"`
	ChannelID models.ChannelID `form:"channel" validate:"required"`
	StartID   models.StartID   `form:"start"   validate:"required"`
	ContactID flows.ContactID  `form:"contact" validate:"required"`
}

func (s *Server) handleIVRStart(w http.ResponseWriter, r *http.Request) (interface{}, int, error) {
	request := &StartRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, maxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "request failed validation")
	}

	// load our org
	org, err := models.GetOrgAssets(s.ctx, s.db, request.OrgID)
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
	flowID, err := models.FlowIDForStart(s.ctx, s.db, request.OrgID, request.StartID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load start: %d", request.StartID)
	}

	// start our actual flow

	return "success", http.StatusOK, nil
}
