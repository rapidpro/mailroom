package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/msg/resend", web.RequireAuthToken(handleResend))
}

// Request to resend failed messages.
//
//   {
//     "org_id": 1,
//     "msg_ids": [123456, 345678]
//   }
//
type resendRequest struct {
	OrgID  models.OrgID   `json:"org_id"   validate:"required"`
	MsgIDs []models.MsgID `json:"msg_ids"  validate:"required"`
}

// handles a request to resend the given messages
func handleResend(ctx context.Context, s *web.Server, r *http.Request) (interface{}, int, error) {
	request := &resendRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(s.CTX, s.DB, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	msgs, err := models.LoadMessages(ctx, s.DB, request.OrgID, models.DirectionOut, request.MsgIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error loading messages to resend")
	}

	err = models.ResendMessages(ctx, s.DB, s.RP, oa, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error resending messages")
	}

	msgio.SendMessages(ctx, s.DB, s.RP, nil, msgs)

	return nil, http.StatusOK, nil
}
