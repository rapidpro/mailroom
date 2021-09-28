package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
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
func handleResend(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &resendRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	msgs, err := models.LoadMessages(ctx, rt.DB, request.OrgID, models.DirectionOut, request.MsgIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error loading messages to resend")
	}

	err = models.ResendMessages(ctx, rt.DB, rt.RP, oa, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error resending messages")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, msgs)

	// response is the ids of the messages that were actually resent
	resentMsgIDs := make([]flows.MsgID, len(msgs))
	for i, m := range msgs {
		resentMsgIDs[i] = m.ID()
	}
	return map[string]interface{}{"msg_ids": resentMsgIDs}, http.StatusOK, nil
}
