package msg

import (
	"context"
	"net/http"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/msgio"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/msg/resend", web.RequireAuthToken(web.JSONPayload(handleResend)))
}

// Request to resend failed messages.
//
//	{
//	  "org_id": 1,
//	  "msg_ids": [123456, 345678]
//	}
type resendRequest struct {
	OrgID  models.OrgID   `json:"org_id"   validate:"required"`
	MsgIDs []models.MsgID `json:"msg_ids"  validate:"required"`
}

// handles a request to resend the given messages
func handleResend(ctx context.Context, rt *runtime.Runtime, r *resendRequest) (any, int, error) {
	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, r.OrgID)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unable to load org assets")
	}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, r.OrgID, models.DirectionOut, r.MsgIDs)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error loading messages to resend")
	}

	resends, err := models.ResendMessages(ctx, rt, oa, msgs)
	if err != nil {
		return nil, 0, errors.Wrap(err, "error resending messages")
	}

	msgio.QueueMessages(ctx, rt, rt.DB, nil, resends)

	// response is the ids of the messages that were actually resent
	resentMsgIDs := make([]flows.MsgID, len(resends))
	for i, m := range resends {
		resentMsgIDs[i] = m.ID()
	}
	return map[string]any{"msg_ids": resentMsgIDs}, http.StatusOK, nil
}
