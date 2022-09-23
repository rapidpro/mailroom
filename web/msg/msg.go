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
	web.RegisterJSONRoute(http.MethodPost, "/mr/msg/fail", web.RequireAuthToken(handleFail))
}

// Request to resend failed messages.
//
//   {
//     "org_id": 1,
//     "msg_ids": [123456, 345678]
//   }
//
type msgsRequest struct {
	OrgID  models.OrgID   `json:"org_id"   validate:"required"`
	MsgIDs []models.MsgID `json:"msg_ids"  validate:"required"`
}

// handles a request to resend the given messages
func handleResend(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &msgsRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, request.OrgID, models.DirectionOut, request.MsgIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error loading messages to resend")
	}

	resends, err := models.ResendMessages(ctx, rt.DB, rt.RP, oa, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error resending messages")
	}

	msgio.SendMessages(ctx, rt, rt.DB, nil, resends)

	// response is the ids of the messages that were actually resent
	resentMsgIDs := make([]flows.MsgID, len(resends))
	for i, m := range resends {
		resentMsgIDs[i] = m.ID()
	}
	return map[string]interface{}{"msg_ids": resentMsgIDs}, http.StatusOK, nil
}

// handles a request to fail the given messages
func handleFail(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &msgsRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return errors.Wrapf(err, "request failed validation"), http.StatusBadRequest, nil
	}

	// grab our org
	oa, err := models.GetOrgAssets(ctx, rt, request.OrgID)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to load org assets")
	}

	msgs, err := models.GetMessagesByID(ctx, rt.DB, request.OrgID, models.DirectionOut, request.MsgIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error loading messages to fail")
	}

	failedMsgs, err := models.FailMessages(ctx, rt.DB, rt.RP, oa, msgs)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrap(err, "error failing messages")
	}

	// response is the ids of the messages that were actually failed
	failedMsgIDs := make([]flows.MsgID, len(failedMsgs))
	for i, m := range failedMsgs {
		failedMsgIDs[i] = m.ID()
	}
	return map[string]interface{}{"msg_ids": failedMsgIDs}, http.StatusOK, nil
}
