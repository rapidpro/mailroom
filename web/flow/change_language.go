package flow

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/flow/change_language", web.RequireAuthToken(web.JSONPayload(handleChangeLanguage)))
}

// Changes the language of a flow by replacing the text with a translation.
//
//	{
//	  "language": "spa",
//	  "flow": { "uuid": "468621a8-32e6-4cd2-afc1-04416f7151f0", "nodes": [...]}
//	}
type changeLanguageRequest struct {
	Language i18n.Language   `json:"language" validate:"required"`
	Flow     json.RawMessage `json:"flow"     validate:"required"`
}

func handleChangeLanguage(ctx context.Context, rt *runtime.Runtime, r *changeLanguageRequest) (any, int, error) {
	flow, err := goflow.ReadFlow(rt.Config, r.Flow)
	if err != nil {
		return errors.Wrapf(err, "unable to read flow"), http.StatusUnprocessableEntity, nil
	}

	copy, err := flow.ChangeLanguage(r.Language)
	if err != nil {
		return errors.Wrapf(err, "unable to change flow language"), http.StatusUnprocessableEntity, nil
	}

	return copy, http.StatusOK, nil
}
