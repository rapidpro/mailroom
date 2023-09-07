package po

import (
	"context"
	"net/http"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/goflow/flows/translation"
	"github.com/nyaruka/goflow/utils/po"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/po/import", web.RequireAuthToken(web.MarshaledResponse(handleImport)))
}

// Imports translations from a PO file into the given set of flows.
//
//	{
//	  "org_id": 123,
//	  "flow_ids": [123, 354, 456],
//	  "language": "spa"
//	}
type importForm struct {
	OrgID    models.OrgID    `form:"org_id"  validate:"required"`
	FlowIDs  []models.FlowID `form:"flow_ids" validate:"required"`
	Language i18n.Language   `form:"language" validate:"required"`
}

func handleImport(ctx context.Context, rt *runtime.Runtime, r *http.Request) (any, int, error) {
	form := &importForm{}
	if err := web.DecodeAndValidateForm(form, r); err != nil {
		return err, http.StatusBadRequest, nil
	}

	poFile, _, err := r.FormFile("po")
	if err != nil {
		return errors.Wrapf(err, "missing po file on request"), http.StatusBadRequest, nil
	}

	p, err := po.ReadPO(poFile)
	if err != nil {
		return errors.Wrapf(err, "invalid po file"), http.StatusBadRequest, nil
	}

	flows, err := loadFlows(ctx, rt, form.OrgID, form.FlowIDs)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	err = translation.ImportIntoFlows(p, form.Language, excludeProperties, flows...)
	if err != nil {
		return err, http.StatusBadRequest, nil
	}

	return map[string]any{"flows": flows}, http.StatusOK, nil
}
