package contact

import (
	"context"
	"net/http"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
)

func init() {
	web.RegisterRoute(http.MethodPost, "/mr/contact/interrupt", web.RequireAuthToken(web.JSONPayload(handleInterrupt)))
}

// Request that a single contact is interrupted. Multiple contacts should be interrupted via the task.
//
//	{
//	  "org_id": 1,
//	  "user_id": 3,
//	  "contact_id": 235
//	}
type interruptRequest struct {
	OrgID     models.OrgID     `json:"org_id"     validate:"required"`
	UserID    models.UserID    `json:"user_id"    validate:"required"`
	ContactID models.ContactID `json:"contact_id" validate:"required"`
}

// handles a request to interrupt a contact
func handleInterrupt(ctx context.Context, rt *runtime.Runtime, r *interruptRequest) (any, int, error) {
	count, err := models.InterruptSessionsForContacts(ctx, rt.DB, []models.ContactID{r.ContactID})
	if err != nil {
		return nil, 0, errors.Wrapf(err, "unable to interrupt contact")
	}

	return map[string]any{"sessions": count}, http.StatusOK, nil
}
