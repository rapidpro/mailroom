package surveyor

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/goflow"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
)

func init() {
	web.RegisterJSONRoute(http.MethodPost, "/mr/surveyor/submit", web.RequireUserToken(handleSubmit))
}

// Represents a surveyor submission
//
//   {
//     "session": {...},
//     "events": [{...}],
//     "modifiers": [{...}]
//   }
//
type submitRequest struct {
	Session   json.RawMessage   `json:"session"    validate:"required"`
	Events    []json.RawMessage `json:"events"`
	Modifiers []json.RawMessage `json:"modifiers"`
}

type submitResponse struct {
	Session struct {
		ID     models.SessionID     `json:"id"`
		Status models.SessionStatus `json:"status"`
	} `json:"session"`
	Contact struct {
		ID   flows.ContactID   `json:"id"`
		UUID flows.ContactUUID `json:"uuid"`
	} `json:"contact"`
}

// handles a surveyor request
func handleSubmit(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	request := &submitRequest{}
	if err := utils.UnmarshalAndValidateWithLimit(r.Body, request, web.MaxRequestBytes); err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "request failed validation")
	}

	// grab our org assets
	orgID := ctx.Value(web.OrgIDKey).(models.OrgID)
	oa, err := models.GetOrgAssets(ctx, rt, orgID)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "unable to load org assets")
	}

	// and our user id
	_, valid := ctx.Value(web.UserIDKey).(int64)
	if !valid {
		return nil, http.StatusInternalServerError, errors.Errorf("missing request user")
	}

	fs, err := goflow.Engine(rt.Config).ReadSession(oa.SessionAssets(), request.Session, assets.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, errors.Wrapf(err, "error reading session")
	}

	// and our events
	sessionEvents := make([]flows.Event, 0, len(request.Events))
	for _, e := range request.Events {
		event, err := events.ReadEvent(e)
		if err != nil {
			return nil, http.StatusBadRequest, errors.Wrapf(err, "error unmarshalling event: %s", string(e))
		}
		sessionEvents = append(sessionEvents, event)
	}

	// and our modifiers
	mods, err := goflow.ReadModifiers(oa.SessionAssets(), request.Modifiers, goflow.IgnoreMissing)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// get the current version of this contact from the database
	var flowContact *flows.Contact

	if len(fs.Contact().URNs()) > 0 {
		// create / fetch our contact based on the highest priority URN
		urn := fs.Contact().URNs()[0].URN()

		_, flowContact, _, err = models.GetOrCreateContact(ctx, rt.DB, oa, []urns.URN{urn}, models.NilChannelID)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to look up contact")
		}
	} else {
		_, flowContact, err = models.CreateContact(ctx, rt.DB, oa, models.NilUserID, "", envs.NilLanguage, nil)
		if err != nil {
			return nil, http.StatusInternalServerError, errors.Wrapf(err, "unable to create contact")
		}
	}

	modifierEvents := make([]flows.Event, 0, len(mods))
	appender := func(e flows.Event) {
		modifierEvents = append(modifierEvents, e)
	}

	// run through each contact modifier, applying it to our contact
	for _, m := range mods {
		m.Apply(oa.Env(), oa.SessionAssets(), flowContact, appender)
	}

	// set this updated contact on our session
	fs.SetContact(flowContact)

	// append our session events to our modifiers events, the union will be used to update the db/contact
	modifierEvents = append(modifierEvents, sessionEvents...)

	// create our sprint
	sprint := engine.NewSprint(mods, modifierEvents)

	// write our session out
	tx, err := rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting transaction for session write")
	}
	sessions, err := models.WriteSessions(ctx, rt, tx, oa, []flows.Session{fs}, []flows.Sprint{sprint}, nil)
	if err == nil && len(sessions) == 0 {
		err = errors.Errorf("no sessions written")
	}
	if err != nil {
		tx.Rollback()
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error writing session")
	}
	err = tx.Commit()
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error committing sessions")
	}

	tx, err = rt.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error starting transaction for post commit hooks")
	}

	// write our post commit hooks
	err = models.ApplyEventPostCommitHooks(ctx, rt, tx, oa, []*models.Scene{sessions[0].Scene()})
	if err != nil {
		tx.Rollback()
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error applying post commit hooks")
	}
	err = tx.Commit()
	if err != nil {
		return nil, http.StatusInternalServerError, errors.Wrapf(err, "error committing post commit hooks")
	}

	response := &submitResponse{}
	response.Session.ID = sessions[0].ID()
	response.Session.Status = sessions[0].Status()
	response.Contact.ID = flowContact.ID()
	response.Contact.UUID = flowContact.UUID()

	return response, http.StatusCreated, nil
}
