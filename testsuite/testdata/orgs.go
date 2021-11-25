package testdata

import (
	"context"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
)

type Org struct {
	ID   models.OrgID
	UUID uuids.UUID
}

func (o *Org) Load(rt *runtime.Runtime) *models.OrgAssets {
	oa, err := models.GetOrgAssets(context.Background(), rt, o.ID)
	must(err)
	return oa
}

type User struct {
	ID    models.UserID
	Email string
}

func (u *User) SafeID() models.UserID {
	if u != nil {
		return u.ID
	}
	return models.NilUserID
}
