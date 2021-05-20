package testdata

import (
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/core/models"
)

// Constants used in tests, these are tied to the DB created by the RapidPro `mailroom_db` management command.

type testOrg struct {
	ID   models.OrgID
	UUID uuids.UUID
	Msgs []models.MsgID
}

var Org1 = testOrg{
	ID:   1,
	UUID: uuids.UUID("bf0514a5-9407-44c9-b0f9-3f36f9c18414"),
	Msgs: []models.MsgID{10000, 10001, 10002},
}

var Org2 = testOrg{
	ID:   2,
	UUID: uuids.UUID("3ae7cdeb-fd96-46e5-abc4-a4622f349921"),
}
