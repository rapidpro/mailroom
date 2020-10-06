package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// InsertIncomingMsg inserts an incoming message
func InsertIncomingMsg(t *testing.T, db *sqlx.DB, orgID models.OrgID, contactID models.ContactID, urn urns.URN, urnID models.URNID, text string) *flows.MsgIn {
	msgUUID := flows.MsgUUID(uuids.New())
	var id flows.MsgID
	err := db.Get(&id,
		`INSERT INTO msgs_msg(uuid, text, created_on, direction, status, visibility, msg_count, error_count, next_attempt, contact_id, contact_urn_id, org_id)
	  	 VALUES($1, $2, NOW(), 'I', 'P', 'V', 1, 0, NOW(), $3, $4, $5) RETURNING id`, msgUUID, text, contactID, urnID, orgID)
	require.NoError(t, err)

	msg := flows.NewMsgIn(msgUUID, urn, nil, text, nil)
	msg.SetID(id)
	return msg
}
