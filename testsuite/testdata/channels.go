package testdata

import (
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Channel struct {
	ID   models.ChannelID
	UUID assets.ChannelUUID
}

// InsertChannel inserts a channel
func InsertChannel(db *sqlx.DB, org *Org, channelType, name string, schemes []string, role string, config map[string]interface{}) *Channel {
	uuid := assets.ChannelUUID(uuids.New())
	var id models.ChannelID
	must(db.Get(&id,
		`INSERT INTO channels_channel(uuid, org_id, channel_type, name, schemes, role, config, last_seen, is_active, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, $2, $3, $4, $5, $6, $7, NOW(), TRUE, NOW(), NOW(), 1, 1) RETURNING id`, uuid, org.ID, channelType, name, pq.Array(schemes), role, null.NewMap(config),
	))
	return &Channel{id, uuid}
}
