package testdata

import (
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// InsertChannel inserts a channel
func InsertChannel(t *testing.T, db *sqlx.DB, orgID models.OrgID, channelType, name string, schemes []string, role string, config map[string]interface{}) models.ChannelID {
	var id models.ChannelID
	err := db.Get(&id,
		`INSERT INTO channels_channel(uuid, org_id, channel_type, name, schemes, role, config, last_seen, is_active, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, $2, $3, $4, $5, $6, $7, NOW(), TRUE, NOW(), NOW(), 1, 1) RETURNING id`, uuids.New(), orgID, channelType, name, pq.Array(schemes), role, null.NewMap(config),
	)
	require.NoError(t, err)
	return id
}
