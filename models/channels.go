package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/assets"
	null "gopkg.in/guregu/null.v3"
)

type ChannelID int

// Channel is the mailman struct that represents channels
type Channel struct {
	id       ChannelID
	uuid     assets.ChannelUUID
	parent   *assets.ChannelReference
	name     string
	address  string
	country  null.String
	schemes  []string
	roles    []assets.ChannelRole
	prefixes []string
}

// ID returns the id of this channel
func (c *Channel) ID() ChannelID { return c.id }

// UUID returns the UUID of this channel
func (c *Channel) UUID() assets.ChannelUUID { return c.uuid }

// Name returns the name of this channel
func (c *Channel) Name() string { return c.name }

// Address returns the name of this channel
func (c *Channel) Address() string { return c.address }

// Country returns the contry code for this channel
func (c *Channel) Country() string { return c.country.String }

// Schemes returns the schemes this channel supports
func (c *Channel) Schemes() []string { return c.schemes }

// Roles returns the roles this channel supports
func (c *Channel) Roles() []assets.ChannelRole { return c.roles }

// MatchPrefixes returns the prefixes we should also match when determining channel affinity
func (c *Channel) MatchPrefixes() []string { return c.prefixes }

// Parent returns the UUID of the parent channel to this channel
// TODO: add support for parent channels
func (c *Channel) Parent() *assets.ChannelReference { return c.parent }

// loadChannels loads all the channels for the passed in org
func loadChannels(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Channel, error) {
	rows, err := db.Query(selectChannelsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying channels for org: %d", orgID)
	}
	defer rows.Close()

	channels := make([]assets.Channel, 0, 2)
	for rows.Next() {
		channel := &Channel{}
		var roles []string
		var parentName, parentUUID *string

		err := rows.Scan(&channel.id, &channel.uuid, &parentUUID, &parentName, &channel.name, &channel.country, &channel.address, pq.Array(&channel.schemes), pq.Array(&roles))
		if err != nil {
			return nil, errors.Annotate(err, "error scanning channel row")
		}

		// populate our roles
		for _, r := range roles {
			channel.roles = append(channel.roles, assets.ChannelRole(r))
		}

		// and our parent if present
		if parentUUID != nil && parentName != nil {
			channel.parent = assets.NewChannelReference(assets.ChannelUUID(*parentUUID), *parentName)
		}

		channels = append(channels, channel)
	}

	return channels, nil
}

const selectChannelsSQL = `
SELECT
	id,
	uuid,
	(SELECT uuid FROM channels_channel where id = parent_id) as parent_uuid,
	(SELECT name FROM channels_channel where id = parent_id) as parent_name,
	name,
	country,
	address,
	schemes,
	(SELECT ARRAY(
		SELECT CASE r 
		WHEN 'R' THEN 'receive' 
		WHEN 'S' THEN 'send'
		WHEN 'C' THEN 'call'
		WHEN 'A' THEN 'answer'
		WHEN 'U' THEN 'ussd'
		END 
		FROM unnest(regexp_split_to_array(role,'')) as r)
	) as roles
FROM 
	channels_channel
WHERE 
	org_id = $1 AND 
	is_active = TRUE
ORDER BY
	created_on ASC
`
