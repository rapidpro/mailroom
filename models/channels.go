package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/lib/pq"
	"github.com/nyaruka/goflow/flows"
)

// Channel is the mailman struct that represents channels
type Channel struct {
	id      flows.ChannelID
	uuid    flows.ChannelUUID
	name    string
	address string
	schemes []string
	roles   []string
}

// ID returns the id of this channel
func (c *Channel) ID() flows.ChannelID { return c.id }

// UUID returns the UUID of this channel
func (c *Channel) UUID() flows.ChannelUUID { return c.uuid }

// Name returns the name of this channel
func (c *Channel) Name() string { return c.name }

// Address returns the name of this channel
func (c *Channel) Address() string { return c.address }

// Schemes returns the schemes this channel supports
func (c *Channel) Schemes() []string { return c.schemes }

// Roles returns the roles this channel supports
func (c *Channel) Roles() []string { return c.roles }

// loadChannels loads all the channels for the passed in org
func loadChannels(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Channel, error) {
	rows, err := db.Query(selectChannelsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying channels for org: %d", orgID)
	}
	defer rows.Close()

	channels := make([]*Channel, 0, 2)
	for rows.Next() {
		channel := &Channel{}

		err := rows.Scan(&channel.id, &channel.uuid, &channel.name, &channel.address, pq.Array(&channel.schemes), pq.Array(&channel.roles))
		if err != nil {
			return nil, errors.Annotate(err, "error scanning channel row")
		}

		channels = append(channels, channel)
	}

	return channels, nil
}

const selectChannelsSQL = `
SELECT
	id,
	uuid,
	name,
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
