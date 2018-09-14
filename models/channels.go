package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	null "gopkg.in/guregu/null.v3"
)

type ChannelID int

type ChannelType string

const ChannelTypeAndroid = ChannelType("A")

// Channel is the mailroom struct that represents channels
type Channel struct {
	// inner struct for privacy and so we don't collide with method names
	c struct {
		ID            ChannelID                `json:"id"`
		UUID          assets.ChannelUUID       `json:"uuid"`
		Parent        *assets.ChannelReference `json:"parent"`
		Name          string                   `json:"name"`
		Address       string                   `json:"address"`
		ChannelType   ChannelType              `json:"channel_type"`
		TPS           int                      `json:"tps"`
		Country       null.String              `json:"country"`
		Schemes       []string                 `json:"schemes"`
		Roles         []assets.ChannelRole     `json:"roles"`
		MatchPrefixes []string                 `json:"match_prefixes"`
	}
}

// ID returns the id of this channel
func (c *Channel) ID() ChannelID { return c.c.ID }

// UUID returns the UUID of this channel
func (c *Channel) UUID() assets.ChannelUUID { return c.c.UUID }

// Name returns the name of this channel
func (c *Channel) Name() string { return c.c.Name }

// Type returns the channel type for this channel
func (c *Channel) Type() ChannelType { return c.c.ChannelType }

// TPS returns the max number of transactions per second this channel supports
func (c *Channel) TPS() int { return c.c.TPS }

// Address returns the name of this channel
func (c *Channel) Address() string { return c.c.Address }

// Country returns the contry code for this channel
func (c *Channel) Country() string { return c.c.Country.String }

// Schemes returns the schemes this channel supports
func (c *Channel) Schemes() []string { return c.c.Schemes }

// Roles returns the roles this channel supports
func (c *Channel) Roles() []assets.ChannelRole { return c.c.Roles }

// MatchPrefixes returns the prefixes we should also match when determining channel affinity
func (c *Channel) MatchPrefixes() []string { return c.c.MatchPrefixes }

// Parent returns the UUID of the parent channel to this channel
// TODO: add support for parent channels
func (c *Channel) Parent() *assets.ChannelReference { return c.c.Parent }

// loadChannels loads all the channels for the passed in org
func loadChannels(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.Channel, error) {
	rows, err := db.Queryx(selectChannelsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying channels for org: %d", orgID)
	}
	defer rows.Close()

	channels := make([]assets.Channel, 0, 2)
	for rows.Next() {
		channel := &Channel{}
		err := readJSONRow(rows, &channel.c)
		if err != nil {
			return nil, errors.Annotatef(err, "error unmarshalling channel")
		}

		channels = append(channels, channel)
	}

	return channels, nil
}

const selectChannelsSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	c.id as id,
	c.uuid as uuid,
	(SELECT ROW_TO_JSON(p) FROM (SELECT uuid, name FROM channels_channel cc where cc.id = c.parent_id) p) as parent,
	c.name as name,
	c.channel_type as channel_type,
	COALESCE(c.tps, 10) as tps,
	c.country as country,
	c.address as address,
	c.schemes as schemes,
	(SELECT ARRAY(
		SELECT CASE r 
		WHEN 'R' THEN 'receive' 
		WHEN 'S' THEN 'send'
		WHEN 'C' THEN 'call'
		WHEN 'A' THEN 'answer'
		WHEN 'U' THEN 'ussd'
		END 
		FROM unnest(regexp_split_to_array(c.role,'')) as r)
	) as roles,
	JSON_EXTRACT_PATH(c.config::json, 'matching_prefixes') as match_prefixes
FROM 
	channels_channel c
WHERE 
	c.org_id = $1 AND 
	c.is_active = TRUE
ORDER BY
	c.created_on ASC
) r;
`
