package models

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/nyaruka/null"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ChannelID is the type for channel IDs
type ChannelID null.Int

// NilChannelID is the nil value for channel IDs
const NilChannelID = ChannelID(0)

// ChannelType is the type for the type of a channel
type ChannelType string

// channel type constants
const (
	ChannelTypeAndroid = ChannelType("A")
)

// config key constants
const (
	ChannelConfigCallbackDomain      = "callback_domain"
	ChannelConfigMaxConcurrentEvents = "max_concurrent_events"
	ChannelConfigFCMID               = "FCM_ID"
)

// Channel is the mailroom struct that represents channels
type Channel struct {
	// inner struct for privacy and so we don't collide with method names
	c struct {
		ID                 ChannelID                `json:"id"`
		UUID               assets.ChannelUUID       `json:"uuid"`
		Parent             *assets.ChannelReference `json:"parent"`
		Name               string                   `json:"name"`
		Address            string                   `json:"address"`
		ChannelType        ChannelType              `json:"channel_type"`
		TPS                int                      `json:"tps"`
		Country            null.String              `json:"country"`
		Schemes            []string                 `json:"schemes"`
		Roles              []assets.ChannelRole     `json:"roles"`
		MatchPrefixes      []string                 `json:"match_prefixes"`
		AllowInternational bool                     `json:"allow_international"`
		MachineDetection   bool                     `json:"machine_detection"`
		Config             map[string]interface{}   `json:"config"`
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
func (c *Channel) Country() envs.Country { return envs.Country(string(c.c.Country)) }

// Schemes returns the schemes this channel supports
func (c *Channel) Schemes() []string { return c.c.Schemes }

// Roles returns the roles this channel supports
func (c *Channel) Roles() []assets.ChannelRole { return c.c.Roles }

// MatchPrefixes returns the prefixes we should also match when determining channel affinity
func (c *Channel) MatchPrefixes() []string { return c.c.MatchPrefixes }

// AllowInternational returns whether this channel allows sending internationally (only applies to TEL schemes)
func (c *Channel) AllowInternational() bool { return c.c.AllowInternational }

// MachineDetection returns whether this channel should do answering machine detection (only applies to IVR)
func (c *Channel) MachineDetection() bool { return c.c.MachineDetection }

// Parent returns a reference to the parent channel of this channel (if any)
func (c *Channel) Parent() *assets.ChannelReference { return c.c.Parent }

// Config returns the config for this channel
func (c *Channel) Config() map[string]interface{} { return c.c.Config }

// ConfigValue returns the config value for the passed in key
func (c *Channel) ConfigValue(key string, def string) string {
	value := c.c.Config[key]
	strValue, isString := value.(string)
	if isString {
		return strValue
	}
	floatValue, isFloat := value.(float64)
	if isFloat {
		return fmt.Sprintf("%d", int64(math.RoundToEven(floatValue)))
	}
	boolValue, isBool := value.(bool)
	if isBool {
		return fmt.Sprintf("%v", boolValue)
	}
	return def
}

// ChannelReference return a channel reference for this channel
func (c *Channel) ChannelReference() *assets.ChannelReference {
	return assets.NewChannelReference(c.UUID(), c.Name())
}

// GetChannelsByID fetches channels by ID - NOTE these are "lite" channels and only include fields for sending
func GetChannelsByID(ctx context.Context, db Queryer, ids []ChannelID) ([]*Channel, error) {
	rows, err := db.QueryxContext(ctx, selectChannelsByIDSQL, pq.Array(ids))
	if err != nil {
		return nil, errors.Wrapf(err, "error querying channels by id")
	}
	defer rows.Close()

	channels := make([]*Channel, 0, 5)
	for rows.Next() {
		channel := &Channel{}
		err := dbutil.ReadJSONRow(rows, &channel.c)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling channel")
		}

		channels = append(channels, channel)
	}

	return channels, nil
}

const selectChannelsByIDSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	c.id as id,
	c.uuid as uuid,
	c.name as name,
	c.channel_type as channel_type,
	COALESCE(c.tps, 10) as tps,
	COALESCE(c.config, '{}')::json as config
FROM 
	channels_channel c
WHERE 
	c.id = ANY($1)
) r;
`

// loadChannels loads all the channels for the passed in org
func loadChannels(ctx context.Context, db Queryer, orgID OrgID) ([]assets.Channel, error) {
	start := time.Now()

	rows, err := db.QueryxContext(ctx, selectChannelsSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying channels for org: %d", orgID)
	}
	defer rows.Close()

	channels := make([]assets.Channel, 0, 2)
	for rows.Next() {
		channel := &Channel{}
		err := dbutil.ReadJSONRow(rows, &channel.c)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling channel")
		}

		channels = append(channels, channel)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(channels)).Debug("loaded channels")

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
	COALESCE(c.config, '{}')::json as config,
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
	JSON_EXTRACT_PATH(c.config::json, 'matching_prefixes') as match_prefixes,
	JSON_EXTRACT_PATH(c.config::json, 'allow_international') as allow_international,
	JSON_EXTRACT_PATH(c.config::json, 'machine_detection') as machine_detection
FROM 
	channels_channel c
WHERE 
	c.org_id = $1 AND 
	c.is_active = TRUE
ORDER BY
	c.created_on ASC
) r;
`

// OrgIDForChannelUUID returns the org id for the passed in channel UUID if any
func OrgIDForChannelUUID(ctx context.Context, db Queryer, channelUUID assets.ChannelUUID) (OrgID, error) {
	var orgID OrgID
	err := db.GetContext(ctx, &orgID, `SELECT org_id FROM channels_channel WHERE uuid = $1 AND is_active = TRUE`, channelUUID)
	if err != nil {
		return NilOrgID, errors.Wrapf(err, "no channel found with uuid: %s", channelUUID)
	}
	return orgID, nil
}

// MarshalJSON marshals into JSON. 0 values will become null
func (i ChannelID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *ChannelID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i ChannelID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *ChannelID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
