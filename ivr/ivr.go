package ivr

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

type CallID string

const (
	NilCallID = CallID("")
)

var constructors = make(map[models.ChannelType]ClientConstructor)

type ClientConstructor func(c *models.Channel) (IVRClient, error)

func RegisterClientType(channelType models.ChannelType, constructor ClientConstructor) {
	constructors[channelType] = constructor
}

/*
func RequestCallStart(ctx context.Context, db *sqlx.DB, org *models.OrgAssets, start *models.FlowStart, c *models.Contact) error {
	// find a tel URL for the contact
	telURN := urns.NilURN
	for _, u := range c.URNs() {
		if u.Scheme() == urns.TelScheme {
			telURN = u
		}
	}

	if telURN == urns.NilURN {
		return errors.Errorf("no tel URN on contact, cannot start IVR flow")
	}

	// get the channel for this URN
	channels, err := org.Channels()
	if err != nil {
		return errors.Wrapf(err, "unable to fetch channels for org")
	}

	// TODO: get the channel for the URN
	channel := channels[0].(*models.Channel)

	// create our callback
	url := "https://mr/ivr/start?start=UUID&contact=UUID"

	// create the right client
	client, err := GetClient(channel)
	if err != nil {
		return errors.Wrapf(err, "unable to create ivr client")
	}

	// start our call
	callID, err := client.RequestCall(channel, urn, url, url)

	// create our channel session and return it
	return models.CreateChannelSession()
}
*/

// GetClient creates the right kind of IVRClient for the passed in channel
func GetClient(channel *models.Channel) (IVRClient, error) {
	constructor := constructors[channel.Type()]
	if constructor == nil {
		return nil, errors.Errorf("no ivr client for chanel type: %s", channel.Type())
	}

	return constructor(channel)
}

type IVRClient interface {
	RequestCall(c *models.Channel, number urns.URN, callbackURL string, statusURL string) (CallID, error)
}
