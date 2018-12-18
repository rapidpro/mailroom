package ivr

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/pkg/errors"
)

type CallID string

const (
	NilCallID = CallID("")
)

var constructors = make(map[models.ChannelType]ClientConstructor)

// ClientConstructor defines our signature for creating a new IVR client from a channel
type ClientConstructor func(c *models.Channel) (IVRClient, error)

// RegisterClientType registers the passed in channel type with the passed in constructor
func RegisterClientType(channelType models.ChannelType, constructor ClientConstructor) {
	constructors[channelType] = constructor
}

// RequestCallStart creates a new ChannelSession for the passed in flow start and contact, returning the created session
func RequestCallStart(ctx context.Context, config *config.Config, db *sqlx.DB, org *models.OrgAssets, start *models.FlowStart, c *models.Contact) (*models.ChannelSession, error) {
	// find a tel URL for the contact
	telURN := urns.NilURN
	for _, u := range c.URNs() {
		if u.Scheme() == urns.TelScheme {
			telURN = u
		}
	}

	if telURN == urns.NilURN {
		return nil, errors.Errorf("no tel URN on contact, cannot start IVR flow")
	}

	// get the ID of our URN
	urnID := models.GetURNInt(telURN, "id")
	if urnID == 0 {
		return nil, errors.Errorf("no urn id for URN: %s, cannot start IVR flow", telURN)
	}

	// build our channel assets, we need these to calculate the preferred channel for a call
	channels, err := org.Channels()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load channels for org")
	}
	ca := flows.NewChannelAssets(channels)

	urn, err := flows.ParseRawURN(ca, telURN)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse URN: %s", telURN)
	}

	// get the channel to use for outgoing calls
	callChannel := ca.GetForURN(urn, assets.ChannelRoleCall)
	if callChannel == nil {
		// can't start call, no channel that can call
		return nil, nil
	}

	// get the channel for this URN
	channel := callChannel.Asset().(*models.Channel)

	// create our session
	session, err := models.CreateIVRSession(
		ctx, db, org.OrgID(), channel.ID(), c.ID(), models.URNID(urnID),
		models.ChannelSessionDirectionOut, models.ChannelSessionStatusPending, "",
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating ivr session")
	}

	domain := channel.ConfigValue(models.ChannelConfigCallbackDomain, config.Domain)

	// create our callback
	callbackURL := fmt.Sprintf("https://%s/mr/ivr/start?channel=%d&start=%d&contact=%s&channel_session=%d", domain, channel.ID(), start.StartID().Int64, c.UUID(), session.ID())
	statusURL := fmt.Sprintf("https://%s/mr/ivr/status?channel=%d&start=%d&contact=%s&channel_session=%d", domain, channel.ID(), start.StartID().Int64, c.UUID(), session.ID())

	// create the right client
	client, err := GetClient(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to create ivr client")
	}

	// start our call
	// TODO: our interface really really needs to return a ChannelLog type thing
	callID, err := client.RequestCall(channel, telURN, callbackURL, statusURL)
	if err != nil {
		// set our status as errored
		err := session.UpdateStatus(ctx, db, models.ChannelSessionStatusFailed)
		if err != nil {
			return nil, errors.Wrapf(err, "error setting errored status on session")
		}
		return session, nil
	}

	// create our channel session and return it
	err = session.UpdateExternalID(ctx, db, string(callID))
	if err != nil {
		return nil, errors.Wrapf(err, "error updating session external id")
	}

	return session, nil
}

// GetClient creates the right kind of IVRClient for the passed in channel
func GetClient(channel *models.Channel) (IVRClient, error) {
	constructor := constructors[channel.Type()]
	if constructor == nil {
		return nil, errors.Errorf("no ivr client for chanel type: %s", channel.Type())
	}

	return constructor(channel)
}

// IVRClient defines the interface IVR clients must satisfy
type IVRClient interface {
	RequestCall(c *models.Channel, number urns.URN, callbackURL string, statusURL string) (CallID, error)
}
