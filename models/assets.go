package models

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/goflow/utils"
	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

// OrgAssets is our top level cache of all things contained in an org. It is used to build
// SessionAssets for the engine but also used to cache campaigns and other org level attributes
type OrgAssets struct {
	ctx     context.Context
	db      *sqlx.DB
	builtAt time.Time

	orgID OrgID

	env *Org

	flowByUUID    map[assets.FlowUUID]assets.Flow
	flowByID      map[FlowID]assets.Flow
	flowCacheLock sync.RWMutex

	channels       []assets.Channel
	channelsByID   map[ChannelID]*Channel
	channelsByUUID map[assets.ChannelUUID]*Channel

	campaigns             []*Campaign
	campaignEventsByField map[FieldID][]*CampaignEvent
	campaignEventsByID    map[CampaignEventID]*CampaignEvent
	campaignsByGroup      map[GroupID][]*Campaign

	fields       []assets.Field
	fieldsByUUID map[FieldUUID]*Field
	fieldsByKey  map[string]*Field

	groups       []assets.Group
	groupsByID   map[GroupID]*Group
	groupsByUUID map[assets.GroupUUID]*Group

	labels    []assets.Label
	resthooks []assets.Resthook
	triggers  []*Trigger

	locations        []assets.LocationHierarchy
	locationsBuiltAt time.Time
}

var orgCache = cache.New(time.Hour, time.Minute*5)
var assetCache = cache.New(5*time.Second, time.Minute*5)

const cacheTimeout = time.Second * 5
const locationCacheTimeout = time.Hour

// FlushCache clears our entire org cache
func FlushCache() {
	orgCache.Flush()
	assetCache.Flush()
}

// GetOrgAssets creates or gets org assets for the passed in org
func GetOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) (*OrgAssets, error) {
	// do we have a recent cache?
	key := fmt.Sprintf("%d", orgID)
	var cached *OrgAssets
	c, found := orgCache.Get(key)
	if found {
		cached = c.(*OrgAssets)
	}

	// if we found a source built in the last five seconds, use it
	if found && time.Since(cached.builtAt) < cacheTimeout {
		return cached, nil
	}

	// otherwire, we build one from scratch
	o := &OrgAssets{
		ctx:     ctx,
		db:      db,
		builtAt: time.Now(),

		orgID: orgID,

		channelsByID:   make(map[ChannelID]*Channel),
		channelsByUUID: make(map[assets.ChannelUUID]*Channel),

		fieldsByUUID: make(map[FieldUUID]*Field),
		fieldsByKey:  make(map[string]*Field),

		groupsByID:   make(map[GroupID]*Group),
		groupsByUUID: make(map[assets.GroupUUID]*Group),

		campaignEventsByField: make(map[FieldID][]*CampaignEvent),
		campaignEventsByID:    make(map[CampaignEventID]*CampaignEvent),
		campaignsByGroup:      make(map[GroupID][]*Campaign),

		flowByUUID: make(map[assets.FlowUUID]assets.Flow),
		flowByID:   make(map[FlowID]assets.Flow),
	}

	// we load everything at once except for flows which are lazily loaded
	var err error

	o.env, err = loadOrg(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading environment for org %d", orgID)
	}

	o.channels, err = loadChannels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading channel assets for org %d", orgID)
	}
	for _, c := range o.channels {
		channel := c.(*Channel)
		o.channelsByID[channel.ID()] = channel
		o.channelsByUUID[channel.UUID()] = channel
	}

	o.fields, err = loadFields(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading field assets for org %d", orgID)
	}
	for _, f := range o.fields {
		field := f.(*Field)
		o.fieldsByUUID[field.UUID()] = field
		o.fieldsByKey[field.Key()] = field
	}

	o.groups, err = loadGroups(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading group assets for org %d", orgID)
	}
	for _, g := range o.groups {
		group := g.(*Group)
		o.groupsByID[group.ID()] = group
		o.groupsByUUID[group.UUID()] = group
	}

	o.labels, err = loadLabels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading group labels for org %d", orgID)
	}

	o.resthooks, err = loadResthooks(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading resthooks for org %d", orgID)
	}

	o.campaigns, err = loadCampaigns(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading campaigns for org %d", orgID)
	}
	for _, c := range o.campaigns {
		o.campaignsByGroup[c.GroupID()] = append(o.campaignsByGroup[c.GroupID()], c)
		for _, e := range c.Events() {
			o.campaignEventsByField[e.RelativeToID()] = append(o.campaignEventsByField[e.RelativeToID()], e)
			o.campaignEventsByID[e.ID()] = e
		}
	}

	o.triggers, err = loadTriggers(ctx, db, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading triggers for org %d", orgID)
	}

	// cache locations for an hour
	if cached != nil && time.Since(cached.locationsBuiltAt) < locationCacheTimeout {
		o.locations = cached.locations
		o.locationsBuiltAt = cached.locationsBuiltAt
	} else {
		o.locations, err = loadLocations(ctx, db, orgID)
		o.locationsBuiltAt = time.Now()
		if err != nil {
			return nil, errors.Wrapf(err, "error loading group locations for org %d", orgID)
		}
	}

	// add this org to our cache
	orgCache.Add(key, o, time.Minute)
	return o, nil
}

func (a *OrgAssets) OrgID() OrgID { return a.orgID }

func (a *OrgAssets) Env() utils.Environment { return a.env }

func (a *OrgAssets) Org() *Org { return a.env }

func (a *OrgAssets) Channels() ([]assets.Channel, error) {
	return a.channels, nil
}

func (a *OrgAssets) ChannelByUUID(channelUUID assets.ChannelUUID) *Channel {
	return a.channelsByUUID[channelUUID]
}

func (a *OrgAssets) ChannelByID(channelID ChannelID) *Channel {
	return a.channelsByID[channelID]
}

func (a *OrgAssets) Fields() ([]assets.Field, error) {
	return a.fields, nil
}

func (a *OrgAssets) FieldByUUID(fieldUUID FieldUUID) *Field {
	return a.fieldsByUUID[fieldUUID]
}

func (a *OrgAssets) FieldByKey(key string) *Field {
	return a.fieldsByKey[key]
}

func (a *OrgAssets) Flow(flowUUID assets.FlowUUID) (assets.Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowByUUID[flowUUID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow, nil
	}

	dbFlow, err := loadFlowByUUID(a.ctx, a.db, flowUUID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %s", flowUUID)
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

func (a *OrgAssets) FlowByID(flowID FlowID) (*Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowByID[flowID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow.(*Flow), nil
	}

	dbFlow, err := loadFlowByID(a.ctx, a.db, flowID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %d", flowID)
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

// SetFlow sets the flow definition for the passed in ID. Should only be used for unit tests
func (a *OrgAssets) SetFlow(flowID FlowID, flow flows.Flow) (*Flow, error) {
	// build our definition
	definition, err := json.Marshal(flow)
	if err != nil {
		return nil, errors.Wrapf(err, "error marshalling flow definition")
	}

	f := &Flow{}
	f.f.UUID = flow.UUID()
	f.f.Name = flow.Name()
	f.f.ID = flowID
	f.f.Definition = definition

	a.flowByID[flowID] = f
	a.flowByUUID[flow.UUID()] = f

	return f, nil
}

func (a *OrgAssets) Campaigns() []*Campaign {
	return a.campaigns
}

func (a *OrgAssets) CampaignByGroupID(groupID GroupID) []*Campaign {
	return a.campaignsByGroup[groupID]
}

func (a *OrgAssets) CampaignEventsByFieldID(fieldID FieldID) []*CampaignEvent {
	return a.campaignEventsByField[fieldID]
}

func (a *OrgAssets) CampaignEventByID(eventID CampaignEventID) *CampaignEvent {
	return a.campaignEventsByID[eventID]
}

func (a *OrgAssets) Groups() ([]assets.Group, error) {
	return a.groups, nil
}

func (a *OrgAssets) GroupByID(groupID GroupID) *Group {
	return a.groupsByID[groupID]
}

func (a *OrgAssets) GroupByUUID(groupUUID assets.GroupUUID) *Group {
	return a.groupsByUUID[groupUUID]
}

func (a *OrgAssets) Labels() ([]assets.Label, error) {
	return a.labels, nil
}

func (a *OrgAssets) Triggers() []*Trigger {
	return a.triggers
}

func (a *OrgAssets) Locations() ([]assets.LocationHierarchy, error) {
	return a.locations, nil
}

func (a *OrgAssets) Resthooks() ([]assets.Resthook, error) {
	return a.resthooks, nil
}

// GetSessionAssets returns a goflow session assets object for the parred in org assets
func GetSessionAssets(org *OrgAssets) (flows.SessionAssets, error) {
	key := fmt.Sprintf("%d", org.OrgID())
	cached, found := assetCache.Get(key)
	if found {
		return cached.(flows.SessionAssets), nil
	}

	assets, err := engine.NewSessionAssets(org)
	if err != nil {
		return nil, err
	}

	assetCache.Set(key, assets, cache.DefaultExpiration)
	return assets, nil
}
