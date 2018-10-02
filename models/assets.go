package models

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/utils"
)

type OrgAssets struct {
	ctx     context.Context
	db      *sqlx.DB
	builtAt time.Time

	orgID OrgID

	env utils.Environment

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

	locations        []assets.LocationHierarchy
	locationsBuiltAt time.Time
}

var sourceCache = make(map[OrgID]*OrgAssets)
var sourceCacheLock = sync.RWMutex{}

const cacheTimeout = time.Second * 5
const locationCacheTimeout = time.Hour

// GetOrgAssets creates or gets org assets for the passed in org
func GetOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) (*OrgAssets, error) {
	// do we have a recent cache?
	sourceCacheLock.RLock()
	cached, found := sourceCache[orgID]
	sourceCacheLock.RUnlock()

	// if we found a source built in the last five seconds, use it
	if found && time.Since(cached.builtAt) < cacheTimeout {
		return cached, nil
	}

	// otherwire, we build one from scratch
	a := &OrgAssets{
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

	a.env, err = loadOrg(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading environment for org %d", orgID)
	}

	a.channels, err = loadChannels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading channel assets for org %d", orgID)
	}
	for _, c := range a.channels {
		channel := c.(*Channel)
		a.channelsByID[channel.ID()] = channel
		a.channelsByUUID[channel.UUID()] = channel
	}

	a.fields, err = loadFields(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading field assets for org %d", orgID)
	}
	for _, f := range a.fields {
		field := f.(*Field)
		a.fieldsByUUID[field.UUID()] = field
		a.fieldsByKey[field.Key()] = field
	}

	a.groups, err = loadGroups(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading group assets for org %d", orgID)
	}
	for _, g := range a.groups {
		group := g.(*Group)
		a.groupsByID[group.ID()] = group
		a.groupsByUUID[group.UUID()] = group
	}

	a.labels, err = loadLabels(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading group labels for org %d", orgID)
	}

	a.resthooks, err = loadResthooks(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading resthooks for org %d", orgID)
	}

	a.campaigns, err = loadCampaigns(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading campaigns for org %d", orgID)
	}
	for _, c := range a.campaigns {
		a.campaignsByGroup[c.GroupID()] = append(a.campaignsByGroup[c.GroupID()], c)
		for _, e := range c.Events() {
			a.campaignEventsByField[e.RelativeToID()] = append(a.campaignEventsByField[e.RelativeToID()], e)
			a.campaignEventsByID[e.ID()] = e
		}
	}

	// cache locations for an hour
	if cached != nil && time.Since(cached.locationsBuiltAt) < locationCacheTimeout {
		a.locations = cached.locations
		a.locationsBuiltAt = cached.locationsBuiltAt
	} else {
		a.locations, err = loadLocations(ctx, db, orgID)
		a.locationsBuiltAt = time.Now()
		if err != nil {
			return nil, errors.Annotatef(err, "error loading group locations for org %d", orgID)
		}
	}

	sourceCacheLock.Lock()
	sourceCache[orgID] = a
	sourceCacheLock.Unlock()

	return a, nil
}

func (a *OrgAssets) OrgID() OrgID { return a.orgID }

func (a *OrgAssets) Env() utils.Environment { return a.env }

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
		return nil, errors.Annotatef(err, "error loading flow: %s", flowUUID)
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

func (a *OrgAssets) FlowByID(flowID FlowID) (assets.Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowByID[flowID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow, nil
	}

	dbFlow, err := loadFlowByID(a.ctx, a.db, flowID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading flow: %d", flowID)
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
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

func (a *OrgAssets) Locations() ([]assets.LocationHierarchy, error) {
	return a.locations, nil
}

func (a *OrgAssets) Resthooks() ([]assets.Resthook, error) {
	return a.resthooks, nil
}
