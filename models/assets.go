package models

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/goflow"
	cache "github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

// OrgAssets is our top level cache of all things contained in an org. It is used to build
// SessionAssets for the engine but also used to cache campaigns and other org level attributes
type OrgAssets struct {
	db      *sqlx.DB
	builtAt time.Time

	orgID OrgID

	org *Org

	sessionAssets flows.SessionAssets

	flowByUUID map[assets.FlowUUID]assets.Flow

	flowByID      map[FlowID]assets.Flow
	flowCacheLock sync.RWMutex

	channels       []assets.Channel
	channelsByID   map[ChannelID]*Channel
	channelsByUUID map[assets.ChannelUUID]*Channel

	classifiers       []assets.Classifier
	classifiersByUUID map[assets.ClassifierUUID]*Classifier

	campaigns             []*Campaign
	campaignEventsByField map[FieldID][]*CampaignEvent
	campaignEventsByID    map[CampaignEventID]*CampaignEvent
	campaignsByGroup      map[GroupID][]*Campaign

	fields       []assets.Field
	fieldsByUUID map[assets.FieldUUID]*Field
	fieldsByKey  map[string]*Field

	groups       []assets.Group
	groupsByID   map[GroupID]*Group
	groupsByUUID map[assets.GroupUUID]*Group

	labels       []assets.Label
	labelsByUUID map[assets.LabelUUID]*Label

	ticketers       []assets.Ticketer
	ticketersByID   map[TicketerID]*Ticketer
	ticketersByUUID map[assets.TicketerUUID]*Ticketer

	resthooks []assets.Resthook
	templates []assets.Template
	triggers  []*Trigger
	globals   []assets.Global

	locations        []assets.LocationHierarchy
	locationsBuiltAt time.Time

	cloned bool
}

var ErrNotFound = errors.New("not found")

var orgCache = cache.New(time.Minute*15, time.Minute*15)

const cacheTimeout = time.Second * 5
const locationCacheTimeout = time.Hour

// FlushCache clears our entire org cache
func FlushCache() {
	orgCache.Flush()
}

// NewOrgAssets creates and returns a new org assets objects, potentially using the previous
// org assets passed in to prevent refetching locations
func NewOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID, prev *OrgAssets, refresh Refresh) (*OrgAssets, error) {
	// build our new assets
	o := &OrgAssets{
		db:      db,
		builtAt: time.Now(),
		orgID:   orgID,
	}

	// inherit our built at if we reusing anything
	if prev != nil && refresh&RefreshAll > 0 {
		o.builtAt = prev.builtAt
	}

	// we load everything at once except for flows which are lazily loaded
	var err error

	if prev == nil || refresh&RefreshOrg > 0 {
		o.org, err = loadOrg(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading environment for org %d", orgID)
		}
	} else {
		o.org = prev.org
	}

	if prev == nil || refresh&RefreshChannels > 0 {
		o.channels, err = loadChannels(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading channel assets for org %d", orgID)
		}
		o.channelsByID = make(map[ChannelID]*Channel)
		o.channelsByUUID = make(map[assets.ChannelUUID]*Channel)
		for _, c := range o.channels {
			channel := c.(*Channel)
			o.channelsByID[channel.ID()] = channel
			o.channelsByUUID[channel.UUID()] = channel
		}
	} else {
		o.channels = prev.channels
		o.channelsByID = prev.channelsByID
		o.channelsByUUID = prev.channelsByUUID
	}

	if prev == nil || refresh&RefreshFields > 0 {
		o.fields, err = loadFields(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading field assets for org %d", orgID)
		}
		o.fieldsByUUID = make(map[assets.FieldUUID]*Field)
		o.fieldsByKey = make(map[string]*Field)
		for _, f := range o.fields {
			field := f.(*Field)
			o.fieldsByUUID[field.UUID()] = field
			o.fieldsByKey[field.Key()] = field
		}
	} else {
		o.fields = prev.fields
		o.fieldsByUUID = prev.fieldsByUUID
		o.fieldsByKey = prev.fieldsByKey
	}

	if prev == nil || refresh&RefreshGroups > 0 {
		o.groups, err = loadGroups(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading group assets for org %d", orgID)
		}
		o.groupsByID = make(map[GroupID]*Group)
		o.groupsByUUID = make(map[assets.GroupUUID]*Group)
		for _, g := range o.groups {
			group := g.(*Group)
			o.groupsByID[group.ID()] = group
			o.groupsByUUID[group.UUID()] = group
		}
	} else {
		o.groups = prev.groups
		o.groupsByID = prev.groupsByID
		o.groupsByUUID = prev.groupsByUUID
	}

	if prev == nil || refresh&RefreshClassifiers > 0 {
		o.classifiers, err = loadClassifiers(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading classifier assets for org %d", orgID)
		}
		o.classifiersByUUID = make(map[assets.ClassifierUUID]*Classifier)
		for _, c := range o.classifiers {
			o.classifiersByUUID[c.UUID()] = c.(*Classifier)
		}
	} else {
		o.classifiers = prev.classifiers
		o.classifiersByUUID = prev.classifiersByUUID
	}

	if prev == nil || refresh&RefreshLabels > 0 {
		o.labels, err = loadLabels(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading group labels for org %d", orgID)
		}
		o.labelsByUUID = make(map[assets.LabelUUID]*Label)
		for _, l := range o.labels {
			o.labelsByUUID[l.UUID()] = l.(*Label)
		}
	} else {
		o.labels = prev.labels
		o.labelsByUUID = prev.labelsByUUID
	}

	if prev == nil || refresh&RefreshResthooks > 0 {
		o.resthooks, err = loadResthooks(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading resthooks for org %d", orgID)
		}
	} else {
		o.resthooks = prev.resthooks
	}

	if prev == nil || refresh&RefreshCampaigns > 0 {
		o.campaigns, err = loadCampaigns(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading campaigns for org %d", orgID)
		}
		o.campaignEventsByField = make(map[FieldID][]*CampaignEvent)
		o.campaignEventsByID = make(map[CampaignEventID]*CampaignEvent)
		o.campaignsByGroup = make(map[GroupID][]*Campaign)
		for _, c := range o.campaigns {
			o.campaignsByGroup[c.GroupID()] = append(o.campaignsByGroup[c.GroupID()], c)
			for _, e := range c.Events() {
				o.campaignEventsByField[e.RelativeToID()] = append(o.campaignEventsByField[e.RelativeToID()], e)
				o.campaignEventsByID[e.ID()] = e
			}
		}
	} else {
		o.campaigns = prev.campaigns
		o.campaignEventsByField = prev.campaignEventsByField
		o.campaignEventsByID = prev.campaignEventsByID
		o.campaignsByGroup = prev.campaignsByGroup
	}

	if prev == nil || refresh&RefreshTriggers > 0 {
		o.triggers, err = loadTriggers(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading triggers for org %d", orgID)
		}
	} else {
		o.triggers = prev.triggers
	}

	if prev == nil || refresh&RefreshTemplates > 0 {
		o.templates, err = loadTemplates(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading templates for org %d", orgID)
		}
	} else {
		o.templates = prev.templates
	}

	if prev == nil || refresh&RefreshGlobals > 0 {
		o.globals, err = loadGlobals(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading globals for org %d", orgID)
		}
	} else {
		o.globals = prev.globals
	}

	if prev == nil || refresh&RefreshLocations > 0 {
		o.locations, err = loadLocations(ctx, db, orgID)
		o.locationsBuiltAt = time.Now()
		if err != nil {
			return nil, errors.Wrapf(err, "error loading group locations for org %d", orgID)
		}
	} else {
		o.locations = prev.locations
		o.locationsBuiltAt = prev.locationsBuiltAt
	}

	if prev == nil || refresh&RefreshFlows > 0 {
		o.flowByUUID = make(map[assets.FlowUUID]assets.Flow)
		o.flowByID = make(map[FlowID]assets.Flow)
	} else {
		o.flowByUUID = prev.flowByUUID
		o.flowByID = prev.flowByID
	}

	if prev == nil || refresh&RefreshTicketers > 0 {
		o.ticketers, err = loadTicketers(ctx, db, orgID)
		if err != nil {
			return nil, errors.Wrapf(err, "error loading ticketer assets for org %d", orgID)
		}
		o.ticketersByID = make(map[TicketerID]*Ticketer)
		o.ticketersByUUID = make(map[assets.TicketerUUID]*Ticketer)
		for _, t := range o.ticketers {
			o.ticketersByID[t.(*Ticketer).ID()] = t.(*Ticketer)
			o.ticketersByUUID[t.UUID()] = t.(*Ticketer)
		}
	} else {
		o.ticketers = prev.ticketers
		o.ticketersByUUID = prev.ticketersByUUID
	}

	// intialize our session assets
	o.sessionAssets, err = engine.NewSessionAssets(o.Env(), o, goflow.MigrationConfig())
	if err != nil {
		return nil, errors.Wrapf(err, "error build session assets for org: %d", orgID)
	}

	return o, nil
}

// Refresh is our type for the pieces of org assets we want fresh (not cached)
type Refresh int

const (
	RefreshNone        = Refresh(0)
	RefreshAll         = Refresh(^0)
	RefreshOrg         = Refresh(1 << 1)
	RefreshChannels    = Refresh(1 << 2)
	RefreshFields      = Refresh(1 << 3)
	RefreshGroups      = Refresh(1 << 4)
	RefreshLocations   = Refresh(1 << 5)
	RefreshGlobals     = Refresh(1 << 6)
	RefreshTemplates   = Refresh(1 << 7)
	RefreshTriggers    = Refresh(1 << 8)
	RefreshCampaigns   = Refresh(1 << 9)
	RefreshResthooks   = Refresh(1 << 10)
	RefreshClassifiers = Refresh(1 << 11)
	RefreshLabels      = Refresh(1 << 12)
	RefreshFlows       = Refresh(1 << 13)
	RefreshTicketers   = Refresh(1 << 14)
)

// GetOrgAssets creates or gets org assets for the passed in org
func GetOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) (*OrgAssets, error) {
	return GetOrgAssetsWithRefresh(ctx, db, orgID, RefreshNone)
}

// GetOrgAssetsWithRefresh creates or gets org assets for the passed in org refreshing the passed in assets
func GetOrgAssetsWithRefresh(ctx context.Context, db *sqlx.DB, orgID OrgID, refresh Refresh) (*OrgAssets, error) {
	if db == nil {
		return nil, errors.Errorf("nil db, cannot load org")
	}

	// do we have a recent cache?
	key := fmt.Sprintf("%d", orgID)
	var cached *OrgAssets
	c, found := orgCache.Get(key)
	if found {
		cached = c.(*OrgAssets)
	}

	if found {
		// we found assets to use, but they are stale, refresh everything but locations
		if time.Since(cached.builtAt) > cacheTimeout {
			refresh = ^RefreshLocations
		}

		// our locations are stale, refresh those
		if time.Since(cached.locationsBuiltAt) > locationCacheTimeout {
			refresh = refresh | RefreshLocations
		}
	}

	// if found and nothing to refresh, return it
	if found && refresh == RefreshNone {
		return cached, nil
	}

	// otherwise build our new assets
	o, err := NewOrgAssets(ctx, db, orgID, cached, refresh)
	if err != nil {
		return nil, err
	}

	orgCache.SetDefault(key, o)

	// return our assets
	return o, nil
}

func (a *OrgAssets) OrgID() OrgID { return a.orgID }

func (a *OrgAssets) Env() envs.Environment { return a.org }

func (a *OrgAssets) Org() *Org { return a.org }

func (a *OrgAssets) SessionAssets() flows.SessionAssets { return a.sessionAssets }

func (a *OrgAssets) Channels() ([]assets.Channel, error) {
	return a.channels, nil
}

func (a *OrgAssets) ChannelByUUID(channelUUID assets.ChannelUUID) *Channel {
	return a.channelsByUUID[channelUUID]
}

func (a *OrgAssets) ChannelByID(channelID ChannelID) *Channel {
	return a.channelsByID[channelID]
}

func (a *OrgAssets) Classifiers() ([]assets.Classifier, error) {
	return a.classifiers, nil
}

func (a *OrgAssets) ClassifierByUUID(classifierUUID assets.ClassifierUUID) *Classifier {
	return a.classifiersByUUID[classifierUUID]
}

func (a *OrgAssets) Fields() ([]assets.Field, error) {
	return a.fields, nil
}

func (a *OrgAssets) FieldByUUID(fieldUUID assets.FieldUUID) *Field {
	return a.fieldsByUUID[fieldUUID]
}

func (a *OrgAssets) FieldByKey(key string) *Field {
	return a.fieldsByKey[key]
}

// Clone clones our org assets, returning a copy that can be modified without affecting the main
func (a *OrgAssets) Clone(ctx context.Context, db *sqlx.DB) (*OrgAssets, error) {
	// only channels and flows can be modified so only refresh those
	org, err := NewOrgAssets(context.Background(), a.db, a.OrgID(), a, RefreshFlows|RefreshChannels)
	org.cloned = true

	// rebuild our session assets with our new items
	org.sessionAssets, err = engine.NewSessionAssets(a.Env(), org, goflow.MigrationConfig())
	if err != nil {
		return nil, errors.Wrapf(err, "error build session assets for org: %d", org.OrgID())
	}

	return org, err
}

// AddTestChannel adds a test channel to our org, this is only used in session assets during simulation
func (a *OrgAssets) AddTestChannel(channel assets.Channel) {
	if !a.cloned {
		panic("can only add test channels to cloned orgs")
	}

	a.channels = append(a.channels, channel)
	a.sessionAssets, _ = engine.NewSessionAssets(a.Env(), a, goflow.MigrationConfig())

	// we don't populate our maps for uuid or id, shouldn't be used in any hook anyways
}

// Flow returns the flow with the passed in UUID
func (a *OrgAssets) Flow(flowUUID assets.FlowUUID) (assets.Flow, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	a.flowCacheLock.RLock()
	flow, found := a.flowByUUID[flowUUID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow, nil
	}

	dbFlow, err := loadFlowByUUID(ctx, a.db, a.orgID, flowUUID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %s", flowUUID)
	}

	if dbFlow == nil {
		return nil, ErrNotFound
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

// FlowByID returns the flow with the passed in ID
func (a *OrgAssets) FlowByID(flowID FlowID) (*Flow, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	a.flowCacheLock.RLock()
	flow, found := a.flowByID[flowID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow.(*Flow), nil
	}

	dbFlow, err := loadFlowByID(ctx, a.db, a.orgID, flowID)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading flow: %d", flowID)
	}

	if dbFlow == nil {
		return nil, ErrNotFound
	}

	a.flowCacheLock.Lock()
	a.flowByID[dbFlow.ID()] = dbFlow
	a.flowByUUID[dbFlow.UUID()] = dbFlow
	a.flowCacheLock.Unlock()

	return dbFlow, nil
}

// SetFlow sets the flow definition for the passed in ID. Should only be used for unit tests
func (a *OrgAssets) SetFlow(id FlowID, uuid assets.FlowUUID, name string, definition json.RawMessage) *Flow {
	if !a.cloned {
		panic("can only override flow definitions on cloned orgs")
	}

	f := &Flow{}
	f.f.UUID = uuid
	f.f.Name = name
	f.f.ID = id
	f.f.Definition = definition

	a.flowByID[id] = f
	a.flowByUUID[uuid] = f

	return f
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

func (a *OrgAssets) LabelByUUID(uuid assets.LabelUUID) *Label {
	return a.labelsByUUID[uuid]
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

func (a *OrgAssets) ResthookBySlug(slug string) *Resthook {
	for _, r := range a.resthooks {
		if r.Slug() == slug {
			return r.(*Resthook)
		}
	}
	return nil
}

func (a *OrgAssets) Templates() ([]assets.Template, error) {
	return a.templates, nil
}

func (a *OrgAssets) Globals() ([]assets.Global, error) {
	return a.globals, nil
}

func (a *OrgAssets) Ticketers() ([]assets.Ticketer, error) {
	return a.ticketers, nil
}

func (a *OrgAssets) TicketerByID(id TicketerID) *Ticketer {
	return a.ticketersByID[id]
}

func (a *OrgAssets) TicketerByUUID(uuid assets.TicketerUUID) *Ticketer {
	return a.ticketersByUUID[uuid]
}
