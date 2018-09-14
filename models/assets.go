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

	flowCache     map[assets.FlowUUID]assets.Flow
	flowCacheLock sync.RWMutex

	channels       []assets.Channel
	channelsByID   map[ChannelID]*Channel
	channelsByUUID map[assets.ChannelUUID]*Channel

	fields       []assets.Field
	fieldsByUUID map[FieldUUID]*Field

	groups       []assets.Group
	groupsByID   map[GroupID]*Group
	groupsByUUID map[assets.GroupUUID]*Group

	labels    []assets.Label
	locations []assets.LocationHierarchy
	resthooks []assets.Resthook
}

var sourceCache = make(map[OrgID]*OrgAssets)
var sourceCacheLock = sync.RWMutex{}

func NewOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) (*OrgAssets, error) {
	// do we have a recent cache?
	sourceCacheLock.RLock()
	cached, found := sourceCache[orgID]
	sourceCacheLock.RUnlock()

	// if we found a source built in the last five seconds, use it
	if found && time.Since(cached.builtAt) < time.Second*5 {
		return cached, nil
	}

	// otherwire, we build one from scratch
	a := &OrgAssets{
		ctx:     ctx,
		db:      db,
		builtAt: time.Now(),

		channelsByID:   make(map[ChannelID]*Channel),
		channelsByUUID: make(map[assets.ChannelUUID]*Channel),

		fieldsByUUID: make(map[FieldUUID]*Field),

		groupsByID:   make(map[GroupID]*Group),
		groupsByUUID: make(map[assets.GroupUUID]*Group),

		flowCache: make(map[assets.FlowUUID]assets.Flow),
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

	a.locations, err = loadLocations(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading group locations for org %d", orgID)
	}

	a.resthooks, err = loadResthooks(ctx, db, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading resthooks for org %d", orgID)
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

func (a *OrgAssets) Flow(flowUUID assets.FlowUUID) (assets.Flow, error) {
	a.flowCacheLock.RLock()
	flow, found := a.flowCache[flowUUID]
	a.flowCacheLock.RUnlock()

	if found {
		return flow, nil
	}

	flow, err := loadFlow(a.ctx, a.db, flowUUID)
	if err != nil {
		return nil, errors.Annotatef(err, "error loading flow: %s", flowUUID)
	}

	a.flowCacheLock.Lock()
	a.flowCache[flowUUID] = flow
	a.flowCacheLock.Unlock()

	return flow, nil
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
