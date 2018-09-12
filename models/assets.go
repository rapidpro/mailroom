package models

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nyaruka/goflow/legacy"
	"github.com/nyaruka/goflow/utils"
	"github.com/sirupsen/logrus"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
)

type OrgID int

// OrgAssets is the set of assets for an organization. These are loaded lazily from the database as asked for.
// OrgAssets are thread safe and can be shared across goroutines.
// OrgAssets implement the flows.SessionAssets interface.
type OrgAssets struct {
	ctx   context.Context
	db    *sqlx.DB
	orgID OrgID
	env   utils.Environment

	channels     *flows.ChannelSet
	channelsByID map[flows.ChannelID]flows.Channel
	channelOnce  sync.Once

	fields       *flows.FieldSet
	fieldsByUUID map[FieldUUID]*flows.Field
	fieldsLock   sync.RWMutex

	groups     *flows.GroupSet
	groupsByID map[flows.GroupID]*flows.Group
	groupOnce  sync.Once

	labels     *flows.LabelSet
	labelsLock sync.RWMutex

	resthooks     *flows.ResthookSet
	resthooksLock sync.RWMutex

	flows     map[flows.FlowUUID]flows.Flow
	flowIDs   map[flows.FlowUUID]FlowID
	flowsLock sync.RWMutex

	// TODO: implement locations
	locations     *flows.LocationHierarchySet
	locationsLock sync.RWMutex
}

func NewOrgAssets(ctx context.Context, db *sqlx.DB, orgID OrgID) *OrgAssets {
	return &OrgAssets{
		ctx:   ctx,
		db:    db,
		orgID: orgID,
		env:   utils.NewDefaultEnvironment(),

		flows: make(map[flows.FlowUUID]flows.Flow),
	}
}

func (o *OrgAssets) GetOrgID() OrgID {
	return o.orgID
}

func (o *OrgAssets) GetChannelByID(id flows.ChannelID) (flows.Channel, error) {
	_, err := o.GetChannelSet()
	if err != nil {
		return nil, err
	}
	channel, found := o.channelsByID[id]
	if !found {
		return nil, fmt.Errorf("no channel found with ID: %d", id)
	}
	return channel, nil
}

func (o *OrgAssets) GetChannel(uuid flows.ChannelUUID) (flows.Channel, error) {
	cs, err := o.GetChannelSet()
	if err != nil {
		return nil, err
	}

	return cs.FindByUUID(uuid), nil
}

func (o *OrgAssets) GetChannelSet() (*flows.ChannelSet, error) {
	return o.channels, nil
}

func (o *OrgAssets) GetFieldByUUID(uuid FieldUUID) (*flows.Field, error) {
	_, err := o.GetFieldSet()
	if err != nil {
		return nil, err
	}
	field, found := o.fieldsByUUID[uuid]
	if !found {
		return nil, nil
	}

	return field, nil
}

func (o *OrgAssets) GetField(key string) (*flows.Field, error) {
	fs, err := o.GetFieldSet()
	if err != nil {
		return nil, err
	}
	return fs.FindByKey(key), nil
}

func (o *OrgAssets) GetFieldSet() (*flows.FieldSet, error) {
	return o.fields, nil
}

func (o *OrgAssets) GetFlow(uuid flows.FlowUUID) (flows.Flow, error) {
	o.flowsLock.RLock()
	flow, found := o.flows[uuid]
	o.flowsLock.RUnlock()
	if found {
		return flow, nil
	}

	flow, err := o.loadFlow(uuid)
	if err != nil {
		return nil, err
	}
	o.flowsLock.Lock()
	o.flows[uuid] = flow
	o.flowsLock.Unlock()

	return flow, nil
}

func (o *OrgAssets) GetFlowID(uuid flows.FlowUUID) (FlowID, error) {
	o.flowsLock.RLock()
	flowID, found := o.flowIDs[uuid]
	o.flowsLock.RUnlock()
	if !found {
		return -1, fmt.Errorf("no flow known with uuid: %s", uuid)
	}
	return flowID, nil
}

func (o *OrgAssets) GetGroupByID(id flows.GroupID) (*flows.Group, error) {
	_, err := o.GetGroupSet()
	if err != nil {
		return nil, err
	}
	group, found := o.groupsByID[id]
	if !found {
		return nil, fmt.Errorf("no group found with id: %d", id)
	}
	return group, nil
}

func (o *OrgAssets) GetGroup(uuid flows.GroupUUID) (*flows.Group, error) {
	gs, err := o.GetGroupSet()
	if err != nil {
		return nil, err
	}
	return gs.FindByUUID(uuid), nil
}

func (o *OrgAssets) GetGroupSet() (*flows.GroupSet, error) {
	return o.groups, nil
}

func (o *OrgAssets) GetLabel(uuid flows.LabelUUID) (*flows.Label, error) {
	ls, err := o.GetLabelSet()
	if err != nil {
		return nil, err
	}
	return ls.FindByUUID(uuid), nil
}

func (o *OrgAssets) GetLabelSet() (*flows.LabelSet, error) {
	return o.labels, nil
}

func (o *OrgAssets) HasLocations() bool {
	return false
}

func (o *OrgAssets) GetLocationHierarchySet() (*flows.LocationHierarchySet, error) {
	return nil, nil
}

func (o *OrgAssets) GetResthookSet() (*flows.ResthookSet, error) {
	return o.resthooks, nil
}

const selectFlowSQL = `
SELECT 
	fr.definition::jsonb || 
	jsonb_build_object(
		'flow_type', f.flow_type, 
		'metadata', jsonb_build_object(
			'uuid', f.uuid, 
			'id', f.id,
			'name', f.name, 
			'revision', fr.revision, 
			'expires', f.expires_after_minutes
		)
	) as definition
FROM 
	flows_flowrevision fr, 
	flows_flow f 
WHERE 
	f.uuid = $1 AND 
	fr.flow_id = f.id AND 
	fr.is_active = TRUE AND
	f.is_active = TRUE 
ORDER BY 
	revision DESC LIMIT 1;`

// loads the flow with the passed in UUID
func (o *OrgAssets) loadFlow(uuid flows.FlowUUID) (flows.Flow, error) {
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	var definition string
	err := o.db.GetContext(ctx, &definition, selectFlowSQL, uuid)
	if err != nil {
		return nil, err
	}

	// load it in from our json
	legacyFlow, err := legacy.ReadLegacyFlow([]byte(definition))
	if err != nil {
		logrus.WithField("definition", definition).WithError(err).Error("error loading flow")
		return nil, err
	}

	// migrate forwards returning our final flow definition
	flow, err := legacyFlow.Migrate(false, false)
	return flow, err
}
