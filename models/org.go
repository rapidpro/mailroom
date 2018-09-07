package models

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/goflow/legacy"
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

	channels     *flows.ChannelSet
	channelIDs   map[flows.ChannelUUID]ChannelID
	channelsLock sync.RWMutex

	fields     *flows.FieldSet
	fieldsLock sync.RWMutex

	groups     *flows.GroupSet
	groupsLock sync.RWMutex

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

		channelIDs: make(map[flows.ChannelUUID]ChannelID),

		flows:   make(map[flows.FlowUUID]flows.Flow),
		flowIDs: make(map[flows.FlowUUID]FlowID),
	}
}

func (o *OrgAssets) GetOrgID() OrgID {
	return o.orgID
}

func (o *OrgAssets) GetChannelID(uuid flows.ChannelUUID) (ChannelID, error) {
	_, err := o.GetChannelSet()
	if err != nil {
		return ChannelID(-1), err
	}
	id, found := o.channelIDs[uuid]
	if !found {
		return ChannelID(-1), fmt.Errorf("no channel found with UUID: %s", uuid)
	}
	return id, nil
}

func (o *OrgAssets) GetChannel(uuid flows.ChannelUUID) (flows.Channel, error) {
	cs, err := o.GetChannelSet()
	if err != nil {
		return nil, err
	}

	return cs.FindByUUID(uuid), nil
}

func (o *OrgAssets) GetChannelSet() (*flows.ChannelSet, error) {
	if o.channels == nil {
		err := o.loadChannels()
		if err != nil {
			return nil, err
		}
	}
	return o.channels, nil
}

func (o *OrgAssets) GetField(key string) (*flows.Field, error) {
	fs, err := o.GetFieldSet()
	if err != nil {
		return nil, err
	}
	return fs.FindByKey(key), nil
}

func (o *OrgAssets) GetFieldSet() (*flows.FieldSet, error) {
	if o.fields == nil {
		err := o.loadFields()
		if err != nil {
			return nil, err
		}
	}
	return o.fields, nil
}

func (o *OrgAssets) GetFlow(uuid flows.FlowUUID) (flows.Flow, error) {
	o.flowsLock.RLock()
	flow, found := o.flows[uuid]
	o.flowsLock.RUnlock()
	if found {
		return flow, nil
	}

	flow, flowID, err := o.loadFlow(uuid)
	if err != nil {
		return nil, err
	}
	o.flowsLock.Lock()
	o.flows[uuid] = flow
	o.flowIDs[uuid] = flowID
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

func (o *OrgAssets) GetGroup(uuid flows.GroupUUID) (*flows.Group, error) {
	gs, err := o.GetGroupSet()
	if err != nil {
		return nil, err
	}
	return gs.FindByUUID(uuid), nil
}

func (o *OrgAssets) GetGroupSet() (*flows.GroupSet, error) {
	if o.groups == nil {
		err := o.loadGroups()
		if err != nil {
			return nil, err
		}
	}
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
	if o.labels == nil {
		err := o.loadLabels()
		if err != nil {
			return nil, err
		}
	}
	return o.labels, nil
}

func (o *OrgAssets) HasLocations() bool {
	return false
}

func (o *OrgAssets) GetLocationHierarchySet() (*flows.LocationHierarchySet, error) {
	return nil, nil
}

func (o *OrgAssets) GetResthookSet() (*flows.ResthookSet, error) {
	if o.resthooks == nil {
		err := o.loadResthooks()
		if err != nil {
			return nil, err
		}
	}
	return o.resthooks, nil
}

const selectActiveTopup = `
SELECT 
	t.id as id
FROM 
	orgs_topup t
	LEFT OUTER JOIN orgs_topupcredits tc ON (t.id = tc.topup_id) 
WHERE 
	t.org_id = $1 AND
	t.expires_on >= NOW() AND
	t.is_active = TRUE AND
	t.credits > 0
GROUP BY 
	t.id 
HAVING 
	SUM(tc.used) < (t.credits) OR 
	SUM(tc.used) IS NULL 
ORDER BY 
	t.expires_on ASC, t.id ASC
LIMIT 1
`

// TODO: does this need to be optimized further?
func (o *OrgAssets) GetActiveTopup() (TopUpID, error) {
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	var topupID TopUpID
	err := o.db.GetContext(ctx, &topupID, selectActiveTopup, o.orgID)
	return topupID, err
}

const selectChannelsSQL = `
SELECT JSON_AGG(ROW_TO_JSON(c)) FROM
(SELECT
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
FROM channels_channel WHERE org_id = $1 AND is_active = TRUE) c;
`

// grabs a lock on our mutex and loads the channels for this org from the db
func (o *OrgAssets) loadChannels() error {
	o.channelsLock.Lock()
	defer o.channelsLock.Unlock()

	// loading channels shouldn't take any time
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	// somebody loaded the channels while we were getting our lock
	if o.channels != nil {
		return nil
	}

	var channelJSON string
	err := o.db.GetContext(ctx, &channelJSON, selectChannelsSQL, o.orgID)
	if err != nil {
		return err
	}

	// load it in from our json
	set, err := flows.ReadChannelSet([]byte(channelJSON))
	if err != nil {
		return err
	}
	o.channels = set

	// also populate our mapping from UUID -> ID
	_, err = jsonparser.ArrayEach([]byte(channelJSON), func(value []byte, dataType jsonparser.ValueType, offset int, eachErr error) {
		if err != nil {
			return
		}
		if eachErr != nil {
			err = eachErr
			return
		}

		var uuid string
		uuid, err = jsonparser.GetString(value, "uuid")
		if err != nil {
			return
		}

		var id int64
		id, err = jsonparser.GetInt(value, "id")
		if err != nil {
			return
		}
		o.channelIDs[flows.ChannelUUID(uuid)] = ChannelID(id)
	})

	return err
}

const selectFieldsSQL = `
SELECT JSON_AGG(ROW_TO_JSON(r)) FROM 
(SELECT
id,
uuid,
key,
label AS name,
value_type
FROM contacts_contactfield WHERE org_id = $1 AND is_active = TRUE) r;`

// grabs a lock on our mutex and loads the fields for this org from the db
func (o *OrgAssets) loadFields() error {
	o.fieldsLock.Lock()
	defer o.fieldsLock.Unlock()

	// loading fields shouldn't take any time
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	// somebody loaded the fields while we were getting our lock
	if o.fields != nil {
		return nil
	}

	var fieldsJSON string
	err := o.db.GetContext(ctx, &fieldsJSON, selectFieldsSQL, o.orgID)
	if err != nil {
		return err
	}

	// load it in from our json
	set, err := flows.ReadFieldSet([]byte(fieldsJSON))
	if err != nil {
		return err
	}
	o.fields = set
	return nil
}

const selectGroupsSQL = `
SELECT JSON_AGG(ROW_TO_JSON(r)) FROM 
(SELECT
id,
uuid,
name,
query
FROM contacts_contactgroup WHERE org_id = $1 AND is_active = TRUE
ORDER BY name ASC) r;`

// grabs a lock on our mutex and loads the groups for this org from the db
func (o *OrgAssets) loadGroups() error {
	o.groupsLock.Lock()
	defer o.groupsLock.Unlock()

	// loading groups shouldn't take any time
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	// somebody loaded the groups while we were getting our lock
	if o.fields != nil {
		return nil
	}

	var groupsJSON string
	err := o.db.GetContext(ctx, &groupsJSON, selectGroupsSQL, o.orgID)
	if err != nil {
		return err
	}

	// load it in from our json
	set, err := flows.ReadGroupSet([]byte(groupsJSON))
	if err != nil {
		logrus.WithError(err).Error("error loading group")
		return err
	}
	o.groups = set
	return nil
}

const selectLabelsSQL = `
SELECT JSON_AGG(ROW_TO_JSON(r)) FROM 
(SELECT
id,
uuid,
name
FROM msgs_label WHERE org_id = $1 AND is_active = TRUE
ORDER BY name ASC) r;`

// grabs a lock on our mutex and loads the labels for this org from the db
func (o *OrgAssets) loadLabels() error {
	o.labelsLock.Lock()
	defer o.labelsLock.Unlock()

	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	if o.labels != nil {
		return nil
	}

	var labelsJSON string
	err := o.db.GetContext(ctx, &labelsJSON, selectLabelsSQL, o.orgID)
	if err != nil {
		return err
	}

	// load it in from our json
	set, err := flows.ReadLabelSet([]byte(labelsJSON))
	if err != nil {
		return err
	}
	o.labels = set
	return nil
}

const selectResthooksSQL = `
SELECT JSON_AGG(ROW_TO_JSON(r)) FROM 
(SELECT
id,
slug,
subscribers 
FROM api_resthook r 
JOIN LATERAL (
  SELECT JSON_AGG(rs.target_url) AS subscribers FROM api_resthooksubscriber rs WHERE r.id = rs.resthook_id AND rs.is_active = TRUE
) AS subscribers ON True
WHERE r.org_id = $1 AND r.is_active = TRUE) as r;`

// grabs a lock on our mutex and loads the resthooks for this org from the db
func (o *OrgAssets) loadResthooks() error {
	o.resthooksLock.Lock()
	defer o.resthooksLock.Unlock()

	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	if o.resthooks != nil {
		return nil
	}

	var resthooksJSON string
	err := o.db.GetContext(ctx, &resthooksJSON, selectResthooksSQL, o.orgID)
	if err != nil {
		return err
	}

	// load it in from our json
	set, err := flows.ReadResthookSet([]byte(resthooksJSON))
	if err != nil {
		return err
	}
	o.resthooks = set
	return nil
}

const selectFlowSQL = `
SELECT 
	fr.definition::jsonb || 
	jsonb_build_object(
		'flow_type', f.flow_type, 
		'metadata', jsonb_build_object(
			'uuid', f.uuid, 
			'name', f.name, 
			'revision', fr.revision, 
			'expires', f.expires_after_minutes
		)
	) as definition, f.id as id
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

type FlowDefinition struct {
	Definition string `db:"definition"`
	ID         FlowID `db:"id"`
}

// loads the flow with the passed in UUID
func (o *OrgAssets) loadFlow(uuid flows.FlowUUID) (flows.Flow, FlowID, error) {
	ctx, cancel := context.WithTimeout(o.ctx, time.Second*15)
	defer cancel()

	var definition FlowDefinition
	err := o.db.GetContext(ctx, &definition, selectFlowSQL, uuid)
	if err != nil {
		return nil, -1, err
	}

	// load it in from our json
	legacyFlow, err := legacy.ReadLegacyFlow([]byte(definition.Definition))
	if err != nil {
		logrus.WithField("definition", definition.Definition).WithError(err).Error("error loading flow")
		return nil, -1, err
	}

	// migrate forwards returning our final flow definition
	flow, err := legacyFlow.Migrate(false, false)
	return flow, definition.ID, err
}
