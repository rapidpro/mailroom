package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/utils/dbutil"
	"github.com/nyaruka/null"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// FlowID is the type for flow IDs
type FlowID null.Int

// NilFlowID is nil value for flow IDs
const NilFlowID = FlowID(0)

// FlowType is the type for the type of a flow
type FlowType string

// flow type constants
const (
	IVRFlow       = FlowType("V")
	MessagingFlow = FlowType("M")
	SurveyorFlow  = FlowType("S")
)

const (
	flowConfigIVRRetryMinutes = "ivr_retry"
)

var flowTypeMapping = map[flows.FlowType]FlowType{
	flows.FlowTypeMessaging:        MessagingFlow,
	flows.FlowTypeVoice:            IVRFlow,
	flows.FlowTypeMessagingOffline: SurveyorFlow,
}

// Flow is the mailroom type for a flow
type Flow struct {
	f struct {
		ID             FlowID          `json:"id"`
		UUID           assets.FlowUUID `json:"uuid"`
		Name           string          `json:"name"`
		Config         null.Map        `json:"config"`
		Version        string          `json:"version"`
		FlowType       FlowType        `json:"flow_type"`
		Definition     json.RawMessage `json:"definition"`
		IgnoreTriggers bool            `json:"ignore_triggers"`
	}
}

// ID returns the ID for this flow
func (f *Flow) ID() FlowID { return f.f.ID }

// UUID returns the UUID for this flow
func (f *Flow) UUID() assets.FlowUUID { return f.f.UUID }

// Name returns the name of this flow
func (f *Flow) Name() string { return f.f.Name }

// Definition returns the definition for this flow
func (f *Flow) Definition() json.RawMessage { return f.f.Definition }

// FlowType return the type of flow this is
func (f *Flow) FlowType() FlowType { return f.f.FlowType }

// Version returns the version this flow was authored in
func (f *Flow) Version() string { return f.f.Version }

// IVRRetryWait returns the wait before retrying a failed IVR call
func (f *Flow) IVRRetryWait() time.Duration {
	value := f.f.Config.Get(flowConfigIVRRetryMinutes, nil)
	fv, isFloat := value.(float64)
	if isFloat {
		return time.Minute * time.Duration(int(fv))
	}
	return ConnectionRetryWait
}

// IgnoreTriggers returns whether this flow ignores triggers
func (f *Flow) IgnoreTriggers() bool { return f.f.IgnoreTriggers }

// FlowReference return a flow reference for this flow
func (f *Flow) FlowReference() *assets.FlowReference {
	return assets.NewFlowReference(f.UUID(), f.Name())
}

func flowIDForUUID(ctx context.Context, tx *sqlx.Tx, oa *OrgAssets, flowUUID assets.FlowUUID) (FlowID, error) {
	// first try to look up in our assets
	flow, _ := oa.Flow(flowUUID)
	if flow != nil {
		return flow.(*Flow).ID(), nil
	}

	// flow may be inactive, try to look up the ID only
	var flowID FlowID
	err := tx.GetContext(ctx, &flowID, `SELECT id FROM flows_flow WHERE org_id = $1 AND uuid = $2;`, oa.OrgID(), flowUUID)
	return flowID, err
}

func loadFlowByUUID(ctx context.Context, db *sqlx.DB, orgID OrgID, flowUUID assets.FlowUUID) (*Flow, error) {
	return loadFlow(ctx, db, selectFlowByUUIDSQL, orgID, flowUUID)
}

func loadFlowByID(ctx context.Context, db *sqlx.DB, orgID OrgID, flowID FlowID) (*Flow, error) {
	return loadFlow(ctx, db, selectFlowByIDSQL, orgID, flowID)
}

// loads the flow with the passed in UUID
func loadFlow(ctx context.Context, db *sqlx.DB, sql string, orgID OrgID, arg interface{}) (*Flow, error) {
	start := time.Now()
	flow := &Flow{}

	rows, err := db.Queryx(sql, orgID, arg)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying flow by: %s", arg)
	}
	defer rows.Close()

	// no row, no flow!
	if !rows.Next() {
		return nil, nil
	}

	err = dbutil.ReadJSONRow(rows, &flow.f)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading flow definition by: %s", arg)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("flow", arg).Debug("loaded flow")

	return flow, nil
}

const selectFlowByUUIDSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id, 
	uuid, 
	name,
	ignore_triggers,
	flow_type,
	fr.spec_version as version,
	coalesce(metadata, '{}')::jsonb as config,
	definition::jsonb || 
		jsonb_build_object(
			'name', f.name,
			'uuid', f.uuid,
			'flow_type', f.flow_type, 
			'expire_after_minutes', f.expires_after_minutes,
			'metadata', jsonb_build_object(
				'uuid', f.uuid, 
				'id', f.id,
				'name', f.name,
				'revision', revision, 
				'expires', f.expires_after_minutes
			)
	) as definition
FROM
	flows_flow f
LEFT JOIN (
	SELECT 
		flow_id,
		spec_version, 
		definition, 
		revision
	FROM 
		flows_flowrevision
	WHERE
		flow_id = ANY(SELECT id FROM flows_flow WHERE uuid = $2) AND
		is_active = TRUE
	ORDER BY 
		revision DESC
	LIMIT 1
) fr ON fr.flow_id = f.id
WHERE
    org_id = $1 AND
	uuid = $2 AND
	is_active = TRUE AND
	is_archived = FALSE
) r;`

const selectFlowByIDSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	id, 
	uuid, 
	name,
	ignore_triggers,
	flow_type,
	fr.spec_version as version,
	coalesce(metadata, '{}')::jsonb as config,
	definition::jsonb || 
		jsonb_build_object(
			'name', f.name,
			'uuid', f.uuid,
			'flow_type', f.flow_type, 
			'expire_after_minutes', f.expires_after_minutes,
			'metadata', jsonb_build_object(
				'uuid', f.uuid, 
				'id', f.id,
				'name', f.name,
				'revision', revision, 
				'expires', f.expires_after_minutes
			)
	) as definition
FROM
	flows_flow f
LEFT JOIN (
	SELECT 
		flow_id, 
		spec_version,
		definition, 
		revision
	FROM 
		flows_flowrevision
	WHERE
		flow_id = $2 AND
		is_active = TRUE
	ORDER BY 
		revision DESC
	LIMIT 1
) fr ON fr.flow_id = f.id
WHERE
    org_id = $1 AND
	id = $2 AND
	is_active = TRUE AND
	is_archived = FALSE
) r;`

// MarshalJSON marshals into JSON. 0 values will become null
func (i FlowID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *FlowID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i FlowID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *FlowID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}
