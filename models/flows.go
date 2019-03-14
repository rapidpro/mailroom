package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/legacy"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type FlowID null.Int

type FlowType string

const (
	IVRFlow       = FlowType("V")
	MessagingFlow = FlowType("M")
	SurveyorFlow  = FlowType("S")

	FlowConfigIVRRetryMinutes = "ivr_retry"

	NilFlowID = FlowID(0)
)

var FlowTypeMapping = map[flows.FlowType]FlowType{
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
		Config         types.JSONText  `json:"config"`
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

// SetDefinition sets our definition from the passed in new definition format
func (f *Flow) SetDefinition(definition json.RawMessage) {
	f.f.Definition = definition
}

// IntConfigValue returns the value for the key passed in as an int. If the value
// is not an integer or is not present then the defaultValue is returned
func (f *Flow) IntConfigValue(key string, defaultValue int64) int64 {
	value, err := jsonparser.GetInt(f.f.Config, key)
	if err != nil {
		return defaultValue
	}
	return value
}

// StringConfigValue returns the value for the key passed in as a string. If the value
// is not a string or is not present then the defaultValue is returned
func (f *Flow) StringConfigValue(key string, defaultValue string) string {
	value, err := jsonparser.GetString(f.f.Config, key)
	if err != nil {
		return defaultValue
	}
	return value
}

// SetLegacyDefinition sets our definition from the passed in legacy definition
func (f *Flow) SetLegacyDefinition(legacyDefinition json.RawMessage) error {
	// load it in from our json
	legacyFlow, err := legacy.ReadLegacyFlow(legacyDefinition)
	if err != nil {
		return errors.Wrapf(err, "error reading flow into legacy format: %s", legacyDefinition)
	}

	// migrate forwards returning our final flow definition
	newFlow, err := legacyFlow.Migrate(false, false, "https://"+config.Mailroom.AttachmentDomain)
	if err != nil {
		return errors.Wrapf(err, "error migrating flow: %s", legacyDefinition)
	}

	// write this flow back out in our new format
	f.f.Definition, err = json.Marshal(newFlow)
	if err != nil {
		return errors.Wrapf(err, "error mashalling migrated flow definition: %s", legacyDefinition)
	}

	return nil
}

// IgnoreTriggers returns whether this flow ignores triggers
func (f *Flow) IgnoreTriggers() bool { return f.f.IgnoreTriggers }

// FlowReference return a flow reference for this flow
func (f *Flow) FlowReference() *assets.FlowReference {
	return assets.NewFlowReference(f.UUID(), f.Name())
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

	err = readJSONRow(rows, &flow.f)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading flow definition by: %s", arg)
	}

	// our definition is really a legacy definition, set it from that
	err = flow.SetLegacyDefinition(flow.f.Definition)
	if err != nil {
		return nil, errors.Wrapf(err, "error setting flow definition from legacy")
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
	coalesce(metadata, '{}')::jsonb as config,
	definition::jsonb || 
		jsonb_build_object(
			'flow_type', f.flow_type, 
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
	coalesce(metadata, '{}')::jsonb as config,
	definition::jsonb || 
		jsonb_build_object(
			'flow_type', f.flow_type, 
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
