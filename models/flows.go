package models

import (
	"context"
	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/legacy"
)

type FlowID int

// Flow is the mailroom type for a flow
type Flow struct {
	f struct {
		ID         FlowID          `json:"id"`
		UUID       assets.FlowUUID `json:"uuid"`
		Name       string          `json:"name"`
		Definition json.RawMessage `json:"definition"`
		IsArchived bool            `json:"is_archived"`
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

// IsArchived returns whether this flow is archived
func (f *Flow) IsArchived() bool { return f.f.IsArchived }

func loadFlowByUUID(ctx context.Context, db *sqlx.DB, flowUUID assets.FlowUUID) (*Flow, error) {
	return loadFlow(ctx, db, selectFlowByUUIDSQL, flowUUID)
}

func loadFlowByID(ctx context.Context, db *sqlx.DB, flowID FlowID) (*Flow, error) {
	return loadFlow(ctx, db, selectFlowByIDSQL, flowID)
}

// loads the flow with the passed in UUID
func loadFlow(ctx context.Context, db *sqlx.DB, sql string, arg interface{}) (*Flow, error) {
	flow := &Flow{}

	rows, err := db.Queryx(sql, arg)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying flow by: %s", arg)
	}
	defer rows.Close()

	// no row, no flow!
	if !rows.Next() {
		return nil, nil
	}

	err = readJSONRow(rows, &flow.f)
	if err != nil {
		return nil, errors.Annotatef(err, "error reading flow definition by: %s", arg)
	}

	// load it in from our json
	legacyFlow, err := legacy.ReadLegacyFlow([]byte(flow.f.Definition))
	if err != nil {
		return nil, errors.Annotatef(err, "error reading flow into legacy format: %s", arg)
	}

	// migrate forwards returning our final flow definition
	newFlow, err := legacyFlow.Migrate(false, false)
	if err != nil {
		return nil, errors.Annotatef(err, "error migrating flow: %s", arg)
	}

	// write this flow back out in our new format
	flow.f.Definition, err = json.Marshal(newFlow)
	if err != nil {
		return nil, errors.Annotatef(err, "error mashalling migrated flow definition: %s", arg)
	}

	return flow, nil
}

const selectFlowByUUIDSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	f.id as id, 
	f.uuid as uuid,
	f.name as name,
	f.is_archived as is_archived,
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
	fr.revision DESC 
LIMIT 1
) r;`

const selectFlowByIDSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	f.id as id, 
	f.uuid as uuid,
	f.name as name,
	f.is_archived as is_archived,
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
	f.id = $1 AND 
	fr.flow_id = f.id AND 
	fr.is_active = TRUE AND
	f.is_active = TRUE
ORDER BY 
	fr.revision DESC 
LIMIT 1
) r;`
