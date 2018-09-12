package models

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/nyaruka/goflow/flows"
)

// Field is our mailroom type for contact field types
type Field struct {
	uuid      FieldUUID
	key       string
	name      string
	valueType flows.FieldValueType
}

// UUID returns the UUID of this field
func (f *Field) UUID() FieldUUID { return f.uuid }

// Key returns the key for this field
func (f *Field) Key() string { return f.key }

// Name returns the name for this field
func (f *Field) Name() string { return f.name }

// ValueType returns the value type for this field
func (f *Field) ValueType() flows.FieldValueType { return f.valueType }

// loadFields loads the assets for the passed in db
func loadFields(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Field, error) {
	rows, err := db.Query(selectFieldsSQL, orgID)
	if err != nil {
		return nil, errors.Annotatef(err, "error querying fields for org: %d", orgID)
	}
	defer rows.Close()

	fields := make([]*Field, 0, 10)
	for rows.Next() {
		field := &Field{}
		err := rows.Scan(&field.uuid, &field.key, &field.name, &field.valueType)
		if err != nil {
			return nil, errors.Annotate(err, "error scanning field row")
		}

		fields = append(fields, field)
	}

	return fields, nil
}

const selectFieldsSQL = `
SELECT 
	uuid,
	key,
	label as name,
	(SELECT CASE value_type
		WHEN 'T' THEN 'text' 
		WHEN 'N' THEN 'number'
		WHEN 'D' THEN 'datetime'
		WHEN 'S' THEN 'state'
		WHEN 'I' THEN 'district'
		WHEN 'W' THEN 'ward'
	END) value_type
FROM 
	contacts_contactfield 
WHERE 
	org_id = $1 AND 
	is_active = TRUE AND
	field_type = 'U'
ORDER BY
    key ASC
`
