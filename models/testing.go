package models

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
)

func AssertContactSessionsPresent(t *testing.T, db *sqlx.DB, contactIDs []flows.ContactID, extraWhere string) {
	ids := make([]int64, len(contactIDs))
	for i := range contactIDs {
		ids[i] = int64(contactIDs[i])
	}
	sql := fmt.Sprintf(`SELECT COUNT(*) FROM flows_flowsession where contact_id = ANY($1::int[]) %s`, extraWhere)
	var count int
	err := db.Get(&count, sql, pq.Int64Array(ids))
	assert.NoError(t, err)
	assert.Equal(t, len(contactIDs), count)
}

func AssertContactRunsPresent(t *testing.T, db *sqlx.DB, contactIDs []flows.ContactID, flowID FlowID, extraWhere string) {
	ids := make([]int64, len(contactIDs))
	for i := range contactIDs {
		ids[i] = int64(contactIDs[i])
	}
	sql := fmt.Sprintf(`SELECT COUNT(*) FROM flows_flowrun where flow_id = $1 AND contact_id = ANY($2::int[]) %s`, extraWhere)
	var count int
	err := db.Get(&count, sql, flowID, pq.Int64Array(ids))
	assert.NoError(t, err)
	assert.Equal(t, len(contactIDs), count)
}

func AssertContactMessagesPresent(t *testing.T, db *sqlx.DB, contactIDs []flows.ContactID, extraWhere string) {
	ids := make([]int64, len(contactIDs))
	for i := range contactIDs {
		ids[i] = int64(contactIDs[i])
	}
	var count int
	sql := fmt.Sprintf(`SELECT COUNT(*) FROM msgs_msg WHERE contact_id = ANY($1::int[]) %s`, extraWhere)
	err := db.Get(&count, sql, pq.Int64Array(ids))
	assert.NoError(t, err)
	assert.Equal(t, len(contactIDs), count)
}
