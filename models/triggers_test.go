package models

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/engine"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func insertTrigger(t *testing.T, db *sqlx.DB, active bool, flowID FlowID, triggerType TriggerType, keyword string, matchType MatchType, groupIDs []GroupID) TriggerID {
	var triggerID TriggerID
	err := db.Get(&triggerID,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, trigger_count)
		VALUES($1, now(), now(), $2, false, $3, $4, $5, 1, 1, 1, 0) RETURNING id`, active, keyword, flowID, triggerType, matchType)

	assert.NoError(t, err)

	// insert any group associations
	for _, g := range groupIDs {
		db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, triggerID, g)
	}

	return triggerID
}

func TestTriggers(t *testing.T) {
	db := testsuite.DB()
	ctx := testsuite.CTX()
	testsuite.Reset()

	joinID := insertTrigger(t, db, true, FlowID(2), KeywordTriggerType, "join", MatchFirst, nil)
	resistID := insertTrigger(t, db, true, FlowID(3), KeywordTriggerType, "resist", MatchOnly, nil)
	farmersID := insertTrigger(t, db, true, FlowID(3), KeywordTriggerType, "resist", MatchOnly, []GroupID{GroupID(32)})
	farmersAllID := insertTrigger(t, db, true, FlowID(3), CatchallTriggerType, "", MatchOnly, []GroupID{GroupID(32)})
	othersAllID := insertTrigger(t, db, true, FlowID(3), CatchallTriggerType, "", MatchOnly, nil)

	org, err := GetOrgAssets(ctx, db, OrgID(1))
	assert.NoError(t, err)

	contactIDs := []flows.ContactID{42, 43}
	contacts, err := LoadContacts(ctx, db, org, contactIDs)
	assert.NoError(t, err)

	sa, err := engine.NewSessionAssets(org)
	assert.NoError(t, err)

	cathy, err := contacts[0].FlowContact(org, sa)
	assert.NoError(t, err)

	greg, err := contacts[1].FlowContact(org, sa)
	assert.NoError(t, err)

	tcs := []struct {
		Text      string
		Contact   *flows.Contact
		TriggerID TriggerID
	}{
		{"join", cathy, joinID},
		{"join this", cathy, joinID},
		{"resist", greg, resistID},
		{"resist", cathy, farmersID},
		{"resist this", cathy, farmersAllID},
		{"other", cathy, farmersAllID},
		{"other", greg, othersAllID},
		{"", greg, othersAllID},
	}

	for i, tc := range tcs {
		trigger := FindMatchingMsgTrigger(org, tc.Contact, tc.Text)
		if trigger == nil {
			assert.Equal(t, tc.TriggerID, TriggerID(0), "%d: did not get back expected trigger", i)
		} else {
			assert.Equal(t, tc.TriggerID, trigger.ID(), "%d: did not get back expected trigger", i)
		}
	}
}
