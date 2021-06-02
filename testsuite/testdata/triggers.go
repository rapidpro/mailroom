package testdata

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func InsertKeywordTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, keyword string, matchType models.MatchType, includeGroups []Group, excludeGroups []Group) models.TriggerID {
	return insertTrigger(t, db, org, models.KeywordTriggerType, flow, keyword, matchType, includeGroups, excludeGroups, nil, "", nil)
}

func InsertIncomingCallTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, includeGroups []Group, excludeGroups []Group) models.TriggerID {
	return insertTrigger(t, db, org, models.IncomingCallTriggerType, flow, "", "", includeGroups, excludeGroups, nil, "", nil)
}

func InsertMissedCallTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow) models.TriggerID {
	return insertTrigger(t, db, org, models.MissedCallTriggerType, flow, "", "", nil, nil, nil, "", nil)
}

func InsertNewConversationTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, channel *Channel) models.TriggerID {
	return insertTrigger(t, db, org, models.NewConversationTriggerType, flow, "", "", nil, nil, nil, "", channel)
}

func InsertReferralTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, referrerID string, channel *Channel) models.TriggerID {
	return insertTrigger(t, db, org, models.ReferralTriggerType, flow, "", "", nil, nil, nil, referrerID, channel)
}

func InsertCatchallTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, includeGroups []Group, excludeGroups []Group) models.TriggerID {
	return insertTrigger(t, db, org, models.CatchallTriggerType, flow, "", "", includeGroups, excludeGroups, nil, "", nil)
}

func InsertScheduledTrigger(t *testing.T, db *sqlx.DB, org Org, flow Flow, includeGroups []Group, excludeGroups []Group, includeContacts []*Contact) models.TriggerID {
	return insertTrigger(t, db, org, models.ScheduleTriggerType, flow, "", "", includeGroups, excludeGroups, includeContacts, "", nil)
}

func insertTrigger(t *testing.T, db *sqlx.DB, org Org, triggerType models.TriggerType, flow Flow, keyword string, matchType models.MatchType, includeGroups, excludeGroups []Group, contactIDs []*Contact, referrerID string, channel *Channel) models.TriggerID {
	channelID := models.NilChannelID
	if channel != nil {
		channelID = channel.ID
	}

	var id models.TriggerID
	err := db.Get(&id,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, referrer_id, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, channel_id)
		VALUES(TRUE, now(), now(), $1, $5, false, $2, $3, $4, 1, 1, $7, $6) RETURNING id`, keyword, flow.ID, triggerType, matchType, referrerID, channelID, org.ID)

	require.NoError(t, err)

	// insert group associations
	for _, g := range includeGroups {
		db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, id, g.ID)
	}
	for _, g := range excludeGroups {
		db.MustExec(`INSERT INTO triggers_trigger_exclude_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, id, g.ID)
	}

	// insert contact associations
	for _, c := range contactIDs {
		db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2)`, id, c.ID)
	}

	return id
}
