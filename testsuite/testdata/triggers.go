package testdata

import (
	"testing"

	"github.com/nyaruka/mailroom/core/models"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func InsertKeywordTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, keyword string, matchType models.MatchType, includeGroups []models.GroupID, excludeGroups []models.GroupID) models.TriggerID {
	return insertTrigger(t, db, models.KeywordTriggerType, flowID, keyword, matchType, includeGroups, excludeGroups, nil, "", models.NilChannelID)
}

func InsertIncomingCallTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, includeGroups []models.GroupID, excludeGroups []models.GroupID) models.TriggerID {
	return insertTrigger(t, db, models.IncomingCallTriggerType, flowID, "", "", includeGroups, excludeGroups, nil, "", models.NilChannelID)
}

func InsertMissedCallTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID) models.TriggerID {
	return insertTrigger(t, db, models.MissedCallTriggerType, flowID, "", "", nil, nil, nil, "", models.NilChannelID)
}

func InsertNewConversationTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, channelID models.ChannelID) models.TriggerID {
	return insertTrigger(t, db, models.NewConversationTriggerType, flowID, "", "", nil, nil, nil, "", channelID)
}

func InsertReferralTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, referrerID string, channelID models.ChannelID) models.TriggerID {
	return insertTrigger(t, db, models.ReferralTriggerType, flowID, "", "", nil, nil, nil, referrerID, channelID)
}

func InsertCatchallTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, includeGroups []models.GroupID, excludeGroups []models.GroupID) models.TriggerID {
	return insertTrigger(t, db, models.CatchallTriggerType, flowID, "", "", includeGroups, excludeGroups, nil, "", models.NilChannelID)
}

func InsertScheduledTrigger(t *testing.T, db *sqlx.DB, flowID models.FlowID, includeGroups []models.GroupID, excludeGroups []models.GroupID, includeContacts []models.ContactID) models.TriggerID {
	return insertTrigger(t, db, models.ScheduleTriggerType, flowID, "", "", includeGroups, excludeGroups, includeContacts, "", models.NilChannelID)
}

func insertTrigger(t *testing.T, db *sqlx.DB, triggerType models.TriggerType, flowID models.FlowID, keyword string, matchType models.MatchType, includeGroups, excludeGroups []models.GroupID, contactIDs []models.ContactID, referrerID string, channelID models.ChannelID) models.TriggerID {
	var id models.TriggerID
	err := db.Get(&id,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, referrer_id, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, channel_id)
		VALUES(TRUE, now(), now(), $1, $5, false, $2, $3, $4, 1, 1, 1, $6) RETURNING id`, keyword, flowID, triggerType, matchType, referrerID, channelID)

	require.NoError(t, err)

	// insert group associations
	for _, g := range includeGroups {
		db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, id, g)
	}
	for _, g := range excludeGroups {
		db.MustExec(`INSERT INTO triggers_trigger_exclude_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, id, g)
	}

	// insert contact associations
	for _, c := range contactIDs {
		db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2)`, id, c)
	}

	return id
}
