package runner

import (
	"os"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestCampaignStarts(t *testing.T) {
	db := testsuite.DB()
	ctx := testsuite.CTX()
	rp := testsuite.RP()

	// delete our android channel, we want our messages to be sent through courier
	db.MustExec(`DELETE FROM channels_channel where id = 1;`)

	event := triggers.CampaignEvent{
		UUID: "e68f4c70-9db1-44c8-8498-602d6857235e",
		Campaign: triggers.Campaign{
			UUID: "5da68501-61c4-4638-a494-3314a6d5edbd",
			Name: "Doctor Reminders",
		},
	}

	contacts := []flows.ContactID{42, 43}
	sessions, err := FireCampaignEvent(ctx, db, rp, models.OrgID(1), contacts, assets.FlowUUID("ab906843-73db-43fb-b44f-c6f4bce4a8fc"), &event)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sessions))

	models.AssertContactSessionsPresent(t, db, contacts,
		`AND status = 'C' AND responded = FALSE AND org_id = 1 AND connection_id IS NULL AND output IS NOT NULL`,
	)
	models.AssertContactRunsPresent(t, db, contacts, models.FlowID(31),
		`AND is_active = FALSE AND responded = FALSE AND org_id = 1 AND parent_id IS NULL AND exit_type = 'C'
		 AND results IS NOT NULL AND path IS NOT NULL AND events IS NOT NULL
		 AND current_node_uuid = 'b003fd70-aafd-4ccc-bdb5-4f70e870cd64'
		 AND session_id IS NOT NULL`,
	)
	models.AssertContactMessagesPresent(t, db, contacts,
		`AND text like '% it is time to consult with your patients.' AND org_id = 1 AND status = 'Q' 
		 AND queued_on IS NOT NULL AND direction = 'O' AND topup_id IS NOT NULL AND msg_type = 'F' AND channel_id = 2`,
	)
}
