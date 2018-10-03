package runner

import (
	"os"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	_ "github.com/nyaruka/mailroom/hooks"
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

	// create our event fires
	now := time.Now()
	db.MustExec(`INSERT INTO campaigns_eventfire(contact_id, event_id, scheduled) VALUES(42,1, $1),(43,1, $1);`, now)

	contacts := []flows.ContactID{42, 43}
	fires := []*models.EventFire{
		&models.EventFire{
			FireID:    1,
			EventID:   1,
			ContactID: 42,
			Scheduled: now,
		},
		&models.EventFire{
			FireID:    2,
			EventID:   1,
			ContactID: 43,
			Scheduled: now,
		},
	}
	sessions, err := FireCampaignEvents(ctx, db, rp, models.OrgID(1), fires, assets.FlowUUID("ab906843-73db-43fb-b44f-c6f4bce4a8fc"), &event)
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

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NULL`, nil, 0)

	testsuite.AssertQueryCount(t, db,
		`SELECT count(*) from campaigns_eventfire WHERE fired IS NOT NULL AND contact_id IN (42,43) AND event_id = 1`, nil, 2)
}
