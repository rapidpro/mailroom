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

	sessions, err := FireCampaignEvent(ctx, db, rp, models.OrgID(1), []flows.ContactID{42, 43}, assets.FlowUUID("ab906843-73db-43fb-b44f-c6f4bce4a8fc"), &event)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(sessions))
}
