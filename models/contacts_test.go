package models

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
)

func TestContacts(t *testing.T) {
	ctx := context.Background()
	db := Reset(t)

	org, err := NewOrgAssets(ctx, db, 1)
	assert.NoError(t, err)

	contacts, err := LoadContacts(ctx, db, org, []flows.ContactID{42, 43, 80})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(contacts))

	if len(contacts) == 3 {
		assert.Equal(t, "Cathy Quincy", contacts[0].Name())
		assert.Equal(t, len(contacts[0].URNs()), 1)
		assert.Equal(t, contacts[0].URNs()[0].String(), "tel:+250700000001")

		assert.Equal(t, flows.LocationPath("Nigeria > Sokoto"), contacts[0].Fields()["state"].TypedValue())
		assert.Equal(t, flows.LocationPath("Nigeria > Sokoto > Yabo > Kilgori"), contacts[0].Fields()["ward"].TypedValue())
		assert.Equal(t, types.NewXText("F"), contacts[0].Fields()["gender"].TypedValue())
		assert.Equal(t, nil, contacts[0].Fields()["age"].TypedValue())

		assert.Equal(t, "Dave Jameson", contacts[1].Name())
		assert.Equal(t, types.NewXNumber(decimal.RequireFromString("30")), contacts[1].Fields()["age"].TypedValue())

		assert.Equal(t, "Cathy Roberts", contacts[2].Name())
		time, _ := time.Parse("2006-01-02T15:04:05-0700", "2017-12-11T04:36:31.016000+01:00")
		assert.Equal(t, types.NewXDateTime(time), contacts[2].Fields()["joined"].TypedValue())
	}
}
