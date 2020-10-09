package models

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func TestClassifiers(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	classifiers, err := loadClassifiers(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID      ClassifierID
		UUID    assets.ClassifierUUID
		Name    string
		Intents []string
	}{
		{LuisID, LuisUUID, "LUIS", []string{"book_flight", "book_car"}},
		{WitID, WitUUID, "Wit.ai", []string{"register"}},
		{BothubID, BothubUUID, "BotHub", []string{"intent"}},
	}

	assert.Equal(t, len(tcs), len(classifiers))
	for i, tc := range tcs {
		c := classifiers[i].(*Classifier)
		assert.Equal(t, tc.UUID, c.UUID())
		assert.Equal(t, tc.ID, c.ID())
		assert.Equal(t, tc.Name, c.Name())
		assert.Equal(t, tc.Intents, c.Intents())
	}

}
