package models

import (
	"github.com/greatnonprofits-nfp/goflow/flows"
	"testing"

	"github.com/greatnonprofits-nfp/goflow/assets"
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

func TestClassifier_AsService(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	classifiers, err := loadClassifiers(ctx, db, 1)
	assert.NoError(t, err)
	classifier1 := classifiers[0]
	classifier := &flows.Classifier{Classifier: classifier1}

	c := &Classifier{}
	cc := &c.c
	cc.Type = "Fake"
	_, err = c.AsService(classifier)
	assert.EqualError(t, err, "unknown classifier type 'Fake' for classifier: ")

	c = classifier1.(*Classifier)
	classifierService, err := c.AsService(classifier)
	assert.NoError(t, err)
	_, ok := classifierService.(flows.ClassificationService)

	assert.True(t, ok)
}
