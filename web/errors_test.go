package web_test

import (
	"testing"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrorResponse(t *testing.T) {
	// create a simple error
	er1 := web.NewErrorResponse(errors.New("I'm an error!"))
	assert.Equal(t, "I'm an error!", er1.Error)

	er1JSON, err := jsonx.Marshal(er1)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"error": "I'm an error!"}`, string(er1JSON))

	// create a rich error
	_, err = contactql.ParseQuery(envs.NewBuilder().Build(), "$$", nil)

	er2 := web.NewErrorResponse(err)
	assert.Equal(t, "mismatched input '$' expecting {'(', TEXT, STRING}", er2.Error)
	assert.Equal(t, "unexpected_token", er2.Code)

	er2JSON, err := jsonx.Marshal(er2)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"error": "mismatched input '$' expecting {'(', TEXT, STRING}", "code": "unexpected_token", "extra": {"token": "$"}}`, string(er2JSON))
}
