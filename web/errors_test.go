package web_test

import (
	"testing"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils/jsonx"
	"github.com/nyaruka/mailroom/web"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestErrorResponse(t *testing.T) {
	er1 := web.NewErrorResponse(errors.New("I'm an error!"))
	assert.Equal(t, "I'm an error!", er1.Error)

	er1JSON, err := jsonx.Marshal(er1)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"error": "I'm an error!"}`, string(er1JSON))

	er2 := web.NewErrorResponse(contactql.NewQueryError("I'm a rich error!", "foo_code", map[string]string{"foo": "123"}))
	assert.Equal(t, "I'm a rich error!", er2.Error)
	assert.Equal(t, "foo_code", er2.Code)

	er2JSON, err := jsonx.Marshal(er2)
	assert.NoError(t, err)
	assert.JSONEq(t, `{"error": "I'm a rich error!", "code": "foo_code", "extra": {"foo": "123"}}`, string(er2JSON))
}
