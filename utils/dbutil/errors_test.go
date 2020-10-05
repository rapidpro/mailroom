package dbutil_test

import (
	"testing"

	"github.com/lib/pq"
	"github.com/nyaruka/mailroom/utils/dbutil"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsUniqueViolation(t *testing.T) {
	var err error = &pq.Error{Code: pq.ErrorCode("23505")}

	assert.True(t, dbutil.IsUniqueViolation(err))
	assert.False(t, dbutil.IsUniqueViolation(errors.New("boom")))
}
