package storage_test

import (
	"io/ioutil"
	"testing"

	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/storage"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAttachment(t *testing.T) {
	store := testsuite.Storage()
	defer testsuite.ResetStorage()

	image, err := ioutil.ReadFile("testdata/test.jpg")
	require.NoError(t, err)

	attachment, err := storage.StoreAttachment(store, "media", 1, "668383ba-387c-49bc-b164-1213ac0ea7aa.jpg", image)
	require.NoError(t, err)

	assert.Equal(t, utils.Attachment("image/jpeg:_test_storage/media/1/6683/83ba/668383ba-387c-49bc-b164-1213ac0ea7aa.jpg"), attachment)
}
