package storage_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/nyaruka/mailroom/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFS(t *testing.T) {
	s := storage.NewFS("_testing")
	assert.NoError(t, s.Test())

	url, err := s.Put("/foo/bar.txt", "text/plain", []byte(`hello world`))
	assert.NoError(t, err)
	assert.Equal(t, "_testing/foo/bar.txt", url)

	data, err := ioutil.ReadFile(url)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`hello world`), data)

	require.NoError(t, os.RemoveAll("_testing"))
}
