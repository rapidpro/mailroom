package web_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/nyaruka/mailroom/web"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestForm struct {
	Foo []string `form:"foo" validate:"required"`
	Bar string   `form:"bar" validate:"required"`
}

func TestDecodeAndValidateForm(t *testing.T) {
	// make a request with valid form data
	data := url.Values{
		"foo": []string{"a", "b"},
		"bar": []string{"x"},
	}
	request, err := http.NewRequest("POST", "http://temba.io", strings.NewReader(data.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, err)

	form := &TestForm{}
	err = web.DecodeAndValidateForm(form, request)

	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, form.Foo)
	assert.Equal(t, "x", form.Bar)

	// make a request that's missing required data
	data = url.Values{
		"foo": []string{"a", "b"},
	}
	request, err = http.NewRequest("POST", "http://temba.io", strings.NewReader(data.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, err)

	form = &TestForm{}
	err = web.DecodeAndValidateForm(form, request)
	assert.Error(t, err)
}

func TestDecodeAndValidateMultipartForm(t *testing.T) {
	// make a request with valid form data
	request, err := web.MakeMultipartRequest("POST", "http://temba.io", map[string][]string{
		"foo": []string{"a", "b"},
		"bar": []string{"x"},
	}, map[string]string{
		"file1": "hello world\n",
	})
	require.NoError(t, err)

	form := &TestForm{}
	err = web.DecodeAndValidateMultipartForm(form, request)

	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, form.Foo)
	assert.Equal(t, "x", form.Bar)

	// make a request that's missing required data
	request, err = web.MakeMultipartRequest("POST", "http://temba.io", map[string][]string{
		"foo": []string{"a", "b"},
	}, nil)
	require.NoError(t, err)

	form = &TestForm{}
	err = web.DecodeAndValidateMultipartForm(form, request)
	assert.Error(t, err)
}
