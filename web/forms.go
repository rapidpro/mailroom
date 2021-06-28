package web

import (
	"mime"
	"net/http"

	"github.com/gorilla/schema"
	validator "gopkg.in/go-playground/validator.v9"
)

var (
	decoder  = schema.NewDecoder()
	validate = validator.New()
)

const (
	maxMemory = 1024 * 1024
)

func init() {
	decoder.IgnoreUnknownKeys(true)
	decoder.SetAliasTag("form")
}

// Validate validates the passe din struct using our shared validator instance
func Validate(form interface{}) error {
	return validate.Struct(form)
}

// DecodeAndValidateForm takes the passed in request and attempts to decode it as either a URL encoded form or a multipart form
func DecodeAndValidateForm(form interface{}, r *http.Request) error {
	var err error
	contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))

	if contentType == "multipart/form-data" {
		err = r.ParseMultipartForm(maxMemory)
	} else {
		err = r.ParseForm()
	}

	if err != nil {
		return err
	}

	err = decoder.Decode(form, r.Form)
	if err != nil {
		return err
	}

	// check our input is valid
	return validate.Struct(form)
}
