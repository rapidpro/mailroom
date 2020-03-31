package web

import (
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

// DecodeAndValidateForm takes the passed in form and attempts to parse and validate it from the
// URL query parameters as well as any POST parameters of the passed in request
func DecodeAndValidateForm(form interface{}, r *http.Request) error {
	err := r.ParseForm()
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

// DecodeAndValidateMultipartForm takes the passed in form and attempts to parse and validate it from the
// multipart form data
func DecodeAndValidateMultipartForm(form interface{}, r *http.Request) error {
	err := r.ParseMultipartForm(maxMemory)
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
