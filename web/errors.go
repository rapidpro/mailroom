package web

import (
	"github.com/nyaruka/goflow/utils"

	"github.com/pkg/errors"
)

// ErrorResponse is the type for our error responses
type ErrorResponse struct {
	Error string            `json:"error"`
	Code  string            `json:"code,omitempty"`
	Extra map[string]string `json:"extra,omitempty"`
}

// NewErrorResponse creates a new error response from the passed in error
func NewErrorResponse(err error) *ErrorResponse {
	rich, isRich := errors.Cause(err).(utils.RichError)
	if isRich {
		return &ErrorResponse{
			Error: rich.Error(),
			Code:  rich.Code(),
			Extra: rich.Extra(),
		}
	}
	return &ErrorResponse{Error: err.Error()}
}
