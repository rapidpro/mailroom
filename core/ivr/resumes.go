package ivr

import (
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
)

type ResumeType string

const (
	InputResumeType   = ResumeType("input")
	DialResumeType    = ResumeType("dial")
	TimeoutResumeType = ResumeType("timeout")
)

// Resume is our interface for a type of IVR resume
type Resume interface {
	Type() ResumeType
}

// InputResume is our type for resumes as consequences of user inputs (either a digit or recording)
type InputResume struct {
	Input      string
	Attachment utils.Attachment
}

// Type returns the type for InputResume
func (r InputResume) Type() ResumeType {
	return InputResumeType
}

// DialResume is our type for resumes as consequences of dials/transfers completing
type DialResume struct {
	Status   flows.DialStatus
	Duration int
}

// Type returns the type for DialResume
func (r DialResume) Type() ResumeType {
	return DialResumeType
}
