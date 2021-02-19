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

type Resume interface {
	Type() ResumeType
}

type InputResume struct {
	Input      string
	Attachment utils.Attachment
}

func (r InputResume) Type() ResumeType {
	return InputResumeType
}

type DialResume struct {
	Status   flows.DialStatus
	Duration int
}

func (r DialResume) Type() ResumeType {
	return DialResumeType
}

type TimeoutResume struct {
}

func (r TimeoutResume) Type() ResumeType {
	return TimeoutResumeType
}
