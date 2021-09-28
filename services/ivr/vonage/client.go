package vonage

// CallURL is the API endpoint for Vonage/Nexmo calls, public so our main IVR test can change it
var CallURL = `https://api.nexmo.com/v1/calls`

type Phone struct {
	Type   string `json:"type"`
	Number string `json:"number"`
}

type NCCO struct {
	Action string `json:"action"`
	Name   string `json:"name"`
}

// CallRequest is the request payload to create a new call, see https://developer.nexmo.com/api/voice#createCall
type CallRequest struct {
	To           []Phone  `json:"to"`
	From         Phone    `json:"from"`
	AnswerURL    []string `json:"answer_url"`
	AnswerMethod string   `json:"answer_method"`
	EventURL     []string `json:"event_url"`
	EventMethod  string   `json:"event_method"`

	NCCO             []NCCO `json:"ncco,omitempty"`
	MachineDetection string `json:"machine_detection"`
	RingingTimer     int    `json:"ringing_timer,omitempty"`
}

// CallResponse is the response from creating a new call
// {
//  "uuid": "63f61863-4a51-4f6b-86e1-46edebcf9356",
//  "status": "started",
//  "direction": "outbound",
//  "conversation_uuid": "CON-f972836a-550f-45fa-956c-12a2ab5b7d22"
// }
type CallResponse struct {
	UUID             string `json:"uuid"`
	Status           string `json:"status"`
	Direction        string `json:"direction"`
	ConversationUUID string `json:"conversation_uuid"`
}

type Talk struct {
	Action  string `json:"action"`
	Text    string `json:"text"`
	BargeIn bool   `json:"bargeIn,omitempty"`
	Error   string `json:"_error,omitempty"`
	Message string `json:"_message,omitempty"`
}

type Stream struct {
	Action    string   `json:"action"`
	StreamURL []string `json:"streamUrl"`
}

type Hangup struct {
	XMLName string `xml:"Hangup"`
}

type Redirect struct {
	XMLName string `xml:"Redirect"`
	URL     string `xml:",chardata"`
}

type Input struct {
	Action       string   `json:"action"`
	MaxDigits    int      `json:"maxDigits,omitempty"`
	SubmitOnHash bool     `json:"submitOnHash"`
	Timeout      int      `json:"timeOut"`
	EventURL     []string `json:"eventUrl"`
	EventMethod  string   `json:"eventMethod"`
}

type Record struct {
	Action       string   `json:"action"`
	EndOnKey     string   `json:"endOnKey,omitempty"`
	Timeout      int      `json:"timeOut,omitempty"`
	EndOnSilence int      `json:"endOnSilence,omitempty"`
	EventURL     []string `json:"eventUrl"`
	EventMethod  string   `json:"eventMethod"`
}

type Endpoint struct {
	Type   string `json:"type"`
	Number string `json:"number"`
}

type Conversation struct {
	Action string `json:"action"`
	Name   string `json:"name"`
}
