package twiml

// BaseURL is our default base URL for TWIML channels (public for testing overriding)
var BaseURL = `https://api.twilio.com`

type Say struct {
	XMLName  string `xml:"Say"`
	Text     string `xml:",chardata"`
	Language string `xml:"language,attr,omitempty"`
}

type Play struct {
	XMLName string `xml:"Play"`
	URL     string `xml:",chardata"`
}

type Hangup struct {
	XMLName string `xml:"Hangup"`
}

type Redirect struct {
	XMLName string `xml:"Redirect"`
	URL     string `xml:",chardata"`
}

type Dial struct {
	XMLName string `xml:"Dial"`
	Number  string `xml:",chardata"`
	Action  string `xml:"action,attr"`
	Timeout int    `xml:"timeout,attr,omitempty"`
}

type Gather struct {
	XMLName     string        `xml:"Gather"`
	NumDigits   int           `xml:"numDigits,attr,omitempty"`
	FinishOnKey string        `xml:"finishOnKey,attr,omitempty"`
	Timeout     int           `xml:"timeout,attr,omitempty"`
	Action      string        `xml:"action,attr,omitempty"`
	Commands    []interface{} `xml:",innerxml"`
}

type Record struct {
	XMLName   string `xml:"Record"`
	Action    string `xml:"action,attr,omitempty"`
	MaxLength int    `xml:"maxLength,attr,omitempty"`
}

type Response struct {
	XMLName  string        `xml:"Response"`
	Message  string        `xml:",comment"`
	Gather   *Gather       `xml:"Gather"`
	Commands []interface{} `xml:",innerxml"`
}
