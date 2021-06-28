package zendesk

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/pkg/errors"
)

func ParseNumericID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.Errorf("%s is not a valid zendesk numeric id", s)
	}
	return n, nil
}

func NumericIDToString(n int64) string {
	return fmt.Sprintf("%d", n)
}

// RequestID is a helper class to construct and later parse requests IDs for push requests
type RequestID struct {
	Secret    string
	Timestamp time.Time
}

// NewRequestID creates a new unique request ID
func NewRequestID(secret string) RequestID {
	return RequestID{Secret: secret, Timestamp: dates.Now()}
}

func (i RequestID) String() string {
	return fmt.Sprintf("%s:%d", i.Secret, i.Timestamp.UnixNano())
}

// ParseRequestID parses a request ID
func ParseRequestID(s string) (RequestID, error) {
	parts := strings.Split(s, ":")
	if len(parts) == 2 {
		secret := parts[0]
		nanos, err := strconv.ParseInt(parts[1], 10, 64)
		if err == nil {
			return RequestID{Secret: secret, Timestamp: time.Unix(0, nanos)}, nil
		}
	}
	return RequestID{}, errors.New("invalid request ID format")
}

// parses out the zendesk ticket IDs from our local external ID field
func ticketsToZendeskIDs(tickets []*models.Ticket) ([]int64, error) {
	var err error
	ids := make([]int64, len(tickets))
	for i := range tickets {
		ids[i], err = ParseNumericID(string(tickets[i].ExternalID()))
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}
