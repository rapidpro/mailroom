package zendesk

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nyaruka/goflow/utils/dates"
	"github.com/nyaruka/mailroom/models"

	"github.com/pkg/errors"
)

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
		ids[i], err = ticketToZendeskID(tickets[i])
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// parses out the zendesk ticket ID from our local external ID field
func ticketToZendeskID(ticket *models.Ticket) (int64, error) {
	idStr := string(ticket.ExternalID())
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, errors.Errorf("%s is not a valid zendesk ticket id", idStr)
	}
	return id, nil
}
