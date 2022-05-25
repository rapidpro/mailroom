package models

import (
	"context"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/redisx"
	"github.com/pkg/errors"
)

const (
	recentContactsCap    = 5              // number of recent contacts we keep per segment
	recentContactsExpire = time.Hour * 24 // how long we keep recent contacts
	recentContactsKey    = "recent_contacts:%s"
)

var storeOperandsForTypes = map[string]bool{"wait_for_response": true, "split_by_expression": true, "split_by_contact_field": true, "split_by_run_result": true}

type segmentID struct {
	exitUUID flows.ExitUUID
	destUUID flows.NodeUUID
}

func (s segmentID) String() string {
	return fmt.Sprintf("%s:%s", s.exitUUID, s.destUUID)
}

type segmentContact struct {
	contact *flows.Contact
	operand string
	time    time.Time
}

// RecordFlowStatistics records statistics from the given parallel slices of sessions and sprints
func RecordFlowStatistics(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, sessions []flows.Session, sprints []flows.Sprint) error {
	rc := rt.RP.Get()
	defer rc.Close()

	segmentIDs := make([]segmentID, 0, 10)
	recentBySegment := make(map[segmentID][]*segmentContact, 10)
	nodeTypeCache := make(map[flows.NodeUUID]string)

	for i, sprint := range sprints {
		session := sessions[i]

		for _, seg := range sprint.Segments() {
			segID := segmentID{seg.Exit().UUID(), seg.Destination().UUID()}
			uiNodeType := getNodeUIType(seg.Flow(), seg.Node(), nodeTypeCache)

			// only store operand values for certain node types
			operand := ""
			if storeOperandsForTypes[uiNodeType] {
				operand = seg.Operand()
			}

			if _, seen := recentBySegment[segID]; !seen {
				segmentIDs = append(segmentIDs, segID)
			}
			recentBySegment[segID] = append(recentBySegment[segID], &segmentContact{contact: session.Contact(), operand: operand, time: seg.Time()})
		}
	}

	for _, segID := range segmentIDs {
		recentContacts := recentBySegment[segID]

		// trim recent set for each segment - no point in trying to add more values than we keep
		if len(recentContacts) > recentContactsCap {
			recentBySegment[segID] = recentContacts[:len(recentContacts)-recentContactsCap]
		}

		recentSet := redisx.NewCappedZSet(fmt.Sprintf(recentContactsKey, segID), recentContactsCap, recentContactsExpire)

		for _, recent := range recentContacts {
			// set members need to be unique, so we include a random string
			value := fmt.Sprintf("%s|%d|%s", redisx.RandomBase64(10), recent.contact.ID(), utils.TruncateEllipsis(recent.operand, 100))
			score := float64(recent.time.UnixNano()) / float64(1e9) // score is UNIX time as floating point

			err := recentSet.Add(rc, value, score)
			if err != nil {
				return errors.Wrap(err, "error adding recent contact to set")
			}
		}
	}

	return nil
}

func getNodeUIType(flow flows.Flow, node flows.Node, cache map[flows.NodeUUID]string) string {
	uiType, cached := cache[node.UUID()]
	if cached {
		return uiType
	}

	// try to lookup node type but don't error if we can't find it.. could be a bad flow
	value, _ := jsonparser.GetString(flow.UI(), "nodes", string(node.UUID()), "type")
	cache[node.UUID()] = value
	return value
}
