package models

import (
	"context"
	"fmt"
	"time"

	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/mailroom/utils/redisx"
	"github.com/pkg/errors"
)

const (
	recentOperandsCap    = 5
	recentOperandsExpire = time.Hour * 48
	recentOperandsKey    = "recent_operands:%s"
)

var recentOperandsForTypes = map[string]bool{"wait_for_response": true, "split_by_expression": true}

type segmentID struct {
	exitUUID flows.ExitUUID
	destUUID flows.NodeUUID
}

func (s segmentID) String() string {
	return fmt.Sprintf("%s:%s", s.exitUUID, s.destUUID)
}

type segmentValue struct {
	operand string
	time    time.Time
}

// RecordFlowStatistics records statistics from the given parallel slices of sessions and sprints
func RecordFlowStatistics(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, sessions []flows.Session, sprints []flows.Sprint) error {
	rc := rt.RP.Get()
	defer rc.Close()

	segmentIDs := make([]segmentID, 0, 10)
	recentBySegment := make(map[segmentID][]segmentValue, 10)
	nodeTypeCache := make(map[flows.NodeUUID]string)

	for _, sprint := range sprints {
		for _, seg := range sprint.Segments() {
			segID := segmentID{seg.Exit().UUID(), seg.Destination().UUID()}
			uiNodeType := getNodeUIType(seg.Flow(), seg.Node(), nodeTypeCache)
			if recentOperandsForTypes[uiNodeType] {
				if _, seen := recentBySegment[segID]; !seen {
					segmentIDs = append(segmentIDs, segID)
				}

				recentBySegment[segID] = append(recentBySegment[segID], segmentValue{seg.Operand(), seg.Time()})
			}
		}
	}

	for _, segID := range segmentIDs {
		recentOperands := recentBySegment[segID]

		// trim recent values for each segment - no point in trying to add more values than we keep
		if len(recentOperands) > recentOperandsCap {
			recentBySegment[segID] = recentOperands[:len(recentOperands)-recentOperandsCap]
		}

		recentSet := redisx.NewCappedZSet(fmt.Sprintf(recentOperandsKey, segID), recentOperandsCap, recentOperandsExpire)

		for _, recent := range recentOperands {
			// set members need to be unique, so prefix operand with a random UUID
			value := fmt.Sprintf("%s|%s", uuids.New(), recent.operand)
			score := float64(recent.time.UnixNano()) / float64(1e9) // score is UNIX time as floating point

			err := recentSet.Add(rc, value, score)
			if err != nil {
				return errors.Wrap(err, "error adding recent operand to set")
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
