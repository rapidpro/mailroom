package models

import (
	"context"

	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
)

var recordRecentOperands = map[string]bool{"wait_for_response": true, "split_by_expression": true}

func RecordFlowStatistics(ctx context.Context, tx *sqlx.Tx, sessions []flows.Session, sprints []flows.Sprint) error {
	nodeTypeCache := make(map[flows.NodeUUID]string)

	for _, sprint := range sprints {
		for _, seg := range sprint.Segments() {
			uiNodeType := getNodeUIType(seg.Flow(), seg.Node(), nodeTypeCache)
			if recordRecentOperands[uiNodeType] {

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
