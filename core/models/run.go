package models

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/vinovest/sqlx"
)

type FlowRunID int64

const NilFlowRunID = FlowRunID(0)

type RunStatus string

const (
	RunStatusActive      RunStatus = "A"
	RunStatusWaiting     RunStatus = "W"
	RunStatusCompleted   RunStatus = "C"
	RunStatusExpired     RunStatus = "X"
	RunStatusInterrupted RunStatus = "I"
	RunStatusFailed      RunStatus = "F"
)

var runStatusMap = map[flows.RunStatus]RunStatus{
	flows.RunStatusActive:    RunStatusActive,
	flows.RunStatusWaiting:   RunStatusWaiting,
	flows.RunStatusCompleted: RunStatusCompleted,
	flows.RunStatusExpired:   RunStatusExpired,
	flows.RunStatusFailed:    RunStatusFailed,
}

// FlowRun is the type for a run of a flow
type FlowRun struct {
	ID              FlowRunID         `db:"id"`
	UUID            flows.RunUUID     `db:"uuid"`
	Status          RunStatus         `db:"status"`
	CreatedOn       time.Time         `db:"created_on"`
	ModifiedOn      time.Time         `db:"modified_on"`
	ExitedOn        *time.Time        `db:"exited_on"`
	Responded       bool              `db:"responded"`
	Results         string            `db:"results"`
	PathNodes       pq.StringArray    `db:"path_nodes"`
	PathTimes       pq.GenericArray   `db:"path_times"`
	CurrentNodeUUID null.String       `db:"current_node_uuid"`
	ContactID       ContactID         `db:"contact_id"`
	FlowID          FlowID            `db:"flow_id"`
	OrgID           OrgID             `db:"org_id"`
	SessionUUID     flows.SessionUUID `db:"session_uuid"`
	StartID         StartID           `db:"start_id"`
}

// NewRun creates a flow run we can save to the database
func NewRun(oa *OrgAssets, fs flows.Session, fr flows.Run) *FlowRun {
	// build our path elements
	pathNodes := make(pq.StringArray, len(fr.Path()))
	pathTimes := make([]time.Time, len(fr.Path()))
	for i, p := range fr.Path() {
		pathNodes[i] = string(p.NodeUUID())
		pathTimes[i] = p.ArrivedOn()
	}

	// it's possible to resume a session with previous runs in now deleted flows - we don't need flow ID to update such runs
	var flowID FlowID
	if fr.Flow() != nil {
		flowID = fr.Flow().Asset().(*Flow).ID()
	}

	r := &FlowRun{
		UUID:        fr.UUID(),
		Status:      runStatusMap[fr.Status()],
		CreatedOn:   fr.CreatedOn(),
		ExitedOn:    fr.ExitedOn(),
		ModifiedOn:  fr.ModifiedOn(),
		ContactID:   ContactID(fr.Contact().ID()),
		FlowID:      flowID,
		OrgID:       oa.OrgID(),
		SessionUUID: fs.UUID(),
		StartID:     NilStartID,
		PathNodes:   pathNodes,
		PathTimes:   pq.GenericArray{A: pathTimes},
		Results:     string(jsonx.MustMarshal(fr.Results())),
		Responded:   fr.HadInput(),
	}

	if len(pathNodes) > 0 && (fr.Status() == flows.RunStatusActive || fr.Status() == flows.RunStatusWaiting) {
		r.CurrentNodeUUID = null.String(pathNodes[len(pathNodes)-1])
	}

	return r
}

const sqlInsertRun = `
INSERT INTO
flows_flowrun(uuid, created_on, modified_on, exited_on, status, responded, results, path_nodes, path_times,
	          current_node_uuid, contact_id, flow_id, org_id, session_uuid, start_id)
	   VALUES(:uuid, :created_on, NOW(), :exited_on, :status, :responded, :results, :path_nodes, :path_times,
	          :current_node_uuid, :contact_id, :flow_id, :org_id, :session_uuid, :start_id)
RETURNING id
`

func InsertRuns(ctx context.Context, tx *sqlx.Tx, runs []*FlowRun) error {
	if err := BulkQuery(ctx, "insert runs", tx, sqlInsertRun, runs); err != nil {
		return fmt.Errorf("error inserting runs: %w", err)
	}
	return nil
}

const sqlUpdateRun = `
UPDATE
	flows_flowrun fr
SET
	status = r.status,
	exited_on = r.exited_on,
	responded = r.responded,
	results = r.results,
	path_nodes = r.path_nodes,
	path_times = r.path_times,
	current_node_uuid = r.current_node_uuid,
	modified_on = NOW()
FROM (
	VALUES(:uuid::uuid, :status, :exited_on::timestamptz, :responded::bool, :results, :path_nodes::uuid[], :path_times::timestamptz[], :current_node_uuid::uuid)
) AS
	r(uuid, status, exited_on, responded, results, path_nodes, path_times, current_node_uuid)
WHERE
	fr.uuid = r.uuid`

func UpdateRuns(ctx context.Context, tx *sqlx.Tx, runs []*FlowRun) error {
	if err := BulkQuery(ctx, "update runs", tx, sqlUpdateRun, runs); err != nil {
		return fmt.Errorf("error updating runs: %w", err)
	}
	return nil
}

// GetContactIDsAtNode returns the ids of contacts currently waiting or active at the given flow node
func GetContactIDsAtNode(ctx context.Context, rt *runtime.Runtime, orgID OrgID, nodeUUID flows.NodeUUID) ([]ContactID, error) {
	rows, err := rt.ReadonlyDB.QueryContext(ctx,
		`SELECT contact_id FROM flows_flowrun WHERE org_id = $1 AND current_node_uuid = $2 AND status IN ('A' , 'W')`, orgID, nodeUUID,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("error querying contacts at node: %w", err)
	}
	defer rows.Close()

	contactIDs := make([]ContactID, 0, 10)

	for rows.Next() {
		var id ContactID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("error scanning contact id: %w", err)
		}
		contactIDs = append(contactIDs, id)
	}

	return contactIDs, nil
}

const sqlSelectContactFlowHistory = `
SELECT contact_id, ARRAY_AGG(DISTINCT flow_id ORDER BY flow_id) FROM flows_flowrun WHERE contact_id = ANY($1) GROUP BY contact_id`

// GetContactFlowHistory returns the distinct flow IDs that each contact has run, keyed by contact ID.
func GetContactFlowHistory(ctx context.Context, db *sqlx.DB, contactIDs []ContactID) (map[ContactID][]FlowID, error) {
	result := make(map[ContactID][]FlowID, len(contactIDs))
	if len(contactIDs) == 0 {
		return result, nil
	}

	rows, err := db.QueryContext(ctx, sqlSelectContactFlowHistory, pq.Array(contactIDs))
	if err != nil {
		return nil, fmt.Errorf("error querying flow history: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var contactID ContactID
		var flowIDs pq.Int64Array
		if err := rows.Scan(&contactID, &flowIDs); err != nil {
			return nil, fmt.Errorf("error scanning flow history: %w", err)
		}
		ids := make([]FlowID, len(flowIDs))
		for i, id := range flowIDs {
			ids[i] = FlowID(id)
		}
		result[contactID] = ids
	}

	return result, rows.Err()
}

type RunReference struct {
	UUID flows.RunUUID
	Flow *assets.FlowReference
}

const sqlSelectActiveAndWaitingRuns = `
    SELECT r.session_uuid, r.uuid AS uuid, f.uuid AS flow_uuid, f.name AS flow_name
      FROM flows_flowrun r
INNER JOIN flows_flow f ON f.id = r.flow_id
     WHERE session_uuid = ANY($1) AND status IN ('A', 'W')
	 ORDER BY r.id`

// GetActiveAndWaitingRuns gets references to the active/waiting runs for the given sessions
func GetActiveAndWaitingRuns(ctx context.Context, rt *runtime.Runtime, sessionUUIDs []flows.SessionUUID) (map[flows.SessionUUID][]*RunReference, error) {
	type envelope struct {
		SessionUUID flows.SessionUUID `db:"session_uuid"`
		UUID        flows.RunUUID     `db:"uuid"`
		FlowUUID    assets.FlowUUID   `db:"flow_uuid"`
		FlowName    string            `db:"flow_name"`
	}

	var all []*envelope

	if err := rt.DB.SelectContext(ctx, &all, sqlSelectActiveAndWaitingRuns, pq.Array(sessionUUIDs)); err != nil {
		return nil, fmt.Errorf("error fetching ongoing runs: %w", err)
	}

	runRefs := make(map[flows.SessionUUID][]*RunReference, len(sessionUUIDs))
	for _, r := range all {
		runRefs[r.SessionUUID] = append(runRefs[r.SessionUUID], &RunReference{UUID: r.UUID, Flow: assets.NewFlowReference(r.FlowUUID, r.FlowName)})
	}

	return runRefs, nil
}
