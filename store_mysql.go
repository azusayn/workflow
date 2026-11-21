package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var _ WorkflowStore = (*MySQLStore)(nil)

type mysqlStoreTxKey struct{}

type MySQLStore struct {
	db *sql.DB
}

func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{db: db}
}

// Schema returns CREATE TABLE statements for the MySQL store.
func (s *MySQLStore) Schema() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS workflows (
    id VARCHAR(64) NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    total_nodes INT NOT NULL,
    node_configs TEXT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS workflow_runs (
    id VARCHAR(64) NOT NULL PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL,
    data TEXT NOT NULL,
    completed_nodes INT NOT NULL DEFAULT 0,
    trace_carrier TEXT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    KEY idx_workflow_runs_pending (workflow_id, completed_nodes)
)`,
		`CREATE TABLE IF NOT EXISTS node_executions (
    workflow_run_id VARCHAR(64) NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    ready_in_degree INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    err TEXT NULL,
    updated_at DATETIME(3) NOT NULL,
    PRIMARY KEY (workflow_run_id, node_id)
)`,
		`CREATE TABLE IF NOT EXISTS subscriber_events (
    subscriber_id VARCHAR(512) NOT NULL PRIMARY KEY,
    event_ids TEXT NOT NULL,
    updated_at DATETIME(3) NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS workflow_conditions (
    subscriber_id VARCHAR(512) NOT NULL PRIMARY KEY,
    condition_id VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    updated_at DATETIME(3) NOT NULL
)`,
	}
}

type mysqlQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *MySQLStore) querier(ctx context.Context) mysqlQuerier {
	if tx, ok := ctx.Value(mysqlStoreTxKey{}).(*sql.Tx); ok && tx != nil {
		return tx
	}
	return s.db
}

func (s *MySQLStore) Transaction(ctx context.Context, fn func(context.Context) error) error {
	if _, ok := ctx.Value(mysqlStoreTxKey{}).(*sql.Tx); ok {
		return fn(ctx)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}

	txCtx := context.WithValue(ctx, mysqlStoreTxKey{}, tx)
	if err := fn(txCtx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}
	return nil
}

func (s *MySQLStore) LoadSubscriberMap(ctx context.Context) (map[string]map[string]struct{}, error) {
	q := s.querier(ctx)
	rows, err := q.QueryContext(ctx, `
SELECT subscriber_id, event_ids FROM subscriber_events
`)
	if err != nil {
		return nil, fmt.Errorf("load subscriber map failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	out := make(map[string]map[string]struct{})
	for rows.Next() {
		var subscriberID string
		var raw []byte
		if err := rows.Scan(&subscriberID, &raw); err != nil {
			return nil, err
		}
		ids, err := unmarshalEventIDs(raw)
		if err != nil {
			return nil, err
		}
		set := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			set[id] = struct{}{}
		}
		out[subscriberID] = set
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQLStore) UpsertSubscriberMap(ctx context.Context, subscriberID string, eventIDs []string) error {
	q := s.querier(ctx)
	if eventIDs == nil {
		eventIDs = []string{}
	}
	raw, err := json.Marshal(eventIDs)
	if err != nil {
		return fmt.Errorf("marshal event ids failed: %w", err)
	}
	now := time.Now().UTC()
	if _, err := q.ExecContext(ctx, `
INSERT INTO subscriber_events (subscriber_id, event_ids, updated_at)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE event_ids = VALUES(event_ids), updated_at = VALUES(updated_at)
`, subscriberID, raw, now); err != nil {
		return fmt.Errorf("upsert subscriber map failed: %w", err)
	}
	return nil
}

func (s *MySQLStore) LoadConditions(ctx context.Context) (map[string]ConditionModel, error) {
	q := s.querier(ctx)
	rows, err := q.QueryContext(ctx, `
SELECT subscriber_id, condition_id, event_id, payload FROM workflow_conditions
`)
	if err != nil {
		return nil, fmt.Errorf("load conditions failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	out := make(map[string]ConditionModel)
	for rows.Next() {
		var subscriberID string
		var model ConditionModel
		var payload []byte
		if err := rows.Scan(&subscriberID, &model.ID, &model.EventID, &payload); err != nil {
			return nil, err
		}
		model.Payload = append(json.RawMessage(nil), payload...)
		out[subscriberID] = model
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQLStore) UpsertCondition(ctx context.Context, subscriberID, conditionID, eventID string, payload json.RawMessage) error {
	q := s.querier(ctx)
	now := time.Now().UTC()
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	if _, err := q.ExecContext(ctx, `
INSERT INTO workflow_conditions (subscriber_id, condition_id, event_id, payload, updated_at)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    condition_id = VALUES(condition_id),
    event_id = VALUES(event_id),
    payload = VALUES(payload),
    updated_at = VALUES(updated_at)
`, subscriberID, conditionID, eventID, []byte(payload), now); err != nil {
		return fmt.Errorf("upsert condition failed: %w", err)
	}
	return nil
}

func (s *MySQLStore) DeleteCondition(ctx context.Context, subscriberID string) error {
	q := s.querier(ctx)
	if _, err := q.ExecContext(ctx, `
DELETE FROM workflow_conditions WHERE subscriber_id = ?
`, subscriberID); err != nil {
		return fmt.Errorf("delete condition failed: %w", err)
	}
	return nil
}

func (s *MySQLStore) GetOrCreateWorkflow(ctx context.Context, name string, nodeConfigs map[string]*NodeConfig) (*WorkflowModel, error) {
	q := s.querier(ctx)

	configsRaw, err := marshalNodeConfigs(nodeConfigs)
	if err != nil {
		return nil, err
	}
	totalNodes := len(nodeConfigs)
	now := time.Now().UTC()

	var id string
	err = q.QueryRowContext(ctx, `
SELECT id FROM workflows WHERE name = ?
`, name).Scan(&id)
	switch {
	case err == nil:
		if _, err := q.ExecContext(ctx, `
UPDATE workflows
SET total_nodes = ?, node_configs = ?, updated_at = ?
WHERE id = ?
`, totalNodes, configsRaw, now, id); err != nil {
			return nil, fmt.Errorf("update workflow failed: %w", err)
		}
		return &WorkflowModel{
			ID:          id,
			Name:        name,
			TotalNodes:  totalNodes,
			NodeConfigs: nodeConfigs,
		}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("query workflow failed: %w", err)
	}

	id = uuid.NewString()
	if _, err := q.ExecContext(ctx, `
INSERT INTO workflows (id, name, total_nodes, node_configs, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
`, id, name, totalNodes, configsRaw, now, now); err != nil {
		// Concurrent create: load winner and overwrite configs.
		var existingID string
		loadErr := q.QueryRowContext(ctx, `
SELECT id FROM workflows WHERE name = ?
`, name).Scan(&existingID)
		if loadErr != nil {
			return nil, fmt.Errorf("insert workflow failed: %w", err)
		}
		if _, err := q.ExecContext(ctx, `
UPDATE workflows
SET total_nodes = ?, node_configs = ?, updated_at = ?
WHERE id = ?
`, totalNodes, configsRaw, now, existingID); err != nil {
			return nil, fmt.Errorf("update workflow failed: %w", err)
		}
		return &WorkflowModel{
			ID:          existingID,
			Name:        name,
			TotalNodes:  totalNodes,
			NodeConfigs: nodeConfigs,
		}, nil
	}

	return &WorkflowModel{
		ID:          id,
		Name:        name,
		TotalNodes:  totalNodes,
		NodeConfigs: nodeConfigs,
	}, nil
}

func (s *MySQLStore) ListWorkflows(ctx context.Context, ids []string) ([]*WorkflowModel, error) {
	out := make([]*WorkflowModel, 0, len(ids))
	if len(ids) == 0 {
		return out, nil
	}

	q := s.querier(ctx)
	placeholders, args := inPlaceholders(ids)
	rows, err := q.QueryContext(ctx, `
SELECT id, name, total_nodes, node_configs
FROM workflows
WHERE id IN (`+placeholders+`)
`, args...)
	if err != nil {
		return nil, fmt.Errorf("list workflows failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	for rows.Next() {
		var (
			model      WorkflowModel
			configsRaw []byte
		)
		if err := rows.Scan(&model.ID, &model.Name, &model.TotalNodes, &configsRaw); err != nil {
			return nil, err
		}
		configs, err := unmarshalNodeConfigs(configsRaw)
		if err != nil {
			return nil, err
		}
		model.NodeConfigs = configs
		out = append(out, &model)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQLStore) GetWorkflowRun(ctx context.Context, workflowRunID string) (*WorkflowRunModel, error) {
	q := s.querier(ctx)

	row := q.QueryRowContext(ctx, `
SELECT id, workflow_id, data, completed_nodes, trace_carrier, created_at, updated_at
FROM workflow_runs
WHERE id = ?
`, workflowRunID)

	run, err := scanWorkflowRunRow(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("workflow run %q not found", workflowRunID)
		}
		return nil, fmt.Errorf("get workflow run failed: %w", err)
	}

	executions, err := s.loadNodeExecutions(ctx, q, []string{run.ID})
	if err != nil {
		return nil, err
	}
	run.NodeExecutions = executions[run.ID]
	if run.NodeExecutions == nil {
		run.NodeExecutions = make(map[string]*NodeExecution)
	}
	return run, nil
}

func (s *MySQLStore) ListPendingWorkflowRuns(ctx context.Context) ([]*WorkflowRunModel, error) {
	q := s.querier(ctx)

	rows, err := q.QueryContext(ctx, `
SELECT r.id, r.workflow_id, r.data, r.completed_nodes, r.trace_carrier, r.created_at, r.updated_at
FROM workflow_runs r
JOIN workflows w ON w.id = r.workflow_id
WHERE w.total_nodes > r.completed_nodes
ORDER BY r.created_at ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list pending workflow runs failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var runs []*WorkflowRunModel
	var runIDs []string
	for rows.Next() {
		run, err := scanWorkflowRunRow(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
		runIDs = append(runIDs, run.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return runs, nil
	}

	executions, err := s.loadNodeExecutions(ctx, q, runIDs)
	if err != nil {
		return nil, err
	}
	for _, run := range runs {
		run.NodeExecutions = executions[run.ID]
		if run.NodeExecutions == nil {
			run.NodeExecutions = make(map[string]*NodeExecution)
		}
	}
	return runs, nil
}

func (s *MySQLStore) CreateWorkflowRun(ctx context.Context, workflowRun *WorkflowRunModel) (string, error) {
	if workflowRun == nil {
		return "", errors.New("nil workflow run")
	}
	q := s.querier(ctx)

	id := workflowRun.ID
	if id == "" {
		id = uuid.NewString()
	}

	data := workflowRun.Data
	if data == nil {
		data = json.RawMessage("[]")
	}
	carrier := workflowRun.TraceCarrier
	if carrier == nil {
		carrier = map[string]string{}
	}
	carrierBytes, err := json.Marshal(carrier)
	if err != nil {
		return "", fmt.Errorf("marshal trace carrier failed: %w", err)
	}
	now := time.Now().UTC()
	completed := workflowRun.CompletedNodes

	if _, err := q.ExecContext(ctx, `
INSERT INTO workflow_runs (id, workflow_id, data, completed_nodes, trace_carrier, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, id, workflowRun.WorkflowID, []byte(data), completed, carrierBytes, now, now); err != nil {
		return "", fmt.Errorf("insert workflow run failed: %w", err)
	}

	for nodeID, exec := range workflowRun.NodeExecutions {
		if exec == nil {
			exec = &NodeExecution{Status: NodeStatusPending}
		}
		if err := s.upsertNodeExecution(ctx, q, id, nodeID, exec); err != nil {
			return "", err
		}
	}

	return id, nil
}

func (s *MySQLStore) UpdateWorkflowRun(ctx context.Context, input UpdateWorkflowRunInput) error {
	q := s.querier(ctx)
	now := time.Now().UTC()

	if input.Data != nil {
		res, err := q.ExecContext(ctx, `
UPDATE workflow_runs SET data = ?, updated_at = ? WHERE id = ?
`, []byte(*input.Data), now, input.WorkflowRunID)
		if err != nil {
			return fmt.Errorf("update workflow run data failed: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("workflow run %q not found", input.WorkflowRunID)
		}
	}

	if input.NodeID != nil && input.Execution != nil {
		if err := s.upsertNodeExecution(ctx, q, input.WorkflowRunID, *input.NodeID, input.Execution); err != nil {
			return err
		}
	}

	if input.IncrementCompleted {
		res, err := q.ExecContext(ctx, `
UPDATE workflow_runs
SET completed_nodes = completed_nodes + 1, updated_at = ?
WHERE id = ?
`, now, input.WorkflowRunID)
		if err != nil {
			return fmt.Errorf("increment completed nodes failed: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("workflow run %q not found", input.WorkflowRunID)
		}
	} else if input.Data == nil && (input.NodeID == nil || input.Execution == nil) {
		return nil
	} else if input.Data == nil {
		_, err := q.ExecContext(ctx, `
UPDATE workflow_runs SET updated_at = ? WHERE id = ?
`, now, input.WorkflowRunID)
		if err != nil {
			return fmt.Errorf("touch workflow run failed: %w", err)
		}
	}

	return nil
}

func (s *MySQLStore) upsertNodeExecution(ctx context.Context, q mysqlQuerier, runID, nodeID string, exec *NodeExecution) error {
	var errText sql.NullString
	if exec.Err != nil {
		errText = sql.NullString{String: exec.Err.Error(), Valid: true}
	}
	now := time.Now().UTC()

	if _, err := q.ExecContext(ctx, `
INSERT INTO node_executions (workflow_run_id, node_id, ready_in_degree, status, err, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    ready_in_degree = VALUES(ready_in_degree),
    status = VALUES(status),
    err = VALUES(err),
    updated_at = VALUES(updated_at)
`, runID, nodeID, exec.ReadyInDegree, string(exec.Status), errText, now); err != nil {
		return fmt.Errorf("upsert node execution failed: %w", err)
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanWorkflowRunRow(row rowScanner) (*WorkflowRunModel, error) {
	var (
		run          WorkflowRunModel
		data         []byte
		traceCarrier []byte
		createdAt    time.Time
		updatedAt    time.Time
	)
	if err := row.Scan(&run.ID, &run.WorkflowID, &data, &run.CompletedNodes, &traceCarrier, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	run.Data = append(json.RawMessage(nil), data...)
	run.CreatedAt = createdAt
	run.UpdatedAt = updatedAt
	if len(traceCarrier) > 0 {
		if err := json.Unmarshal(traceCarrier, &run.TraceCarrier); err != nil {
			return nil, fmt.Errorf("unmarshal trace carrier failed: %w", err)
		}
	}
	if run.TraceCarrier == nil {
		run.TraceCarrier = map[string]string{}
	}
	return &run, nil
}

func (s *MySQLStore) loadNodeExecutions(ctx context.Context, q mysqlQuerier, runIDs []string) (map[string]map[string]*NodeExecution, error) {
	out := make(map[string]map[string]*NodeExecution, len(runIDs))
	if len(runIDs) == 0 {
		return out, nil
	}

	placeholders, args := inPlaceholders(runIDs)
	rows, err := q.QueryContext(ctx, `
SELECT workflow_run_id, node_id, ready_in_degree, status, err
FROM node_executions
WHERE workflow_run_id IN (`+placeholders+`)
`, args...)
	if err != nil {
		return nil, fmt.Errorf("load node executions failed: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	for rows.Next() {
		var (
			runID         string
			nodeID        string
			readyInDegree int
			status        string
			errText       sql.NullString
		)
		if err := rows.Scan(&runID, &nodeID, &readyInDegree, &status, &errText); err != nil {
			return nil, err
		}
		exec := &NodeExecution{
			ReadyInDegree: readyInDegree,
			Status:        NodeStatus(status),
		}
		if errText.Valid {
			exec.Err = errors.New(errText.String)
		}
		if out[runID] == nil {
			out[runID] = make(map[string]*NodeExecution)
		}
		out[runID][nodeID] = exec
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func inPlaceholders(ids []string) (string, []any) {
	parts := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		parts[i] = "?"
		args[i] = id
	}
	return strings.Join(parts, ","), args
}

func marshalNodeConfigs(configs map[string]*NodeConfig) ([]byte, error) {
	if configs == nil {
		return []byte("{}"), nil
	}
	raw, err := json.Marshal(configs)
	if err != nil {
		return nil, fmt.Errorf("marshal node configs failed: %w", err)
	}
	return raw, nil
}

func unmarshalNodeConfigs(raw []byte) (map[string]*NodeConfig, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]*NodeConfig{}, nil
	}
	var configs map[string]*NodeConfig
	if err := json.Unmarshal(raw, &configs); err != nil {
		return nil, fmt.Errorf("unmarshal node configs failed: %w", err)
	}
	if configs == nil {
		configs = map[string]*NodeConfig{}
	}
	return configs, nil
}

func unmarshalEventIDs(raw []byte) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []string{}, nil
	}
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, fmt.Errorf("unmarshal event ids failed: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}
