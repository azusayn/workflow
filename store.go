package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type WorkflowModel struct {
	ID          string
	Name        string
	TotalNodes  int
	NodeConfigs map[string]*NodeConfig
}

type workflowRunModel struct {
	ID         string
	WorkflowID string
	Data       json.RawMessage
	// NodeExecutions maps node ID to node status.
	NodeExecutions map[string]*NodeExecution
	CompletedNodes int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type NodeExecution struct {
	ReadyInDegree int
	Err           error
	Status        NodeStatus
}

type UpdateWorkflowRunInput struct {
	WorkflowRunID      string
	NodeID             *string
	Execution          *NodeExecution
	Data               *json.RawMessage
	IncrementCompleted bool
}

// ConditionModel is a persisted wait condition keyed by subscriber ID (workflowRunID:nodeID).
type ConditionModel struct {
	ID      string
	EventID string
	Payload json.RawMessage
}

type Transaction interface {
	Transaction(context.Context, func(context.Context) error) error
}

type WorkflowStore interface {
	Transaction
	LoadSubscriberMap(ctx context.Context) (map[string]map[string]struct{}, error)
	UpsertSubscriberMap(ctx context.Context, subscriberID string, eventIDs []string) error

	LoadConditions(ctx context.Context) (map[string]ConditionModel, error)
	UpsertCondition(ctx context.Context, subscriberID, conditionID, eventID string, payload json.RawMessage) error
	DeleteCondition(ctx context.Context, subscriberID string) error

	GetOrCreateWorkflow(ctx context.Context, name string, nodeConfigs map[string]*NodeConfig) (*WorkflowModel, error)
	ListWorkflows(ctx context.Context, ids []string) ([]*WorkflowModel, error)

	GetWorkflowRun(ctx context.Context, workflowRunID string) (*workflowRunModel, error)
	ListPendingWorkflowRuns(ctx context.Context) ([]*workflowRunModel, error)
	CreateWorkflowRun(ctx context.Context, workflowRun *workflowRunModel) (string, error)
	UpdateWorkflowRun(ctx context.Context, input UpdateWorkflowRunInput) error
}

var _ WorkflowStore = (*noopStore)(nil)

type noopStore struct{}

func newNoopStore() WorkflowStore {
	return &noopStore{}
}

func (s *noopStore) Transaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (s *noopStore) LoadSubscriberMap(context.Context) (map[string]map[string]struct{}, error) {
	return map[string]map[string]struct{}{}, nil
}

func (s *noopStore) UpsertSubscriberMap(context.Context, string, []string) error {
	return nil
}

func (s *noopStore) LoadConditions(context.Context) (map[string]ConditionModel, error) {
	return map[string]ConditionModel{}, nil
}

func (s *noopStore) UpsertCondition(context.Context, string, string, string, json.RawMessage) error {
	return nil
}

func (s *noopStore) DeleteCondition(context.Context, string) error {
	return nil
}

func (s *noopStore) GetOrCreateWorkflow(context.Context, string, map[string]*NodeConfig) (*WorkflowModel, error) {
	return &WorkflowModel{ID: uuid.NewString()}, nil
}

func (s *noopStore) ListWorkflows(context.Context, []string) ([]*WorkflowModel, error) {
	return nil, nil
}

func (s *noopStore) GetWorkflowRun(context.Context, string) (*workflowRunModel, error) {
	return nil, errors.New("unimplemented")
}

func (s *noopStore) ListPendingWorkflowRuns(context.Context) ([]*workflowRunModel, error) {
	return nil, nil
}

func (s *noopStore) CreateWorkflowRun(_ context.Context, _ *workflowRunModel) (string, error) {
	return uuid.NewString(), nil
}

func (s *noopStore) UpdateWorkflowRun(context.Context, UpdateWorkflowRunInput) error {
	return nil
}
