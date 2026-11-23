package store

import (
	"context"
	"encoding/json"
	"time"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "pending"
	NodeStatusFailed    NodeStatus = "failed"
	NodeStatusPause     NodeStatus = "pause"
	NodeStatusCompleted NodeStatus = "completed"
	NodeStatusSkipped   NodeStatus = "skipped"
)

type NodeInputConfig map[string]string

type NodeConfig struct {
	Inputs          NodeInputConfig
	InDegree        int
	NextNodeIDs     []string
	ContinueOnError bool
}

type Workflow struct {
	ID          string
	Name        string
	TotalNodes  int
	NodeConfigs map[string]*NodeConfig
}

type WorkflowRun struct {
	ID         string
	WorkflowID string
	Data       json.RawMessage
	// NodeExecutions maps node ID to node status.
	NodeExecutions map[string]*NodeExecution
	CompletedNodes int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	TraceCarrier   map[string]string
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

// Condition is a persisted wait condition keyed by subscriber ID (workflowRunID:nodeID).
type Condition struct {
	ID      string
	EventID string
	Payload json.RawMessage
}

type Transaction interface {
	Transaction(context.Context, func(context.Context) error) error
}

type Store interface {
	Transaction
	LoadSubscriberMap(ctx context.Context) (map[string]map[string]struct{}, error)
	UpsertSubscriberMap(ctx context.Context, subscriberID string, eventIDs []string) error

	LoadConditions(ctx context.Context) (map[string]Condition, error)
	UpsertCondition(ctx context.Context, subscriberID, conditionID, eventID string, payload json.RawMessage) error
	DeleteCondition(ctx context.Context, subscriberID string) error

	GetOrCreateWorkflow(ctx context.Context, name string, nodeConfigs map[string]*NodeConfig) (*Workflow, error)
	ListWorkflows(ctx context.Context, ids []string) ([]*Workflow, error)

	GetWorkflowRun(ctx context.Context, workflowRunID string) (*WorkflowRun, error)
	ListPendingWorkflowRuns(ctx context.Context) ([]*WorkflowRun, error)
	CreateWorkflowRun(ctx context.Context, workflowRun *WorkflowRun) (string, error)
	UpdateWorkflowRun(ctx context.Context, input UpdateWorkflowRunInput) error
}
