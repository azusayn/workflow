package store

import (
	"context"
	"encoding/json"
)

var _ Store = (*noopStore)(nil)

type noopStore struct{}

func NewNoopStore() Store {
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

func (s *noopStore) LoadConditions(context.Context) (map[string]Condition, error) {
	return map[string]Condition{}, nil
}

func (s *noopStore) UpsertCondition(context.Context, string, string, string, json.RawMessage) error {
	return nil
}

func (s *noopStore) DeleteCondition(context.Context, string) error {
	return nil
}

func (s *noopStore) GetOrCreateWorkflow(context.Context, string, map[string]*NodeConfig) (*Workflow, error) {
	id, err := UUID()
	if err != nil {
		return nil, err
	}
	return &Workflow{ID: id}, nil
}

func (s *noopStore) ListWorkflows(context.Context, []string) ([]*Workflow, error) {
	return nil, nil
}

func (s *noopStore) GetWorkflowRun(_ context.Context, workflowRunID string) (*WorkflowRun, error) {
	return &WorkflowRun{
		ID:             workflowRunID,
		NodeExecutions: map[string]*NodeExecution{},
		TraceCarrier:   map[string]string{},
	}, nil
}

func (s *noopStore) ListPendingWorkflowRuns(context.Context) ([]*WorkflowRun, error) {
	return nil, nil
}

func (s *noopStore) CreateWorkflowRun(_ context.Context, _ *WorkflowRun) (string, error) {
	id, err := UUID()
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *noopStore) UpdateWorkflowRun(context.Context, UpdateWorkflowRunInput) error {
	return nil
}
