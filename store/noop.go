package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
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
	return &Workflow{ID: uuid.NewString()}, nil
}

func (s *noopStore) ListWorkflows(context.Context, []string) ([]*Workflow, error) {
	return nil, nil
}

func (s *noopStore) GetWorkflowRun(context.Context, string) (*WorkflowRun, error) {
	return nil, errors.New("unimplemented")
}

func (s *noopStore) ListPendingWorkflowRuns(context.Context) ([]*WorkflowRun, error) {
	return nil, nil
}

func (s *noopStore) CreateWorkflowRun(_ context.Context, _ *WorkflowRun) (string, error) {
	return uuid.NewString(), nil
}

func (s *noopStore) UpdateWorkflowRun(context.Context, UpdateWorkflowRunInput) error {
	return nil
}
