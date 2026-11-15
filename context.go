package workflow

import (
	"context"
	"errors"
	"fmt"
)

type contextKey string

const (
	contextKeyConditionID    contextKey = "condition"
	contextKeyWorkflowNodeID contextKey = "node_id"
	contextKeyNodeStatus     contextKey = "node_status"
	contextKeyWorkflowRun    contextKey = "workflow_run"
)

func withCondition(ctx context.Context, condition *Condition) context.Context {
	return context.WithValue(ctx, contextKeyConditionID, condition)
}

func extractCondition(ctx context.Context) (*Condition, error) {
	condition, ok := ctx.Value(contextKeyConditionID).(*Condition)
	if !ok || condition == nil {
		return nil, errors.New("failed to extract condition from context")
	}
	return condition, nil
}

func withWorkflowRun(ctx context.Context, workflowRun *workflowRun) context.Context {
	return context.WithValue(ctx, contextKeyWorkflowRun, workflowRun)
}

func extractWorkflowRun(ctx context.Context) (*workflowRun, error) {
	workflowRun, ok := ctx.Value(contextKeyWorkflowRun).(*workflowRun)
	if !ok {
		return nil, errors.New("failed to extract workflow run")
	}
	return workflowRun, nil
}

func withNodeID(ctx context.Context, nodeID string) context.Context {
	return context.WithValue(ctx, contextKeyWorkflowNodeID, nodeID)
}

func extractNodeID(ctx context.Context) (string, error) {
	nodeID, ok := ctx.Value(contextKeyWorkflowNodeID).(string)
	if !ok {
		return "", errors.New("failed to extract node ID from context")
	}
	return nodeID, nil
}

func withNodeStatus(ctx context.Context, status *NodeStatus) context.Context {
	return context.WithValue(ctx, contextKeyNodeStatus, status)
}

func extractNodeStatus(ctx context.Context) (*NodeStatus, error) {
	status, ok := ctx.Value(contextKeyNodeStatus).(*NodeStatus)
	if !ok || status == nil {
		return nil, fmt.Errorf("failed to extract node status from context")
	}
	return status, nil
}
