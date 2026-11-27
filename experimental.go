package workflow

import (
	"context"
	"fmt"

	"github.com/azusayn/workflow/store"
)

func (e *Engine) RetryFailedNode(ctx context.Context, workflowRunID, nodeID string) error {
	workflowRunModel, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return err
	}

	run, err := e.getWorkflowRun(workflowRunID)
	if err != nil {
		run, err = e.loadWorkflowRun(ctx, workflowRunModel)
		if err != nil {
			return err
		}
	}

	if err := run.restoreSuccessors(ctx, nodeID); err != nil {
		return err
	}

	ctx = e.propagator.Extract(ctx, workflowRunModel.TraceCarrier)
	e.readyCh <- &readyNodeTask{
		workflowRunID: workflowRunID,
		nodeID:        nodeID,
		ctx:           ctx,
	}

	return nil
}

func (e *Engine) loadWorkflowRun(ctx context.Context, model *store.WorkflowRun) (*workflowRun, error) {
	workflows, err := e.store.ListWorkflows(ctx, []string{model.WorkflowID})
	if err != nil {
		return nil, err
	}
	if len(workflows) == 0 {
		return nil, fmt.Errorf("failed to load workflow %q for run %q", model.WorkflowID, model.ID)
	}
	nodeConfigs := convertToNodeConfigs(workflows[0].NodeConfigs)
	pool := NewVariablePool(nodeConfigs)
	if err := pool.Load(model.Data); err != nil {
		return nil, fmt.Errorf("failed to load variable pool for run %q: %w", model.ID, err)
	}
	run := newWorkflowRun(
		e,
		model.ID,
		pool,
		convertToNodeExecutions(model.NodeExecutions),
		nodeConfigs,
	)

	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.activeRuns[model.ID]; ok {
		return existing, nil
	}
	e.activeRuns[model.ID] = run
	return run, nil
}

func (r *workflowRun) restoreSuccessors(ctx context.Context, nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	exec, ok := r.executions[nodeID]
	if !ok {
		return fmt.Errorf("failed to get node execution for node %q", nodeID)
	}
	if exec.Status != NodeStatusFailed {
		return fmt.Errorf("cannot retry node %q which is not failed", nodeID)
	}

	exec.Status = NodeStatusPending
	exec.Err = nil
	if err := r.engine.store.UpdateWorkflowRun(ctx, convertToStoreUpdateWorkflowRunInput(r.id, &nodeID, exec, nil, false)); err != nil {
		return err
	}

	restoreNodes := make([]string, 0)
	var dfs func(string) error
	dfs = func(id string) error {
		config, ok := r.nodeConfigs[id]
		if !ok {
			return fmt.Errorf("failed to get config for node %q", id)
		}
		for _, nextID := range config.NextNodeIDs {
			restoreNodes = append(restoreNodes, nextID)
			if err := dfs(nextID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := dfs(nodeID); err != nil {
		return err
	}

	for _, id := range restoreNodes {
		exec, ok := r.executions[id]
		if !ok {
			return fmt.Errorf("failed to get execution for node %q", id)
		}
		exec.Status = NodeStatusPending
		exec.Err = nil

		updateWorkflowRunInput := convertToStoreUpdateWorkflowRunInput(r.id, &id, exec, nil, false)
		if err := r.engine.store.UpdateWorkflowRun(ctx, updateWorkflowRunInput); err != nil {
			return err
		}
	}

	return nil
}
