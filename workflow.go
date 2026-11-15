package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	readyChannelCapacity         = 2000
	workflowEventChannelCapacity = 1000
	defaultNodeExecutorNum       = 4
	// TODO: support per-node timeout policy.
	defaultNodeExecutionTimeout   = 3 * time.Second
	defaultConditionCheckTimout   = 1 * time.Second
	defaultConditionCheckInterval = 1 * time.Second
)

type Engine struct {
	store                WorkflowStore
	logger               *slog.Logger
	nodeExecutorNum      int
	nodeExecutionTimeout time.Duration
	readyCh              chan *readyNodeContext
	eventCh              chan *WorkflowEvent
	activeRuns           map[string]*workflowRun
	subscriberMap        subscriberMap
	callbackByEventID    map[string]func(context.Context, any) error
	activeConditions     map[string]Condition
	checkerByConditionID map[string]func(context.Context, any) (bool, error)
	mu                   sync.Mutex
}

type EngineOption func(*Engine)

type Workflow struct {
	ID   string
	Name string
	// NodeConfigs maps node ID to its node config.
	NodeConfigs map[string]*NodeConfig
	engine      *Engine
	logger      *slog.Logger
}

type NodeInputConfig map[string]string

type NodeConfig struct {
	Inputs          NodeInputConfig
	InDegree        int
	NextNodeIDs     []string
	ContinueOnError bool
}

type WorkflowRunStatus struct {
	ID             string
	WorkflowID     string
	NodeExecutions map[string]NodeExecution
	CompletedNodes int
}

type readyNodeContext struct {
	workflowRunID string
	nodeID        string
}

type workflowRun struct {
	engine      *Engine
	id          string
	pool        *VariablePool
	executions  map[string]*NodeExecution
	nodeConfigs map[string]*NodeConfig
	// TODO: use a dedicated struct to to manage workflow run locks.
	mu sync.Mutex
}

func WithLogger(logger *slog.Logger) EngineOption {
	return func(e *Engine) {
		e.logger = logger
	}
}

func WithNodeExecutorNum(executorNum int) EngineOption {
	return func(e *Engine) {
		e.nodeExecutorNum = executorNum
	}
}

func WithNodeExecutionTimeout(duration time.Duration) EngineOption {
	return func(e *Engine) {
		e.nodeExecutionTimeout = duration
	}
}

func WithStore(store WorkflowStore) EngineOption {
	return func(e *Engine) {
		e.store = store
	}
}

func NewEngine(opts ...EngineOption) *Engine {
	e := &Engine{
		store:                newNoopStore(),
		nodeExecutionTimeout: defaultNodeExecutionTimeout,
		logger:               slog.Default(),
		nodeExecutorNum:      defaultNodeExecutorNum,
		readyCh:              make(chan *readyNodeContext, readyChannelCapacity),
		eventCh:              make(chan *WorkflowEvent, workflowEventChannelCapacity),
		activeRuns:           make(map[string]*workflowRun),
		callbackByEventID:    make(map[string]func(context.Context, any) error),
		subscriberMap:        make(map[string]map[string]struct{}),
		activeConditions:     make(map[string]Condition),
		checkerByConditionID: make(map[string]func(context.Context, any) (bool, error)),
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

func (e *Engine) CreateWorkflow(ctx context.Context, name string, nodeConfigs map[string]*NodeConfig) (*Workflow, error) {
	for nodeID, nodeConfig := range nodeConfigs {
		for _, nextNodeID := range nodeConfig.NextNodeIDs {
			nextNodeConfig, ok := nodeConfigs[nextNodeID]
			if !ok {
				return nil, fmt.Errorf("node ID does not exist %q", nodeID)
			}
			nextNodeConfig.InDegree++
		}
	}

	workflowModel, err := e.store.GetOrCreateWorkflow(ctx, name, nodeConfigs)
	if err != nil {
		return nil, err
	}

	w := &Workflow{
		engine:      e,
		ID:          workflowModel.ID,
		Name:        name,
		NodeConfigs: nodeConfigs,
		logger:      e.logger.With(slog.Any("workflow_name", name)),
	}

	return w, nil
}

func (e *Engine) GetWorkflowRun(ctx context.Context, workflowRunID string) (*WorkflowRunStatus, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	model, err := e.store.GetWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return nil, err
	}
	status := &WorkflowRunStatus{
		ID:             model.ID,
		WorkflowID:     model.WorkflowID,
		CompletedNodes: model.CompletedNodes,
		NodeExecutions: make(map[string]NodeExecution, len(model.NodeExecutions)),
	}
	for nodeID, exec := range model.NodeExecutions {
		if exec == nil {
			continue
		}
		status.NodeExecutions[nodeID] = *exec
	}
	return status, nil
}

func (e *Engine) RecoverWorkflowRuns(ctx context.Context) error {
	subscriberData, err := e.store.LoadSubscriberMap(ctx)
	if err != nil {
		return err
	}
	conditionData, err := e.store.LoadConditions(ctx)
	if err != nil {
		return err
	}

	activeConditions := make(map[string]Condition, len(conditionData))
	for subscriberID, model := range conditionData {
		conditionID := model.ID
		payload := append(json.RawMessage(nil), model.Payload...)
		activeConditions[subscriberID] = Condition{
			ID:      &conditionID,
			EventID: model.EventID,
			Payload: payload,
		}
	}

	e.mu.Lock()
	e.subscriberMap = subscriberData
	e.activeConditions = activeConditions
	e.mu.Unlock()

	workflowRunModels, err := e.store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		return err
	}

	workflowIDs := make([]string, 0)
	for _, workflowRun := range workflowRunModels {
		workflowIDs = append(workflowIDs, workflowRun.WorkflowID)
	}
	nodeConfigsMap := make(map[string]map[string]*NodeConfig)
	workflows, err := e.store.ListWorkflows(ctx, workflowIDs)
	if err != nil {
		return err
	}
	for _, workflow := range workflows {
		nodeConfigsMap[workflow.ID] = workflow.NodeConfigs
	}

	for _, workflowRunModel := range workflowRunModels {
		logger := e.logger.With(slog.Any("workflow_run_id", workflowRunModel.ID))

		nodeConfigs, ok := nodeConfigsMap[workflowRunModel.WorkflowID]
		if !ok {
			logger.ErrorContext(ctx, "failed to get node configs")
			continue
		}

		pool := NewVariablePool(nodeConfigs)
		if err := pool.Load(workflowRunModel.Data); err != nil {
			logger.ErrorContext(ctx, "failed to load variable pool", slog.Any("err", err))
			continue
		}

		e.mu.Lock()
		e.activeRuns[workflowRunModel.ID] = newWorkflowRun(
			e,
			workflowRunModel.ID,
			pool,
			workflowRunModel.NodeExecutions,
			nodeConfigs,
		)
		e.mu.Unlock()

		readyNodeIDs, err := getReadyNodeIDs(nodeConfigs, workflowRunModel.NodeExecutions)
		if err != nil {
			logger.ErrorContext(ctx, "failed to get ready nodes", slog.Any("err", err))
			continue
		}

		for _, nodeID := range readyNodeIDs {
			e.readyCh <- &readyNodeContext{
				nodeID:        nodeID,
				workflowRunID: workflowRunModel.ID,
			}
		}
	}

	return nil
}

func (e *Engine) StartNodeExecutors(ctx context.Context) error {
	startExecutor := func(ctx context.Context) error {
		for {
			select {
			case nodeContext := <-e.readyCh:
				logger := e.logger.With(slog.Any("workflow_run_id", nodeContext.workflowRunID), slog.Any("node_id", nodeContext.nodeID))
				run, err := e.getWorkflowRun(nodeContext.workflowRunID)
				if err != nil {
					logger.ErrorContext(ctx, "failed to get workflow run", slog.Any("err", err))
					continue
				}

				nodeStatus, err := run.execute(ctx, nodeContext.nodeID)
				if err != nil {
					logger.ErrorContext(ctx, "failed to execute node", slog.Any("err", err))
					continue
				}

				switch nodeStatus {
				case NodeStatusPause,
					NodeStatusFailed:
					continue

				case NodeStatusCompleted:

				default:
					logger.ErrorContext(ctx, "invalid node status", slog.Any("node_status", nodeStatus))
					continue
				}

				e.checkAndPurgeInactiveRun(run)

				if err := e.schedule(ctx, run.nodeConfigs, nodeContext.workflowRunID, nodeContext.nodeID); err != nil {
					logger.ErrorContext(ctx, "failed to schedule node", slog.Any("err", err))
				}

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, e.nodeExecutorNum)

	for range e.nodeExecutorNum {
		wg.Go(func() {
			errCh <- startExecutor(ctx)
		})
	}

	wg.Wait()

	var merr error
	for range e.nodeExecutorNum {
		merr = errors.Join(merr, <-errCh)
	}

	return merr
}

func (e *Engine) getWorkflowRun(workflowRunID string) (*workflowRun, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	run, ok := e.activeRuns[workflowRunID]
	if !ok {
		return nil, errors.New("workflow run not active")
	}
	return run, nil
}

// TODO: scheduler.
func (e *Engine) schedule(ctx context.Context, nodeConfigs map[string]*NodeConfig, workflowRunID, nodeID string) error {
	nodeConfig, ok := nodeConfigs[nodeID]
	if !ok {
		return fmt.Errorf("failed to get node config for %q", nodeID)
	}

	for _, nextNodeID := range nodeConfig.NextNodeIDs {
		ready, err := e.updateAndCheckReadyInDegree(ctx, nodeConfigs, workflowRunID, nextNodeID)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		e.readyCh <- &readyNodeContext{
			workflowRunID: workflowRunID,
			nodeID:        nextNodeID,
		}
	}
	return nil
}

// updateAndCheckReadyInDegree return true if the node is ready to be executed.
func (e *Engine) updateAndCheckReadyInDegree(ctx context.Context, nodeConfigs map[string]*NodeConfig, workflowRunID, nodeID string) (bool, error) {
	run, err := e.getWorkflowRun(workflowRunID)
	if err != nil {
		return false, err
	}
	run.mu.Lock()
	defer run.mu.Unlock()

	exec, ok := run.executions[nodeID]
	if !ok {
		return false, fmt.Errorf("failed to get execution for %q", nodeID)
	}
	exec.ReadyInDegree++

	if err := e.store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
		WorkflowRunID: run.id,
		NodeID:        &nodeID,
		Execution:     exec,
	}); err != nil {
		return false, err
	}

	nodeConfig, ok := nodeConfigs[nodeID]
	if !ok {
		return false, fmt.Errorf("failed to get node config for %q", nodeID)
	}
	if exec.ReadyInDegree < nodeConfig.InDegree {
		return false, nil
	}

	return true, nil
}

func (e *Engine) checkAndPurgeInactiveRun(run *workflowRun) {
	run.mu.Lock()
	defer run.mu.Unlock()

	for _, exec := range run.executions {
		if exec.Status == NodeStatusPending || exec.Status == NodeStatusPause {
			return
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.activeRuns, run.id)
}

func (w *Workflow) Run(ctx context.Context, data map[string]any) (string, error) {
	pool := NewVariablePool(w.NodeConfigs)
	for k, v := range data {
		pool.Set(RootNodeID, k, v)
	}

	bytes, err := pool.Dump()
	if err != nil {
		return "", err
	}
	nodeExecutions := make(map[string]*NodeExecution)
	for nodeID := range w.NodeConfigs {
		nodeExecutions[nodeID] = &NodeExecution{
			Status: NodeStatusPending,
		}
	}

	workflowRunID, err := w.createWorkflowRun(ctx, pool, nodeExecutions, bytes)
	if err != nil {
		return "", err
	}

	w.engine.readyCh <- &readyNodeContext{
		workflowRunID: workflowRunID,
		nodeID:        RootNodeID,
	}

	return workflowRunID, nil
}

func (w *Workflow) createWorkflowRun(ctx context.Context, pool *VariablePool, executions map[string]*NodeExecution, data json.RawMessage) (string, error) {
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()

	var workflowRunID string
	if err := w.engine.store.Transaction(ctx, func(ctx context.Context) error {
		var err error
		workflowRunID, err = w.engine.store.CreateWorkflowRun(ctx, &workflowRunModel{
			WorkflowID:     w.ID,
			Data:           data,
			NodeExecutions: executions,
		})
		return err
	}); err != nil {
		return "", err
	}

	w.engine.activeRuns[workflowRunID] = newWorkflowRun(
		w.engine,
		workflowRunID,
		pool,
		executions,
		w.NodeConfigs,
	)

	return workflowRunID, nil
}

func newWorkflowRun(
	engine *Engine,
	id string,
	pool *VariablePool,
	executions map[string]*NodeExecution,
	nodeConfigs map[string]*NodeConfig,
) *workflowRun {
	return &workflowRun{
		engine:      engine,
		id:          id,
		pool:        pool,
		executions:  executions,
		nodeConfigs: nodeConfigs,
	}
}

func (r *workflowRun) execute(ctx context.Context, nodeID string) (NodeStatus, error) {
	nodeStatus := NodeStatusPending

	// TODO: distinguish between node types and node IDs.
	act, ok := Lookup(nodeID)
	if !ok {
		return nodeStatus, fmt.Errorf("node type %q not registered", nodeID)
	}

	condition := &Condition{}
	ctx = withNodeID(ctx, nodeID)
	ctx = withWorkflowRun(ctx, r)
	ctx = withNodeStatus(ctx, &nodeStatus)
	ctx = withCondition(ctx, condition)

	ctx, cancel := context.WithTimeout(ctx, r.engine.nodeExecutionTimeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- act.Execute(ctx)
	}()
	var execErr error
	select {
	case err := <-done:
		if err != nil {
			nodeStatus = NodeStatusFailed
			execErr = err
		}
	case <-ctx.Done():
		return NodeStatusPending, ctx.Err()
	}

	switch nodeStatus {
	case NodeStatusFailed:
		config, ok := r.nodeConfigs[nodeID]
		if !ok {
			return NodeStatusPending, fmt.Errorf("failed to find config for %q", nodeID)
		}
		if config.ContinueOnError {
			slog.WarnContext(ctx, "node exection failed but allowed to continue", slog.Any("err", execErr), slog.Any("node_id", nodeID))
			nodeStatus = NodeStatusCompleted
		}

	case NodeStatusPause:
		if err := r.engine.subscribeEvent(ctx, r.id, nodeID, *condition); err != nil {
			return nodeStatus, err
		}

	case NodeStatusPending:
		nodeStatus = NodeStatusCompleted

	default:
		return nodeStatus, fmt.Errorf("node %q entered invalid status %q", nodeID, nodeStatus)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	bytes, err := r.pool.Dump()
	if err != nil {
		return nodeStatus, err
	}
	nodeExecution := NodeExecution{
		Status: nodeStatus,
		Err:    execErr,
	}
	if err := r.engine.store.Transaction(ctx, func(ctx context.Context) error {
		if err := r.engine.store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
			WorkflowRunID:      r.id,
			NodeID:             &nodeID,
			Execution:          &nodeExecution,
			Data:               &bytes,
			IncrementCompleted: nodeStatus == NodeStatusCompleted || nodeStatus == NodeStatusFailed,
		}); err != nil {
			return err
		}
		exec, ok := r.executions[nodeID]
		if !ok || exec == nil {
			return fmt.Errorf("failed to get execution for node %q", nodeID)
		}
		*exec = nodeExecution
		if nodeStatus == NodeStatusFailed {
			return r.skipSuccessors(ctx, nodeID)
		}
		return nil
	}); err != nil {
		return nodeStatus, err
	}

	return nodeStatus, nil
}

func (r *workflowRun) skipSuccessors(ctx context.Context, nodeID string) error {
	skipNodes := make([]string, 0)

	var dfs func(nodeID string) error
	dfs = func(nodeID string) error {
		config, ok := r.nodeConfigs[nodeID]
		if !ok {
			return fmt.Errorf("failed to get config for node %q", nodeID)
		}
		for _, id := range config.NextNodeIDs {
			skipNodes = append(skipNodes, id)
			if err := dfs(id); err != nil {
				return err
			}
		}
		return nil
	}

	if err := dfs(nodeID); err != nil {
		return err
	}

	for _, nodeID := range skipNodes {
		exec, ok := r.executions[nodeID]
		if !ok {
			return fmt.Errorf("failed to get execution for node %q", nodeID)
		}
		exec.Status = NodeStatusSkipped
		if err := r.engine.store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
			WorkflowRunID:      r.id,
			NodeID:             &nodeID,
			Execution:          exec,
			IncrementCompleted: true,
		}); err != nil {
			return err
		}
	}

	return nil
}

func getReadyNodeIDs(configs map[string]*NodeConfig, executions map[string]*NodeExecution) ([]string, error) {
	readyNodeIDs := make([]string, 0)

	var dfs func(string) error
	dfs = func(nodeID string) error {
		execution, ok := executions[nodeID]
		if !ok {
			return fmt.Errorf("failed to get status for %q", nodeID)
		}

		status := execution.Status
		if status == NodeStatusPause || status == NodeStatusFailed {
			return nil
		}

		switch status {
		case NodeStatusPause,
			NodeStatusFailed:
			return nil

		case NodeStatusPending:
			readyNodeIDs = append(readyNodeIDs, nodeID)
			return nil

		default:
		}

		config, ok := configs[nodeID]
		if !ok {
			return fmt.Errorf("failed to get config for %q", nodeID)
		}

		for _, nextNodeID := range config.NextNodeIDs {
			if err := dfs(nextNodeID); err != nil {
				return err
			}
		}

		return nil
	}

	if err := dfs(RootNodeID); err != nil {
		return nil, err
	}

	return readyNodeIDs, nil
}
