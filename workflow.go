package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/azusayn/workflow/store"
	"github.com/azusayn/workflow/telemetry"
)

const (
	readyChannelCapacity         = 2000
	workflowEventChannelCapacity = 1000
	defaultNodeExecutorNum       = 4
	// TODO: support per-node timeout policy.
	defaultNodeExecutionTimeout                          = 3 * time.Second
	defaultConditionCheckTimout                          = 1 * time.Second
	defaultConditionCheckInterval                        = 1 * time.Second
	attributeKeyWorkflowRunID     telemetry.AttributeKey = "workflow.workflow_run_id"
)

type Engine struct {
	store                store.Store
	tracer               telemetry.Tracer
	propagator           telemetry.Propagator
	logger               *slog.Logger
	nodeExecutorNum      int
	nodeExecutionTimeout time.Duration
	readyCh              chan *readyNodeTask
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
type NodeExecution struct {
	ReadyInDegree int
	Err           error
	Status        NodeStatus
}

type WorkflowRunStatus struct {
	ID             string
	WorkflowID     string
	NodeExecutions map[string]NodeExecution
	CompletedNodes int
}

type readyNodeTask struct {
	workflowRunID string
	nodeID        string
	ctx           context.Context
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

func WithStore(store store.Store) EngineOption {
	return func(e *Engine) {
		e.store = store
	}
}

func WithTracing(tracer telemetry.Tracer, propagator telemetry.Propagator) EngineOption {
	return func(e *Engine) {
		e.tracer = tracer
		e.propagator = propagator
	}
}

func NewEngine(opts ...EngineOption) *Engine {
	e := &Engine{
		store:                store.NewNoopStore(),
		tracer:               telemetry.NewNoopTracer(),
		propagator:           telemetry.NewNoopPropagator(),
		nodeExecutionTimeout: defaultNodeExecutionTimeout,
		logger:               slog.Default(),
		nodeExecutorNum:      defaultNodeExecutorNum,
		readyCh:              make(chan *readyNodeTask, readyChannelCapacity),
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
	var workflow *Workflow

	fn := func(ctx context.Context, _ telemetry.SetSpanAttributeFunc) error {
		if v, err := e.createWorkflow(ctx, name, nodeConfigs); err != nil {
			return err
		} else {
			workflow = v
		}
		return nil
	}
	var err = e.tracer.WithSpan(ctx, "workflow.CreateWorkflow", fn)

	return workflow, err
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
		status.NodeExecutions[nodeID] = *convertToNodeExecution(exec)
	}
	return status, nil
}

func (e *Engine) RecoverWorkflowRuns(ctx context.Context) error {
	fn := func(ctx context.Context, _ telemetry.SetSpanAttributeFunc) error {
		return e.recoverWorkflowRuns(ctx)
	}
	return e.tracer.WithSpan(ctx, "workflow.RecoverWorkflowRuns", fn)
}

func (e *Engine) StartNodeExecutors(ctx context.Context) error {
	startExecutor := func(ctx context.Context) error {
		for {
			select {
			case task := <-e.readyCh:
				logger := e.logger.With(slog.Any("workflow_run_id", task.workflowRunID), slog.Any("node_id", task.nodeID))
				run, err := e.getWorkflowRun(task.workflowRunID)
				if err != nil {
					logger.ErrorContext(task.ctx, "failed to get workflow run", slog.Any("err", err))
					continue
				}

				var (
					nodeStatus NodeStatus = NodeStatusPending
					condition  Condition
				)

				execErr := run.execute(ctx, task.nodeID, &nodeStatus, &condition)
				if execErr != nil {
					logger.ErrorContext(task.ctx, "failed to execute node", slog.Any("err", err))
				}

				if err = run.reconcileNodeStatus(ctx, task.nodeID, &nodeStatus, condition, execErr); err != nil {
					logger.ErrorContext(ctx, "failed to reconcile node status", slog.Any("err", err))
					continue
				}

				if err := run.persistNodeState(ctx, task.nodeID, nodeStatus, execErr); err != nil {
					logger.ErrorContext(ctx, "failed to persist node state", slog.Any("err", err))
					continue
				}

				switch nodeStatus {
				case NodeStatusPause,
					NodeStatusFailed:
					continue

				case NodeStatusCompleted:

				default:
					logger.ErrorContext(task.ctx, "invalid node status", slog.Any("node_status", nodeStatus))
					continue
				}

				e.checkAndPurgeInactiveRun(run)

				if err := e.schedule(task.ctx, run.nodeConfigs, task.workflowRunID, task.nodeID); err != nil {
					logger.ErrorContext(task.ctx, "failed to schedule node", slog.Any("err", err))
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

func (e *Engine) createWorkflow(ctx context.Context, name string, nodeConfigs map[string]*NodeConfig) (*Workflow, error) {
	for nodeID, nodeConfig := range nodeConfigs {
		for _, nextNodeID := range nodeConfig.NextNodeIDs {
			nextNodeConfig, ok := nodeConfigs[nextNodeID]
			if !ok {
				return nil, fmt.Errorf("node ID does not exist %q", nodeID)
			}
			nextNodeConfig.InDegree++
		}
	}

	workflowModel, err := e.store.GetOrCreateWorkflow(ctx, name, convertToStoreNodeConfigs(nodeConfigs))
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

func (e *Engine) recoverWorkflowRuns(ctx context.Context) error {
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
		activeConditions[subscriberID] = convertToCondition(model)
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
		nodeConfigsMap[workflow.ID] = convertToNodeConfigs(workflow.NodeConfigs)
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
			convertToNodeExecutions(workflowRunModel.NodeExecutions),
			nodeConfigs,
		)
		e.mu.Unlock()

		readyNodeIDs, err := getReadyNodeIDs(nodeConfigs, convertToNodeExecutions(workflowRunModel.NodeExecutions))
		if err != nil {
			logger.ErrorContext(ctx, "failed to get ready nodes", slog.Any("err", err))
			continue
		}

		runCtx := e.propagator.Extract(ctx, workflowRunModel.TraceCarrier)
		for _, nodeID := range readyNodeIDs {
			e.readyCh <- &readyNodeTask{
				nodeID:        nodeID,
				workflowRunID: workflowRunModel.ID,
				ctx:           runCtx,
			}
		}
	}

	return nil
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
		e.readyCh <- &readyNodeTask{
			workflowRunID: workflowRunID,
			nodeID:        nextNodeID,
			ctx:           ctx,
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

	if err := e.store.UpdateWorkflowRun(ctx, convertToStoreUpdateWorkflowRunInput(
		run.id,
		&nodeID,
		exec,
		nil,
		false,
	)); err != nil {
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
	var workflowRunID string

	fn := func(ctx context.Context, setAttribute telemetry.SetSpanAttributeFunc) error {
		if v, err := w.run(ctx, data); err != nil {
			return err
		} else {
			workflowRunID = v
		}
		setAttribute(attributeKeyWorkflowRunID, workflowRunID)
		return nil
	}
	var err = w.engine.tracer.WithSpan(ctx, "workflow.Run", fn)

	return workflowRunID, err
}

func (w *Workflow) run(ctx context.Context, data map[string]any) (string, error) {
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

	w.engine.readyCh <- &readyNodeTask{
		workflowRunID: workflowRunID,
		nodeID:        RootNodeID,
		ctx:           ctx,
	}

	return workflowRunID, nil
}

func (w *Workflow) createWorkflowRun(ctx context.Context, pool *VariablePool, executions map[string]*NodeExecution, data json.RawMessage) (string, error) {
	w.engine.mu.Lock()
	defer w.engine.mu.Unlock()

	var workflowRunID string
	if err := w.engine.store.Transaction(ctx, func(ctx context.Context) error {
		carrier := make(map[string]string)
		w.engine.propagator.Inject(ctx, carrier)
		var err error
		workflowRunID, err = w.engine.store.CreateWorkflowRun(ctx, convertToStoreWorkflowRunModel(
			w.ID,
			data,
			executions,
			carrier,
		))
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

func (r *workflowRun) execute(ctx context.Context, nodeID string, nodeStatus *NodeStatus, condition *Condition) error {
	fn := func(ctx context.Context, setAttribute telemetry.SetSpanAttributeFunc) error {
		setAttribute(attributeKeyWorkflowRunID, r.id)
		ctx = withNodeID(ctx, nodeID)
		ctx = withWorkflowRun(ctx, r)
		ctx = withNodeStatus(ctx, nodeStatus)
		ctx = withCondition(ctx, condition)
		node, ok := Lookup(nodeID)
		if !ok {
			return fmt.Errorf("node type %q not registered", nodeID)
		}

		ctx, cancel := context.WithTimeout(ctx, r.engine.nodeExecutionTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- node.Execute(ctx)
		}()

		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	spanName := fmt.Sprintf("workflow.ExecuteNode: %s", nodeID)
	return r.engine.tracer.WithSpan(ctx, spanName, fn)
}

func (r *workflowRun) reconcileNodeStatus(ctx context.Context, nodeID string, nodeStatus *NodeStatus, condition Condition, execErr error) error {
	if nodeStatus == nil {
		return errors.New("nil node status")
	}
	if execErr != nil {
		*nodeStatus = NodeStatusFailed
	}

	switch *nodeStatus {
	case NodeStatusFailed:
		config, ok := r.nodeConfigs[nodeID]
		if !ok {
			return fmt.Errorf("failed to find config for %q", nodeID)
		}
		if config.ContinueOnError {
			slog.WarnContext(ctx, "node exection failed but allowed to continue", slog.Any("err", execErr), slog.Any("node_id", nodeID))
			*nodeStatus = NodeStatusCompleted
		}

	case NodeStatusPause:
		if err := r.engine.subscribeEvent(ctx, r.id, nodeID, condition); err != nil {
			return err
		}

	case NodeStatusPending:
		*nodeStatus = NodeStatusCompleted

	default:
		return fmt.Errorf("node %q entered invalid status %q", nodeID, *nodeStatus)
	}

	return nil
}

func (r *workflowRun) persistNodeState(ctx context.Context, nodeID string, nodeStatus NodeStatus, execErr error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	bytes, err := r.pool.Dump()
	if err != nil {
		return err
	}
	nodeExecution := NodeExecution{
		Status: nodeStatus,
		Err:    execErr,
	}

	return r.engine.store.Transaction(ctx, func(ctx context.Context) error {
		if err := r.engine.store.UpdateWorkflowRun(ctx, convertToStoreUpdateWorkflowRunInput(
			r.id,
			&nodeID,
			&nodeExecution,
			&bytes,
			nodeStatus == NodeStatusCompleted || nodeStatus == NodeStatusFailed,
		)); err != nil {
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
	})
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
		if err := r.engine.store.UpdateWorkflowRun(ctx, convertToStoreUpdateWorkflowRunInput(
			r.id,
			&nodeID,
			exec,
			nil,
			true,
		)); err != nil {
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
			config, ok := configs[nodeID]
			if !ok {
				return fmt.Errorf("failed to get config for %q", nodeID)
			}
			if execution.ReadyInDegree >= config.InDegree {
				readyNodeIDs = append(readyNodeIDs, nodeID)
			}
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
