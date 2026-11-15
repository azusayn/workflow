package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func executionOf(run *workflowRun, nodeID string) (NodeStatus, error, bool) {
	run.mu.Lock()
	defer run.mu.Unlock()
	exec, ok := run.executions[nodeID]
	if !ok || exec == nil {
		return "", nil, false
	}
	return exec.Status, exec.Err, true
}

func waitTerminal(t *testing.T, ctx context.Context, run *workflowRun, nodeID string) (NodeStatus, error) {
	t.Helper()
	deadline := time.After(12 * time.Second)
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for node %q: %v", nodeID, ctx.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for node %q", nodeID)
		default:
		}

		status, execErr, ok := executionOf(run, nodeID)
		if ok {
			switch status {
			case NodeStatusCompleted, NodeStatusFailed, NodeStatusSkipped:
				return status, execErr
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitUntilStatus(t *testing.T, ctx context.Context, run *workflowRun, nodeID string, want NodeStatus) {
	t.Helper()
	deadline := time.After(12 * time.Second)
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %q=%q: %v", nodeID, want, ctx.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for %q=%q", nodeID, want)
		default:
		}
		status, _, ok := executionOf(run, nodeID)
		if ok && status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startExecutors(t *testing.T, ctx context.Context, e *Engine) {
	t.Helper()
	go func() {
		if err := e.StartNodeExecutors(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("StartNodeExecutors: %v", err)
		}
	}()
	go func() {
		if err := e.StartEventProcessor(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("StartEventProcessor: %v", err)
		}
	}()
}

func startConditionChecker(t *testing.T, ctx context.Context, e *Engine) {
	t.Helper()
	go func() {
		if err := e.StartConditionChecker(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("StartConditionChecker: %v", err)
		}
	}()
}

func activeRun(t *testing.T, e *Engine, runID string) *workflowRun {
	t.Helper()
	run, err := e.getWorkflowRun(runID)
	if err != nil {
		t.Fatalf("getWorkflowRun: %v", err)
	}
	return run
}

func cloneNodeConfigs(src map[string]*NodeConfig) map[string]*NodeConfig {
	dst := make(map[string]*NodeConfig, len(src))
	for id, cfg := range src {
		cp := *cfg
		if cfg.NextNodeIDs != nil {
			cp.NextNodeIDs = append([]string(nil), cfg.NextNodeIDs...)
		}
		if cfg.Inputs != nil {
			cp.Inputs = make(NodeInputConfig, len(cfg.Inputs))
			for k, v := range cfg.Inputs {
				cp.Inputs[k] = v
			}
		}
		cp.InDegree = 0
		dst[id] = &cp
	}
	return dst
}

func createTestWorkflow(t *testing.T, ctx context.Context, name string, configs map[string]*NodeConfig, opts ...EngineOption) (*Engine, *Workflow) {
	t.Helper()
	engine := NewEngine(opts...)
	w, err := engine.CreateWorkflow(ctx, name, configs)
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	return engine, w
}

// README demo: root fans out to wait_approval + heavy_task, both join at summary.
type waitApprovalNode struct{}

func (waitApprovalNode) Execute(ctx context.Context) error {
	return WaitFor(ctx, Condition{EventID: "approval_event"})
}

type heavyTaskNode struct{}

func (heavyTaskNode) Execute(ctx context.Context) error {
	time.Sleep(100 * time.Millisecond)
	return Set(ctx, "heavy_task", "score", 95)
}

type summaryNode struct{}

func (summaryNode) Execute(ctx context.Context) error {
	score, err := Get[int](ctx, "score")
	if err != nil {
		return err
	}
	if score != 95 {
		return errors.New("unexpected score")
	}
	return nil
}

func TestEngineREADMEDemoPipeline(t *testing.T) {
	Register("wait_approval", waitApprovalNode{})
	Register("heavy_task", heavyTaskNode{})
	Register("summary", summaryNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {
			NextNodeIDs: []string{"wait_approval", "heavy_task"},
		},
		"wait_approval": {
			NextNodeIDs: []string{"summary"},
		},
		"heavy_task": {
			NextNodeIDs: []string{"summary"},
		},
		"summary": {
			Inputs: NodeInputConfig{
				"score": "heavy_task",
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	engine, w := createTestWorkflow(t, ctx, "demo-pipeline", configs, WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	if err := engine.RegisterCallback("approval_event", func(ctx context.Context, _ any) error {
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}

	runID, err := w.Run(ctx, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = engine.Post(ctx, &WorkflowEvent{
			ID:            "approval_event",
			WorkflowRunID: runID,
			NodeID:        "wait_approval",
		})
	}()

	if status, execErr := waitTerminal(t, ctx, run, "summary"); status != NodeStatusCompleted {
		t.Fatalf("summary status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx, run, "wait_approval"); status != NodeStatusCompleted {
		t.Fatalf("wait_approval status = %q, want completed", status)
	}
	if status, _ := waitTerminal(t, ctx, run, "heavy_task"); status != NodeStatusCompleted {
		t.Fatalf("heavy_task status = %q, want completed", status)
	}
}

const (
	faninFailNodeID  = "fanin_fail"
	faninOKNodeID    = "fanin_ok"
	faninSleepNodeID = "fanin_sleep"
	faninWaitNodeID  = "fanin_wait"
	faninSumNodeID   = "fanin_sum"
	faninWaitEvent   = "fanin_wait_event"
	faninExpectedSum = 7
)

type faninFailNode struct{}

func (faninFailNode) Execute(ctx context.Context) error {
	if err := Set(ctx, faninFailNodeID, "a", 1); err != nil {
		return err
	}
	return errors.New("fanin fail node")
}

type faninOKNode struct{}

func (faninOKNode) Execute(ctx context.Context) error {
	return Set(ctx, faninOKNodeID, "a", 1)
}

type faninSleepNode struct{}

func (faninSleepNode) Execute(ctx context.Context) error {
	time.Sleep(50 * time.Millisecond)
	return Set(ctx, faninSleepNodeID, "b", 2)
}

type faninWaitNode struct{}

func (faninWaitNode) Execute(ctx context.Context) error {
	if err := Set(ctx, faninWaitNodeID, "c", 4); err != nil {
		return err
	}
	return WaitFor(ctx, Condition{EventID: faninWaitEvent})
}

type faninSumNode struct{}

func (faninSumNode) Execute(ctx context.Context) error {
	a, err := Get[int](ctx, "a")
	if err != nil {
		return err
	}
	b, err := Get[int](ctx, "b")
	if err != nil {
		return err
	}
	c, err := Get[int](ctx, "c")
	if err != nil {
		return err
	}
	if a+b+c != faninExpectedSum {
		return errors.New("fanin sum mismatch")
	}
	return Set(ctx, faninSumNodeID, "sum", a+b+c)
}

func faninConfigs(firstNodeID string) map[string]*NodeConfig {
	return map[string]*NodeConfig{
		RootNodeID: {
			NextNodeIDs: []string{firstNodeID, faninSleepNodeID, faninWaitNodeID},
		},
		firstNodeID: {
			NextNodeIDs:     []string{faninSumNodeID},
			ContinueOnError: firstNodeID == faninFailNodeID,
		},
		faninSleepNodeID: {
			NextNodeIDs: []string{faninSumNodeID},
		},
		faninWaitNodeID: {
			NextNodeIDs: []string{faninSumNodeID},
		},
		faninSumNodeID: {
			Inputs: NodeInputConfig{
				"a": firstNodeID,
				"b": faninSleepNodeID,
				"c": faninWaitNodeID,
			},
		},
	}
}

func startFanin(t *testing.T, name, firstNodeID string, firstNode Node) (*Engine, context.Context, context.CancelFunc, *workflowRun, string) {
	t.Helper()

	Register(firstNodeID, firstNode)
	Register(faninSleepNodeID, faninSleepNode{})
	Register(faninWaitNodeID, faninWaitNode{})
	Register(faninSumNodeID, faninSumNode{})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	engine, w := createTestWorkflow(t, ctx, name, faninConfigs(firstNodeID), WithNodeExecutorNum(3))
	startExecutors(t, ctx, engine)

	if err := engine.RegisterCallback(faninWaitEvent, func(ctx context.Context, _ any) error {
		return Resume(ctx)
	}); err != nil {
		cancel()
		t.Fatalf("RegisterCallback: %v", err)
	}

	runID, err := w.Run(ctx, nil)
	if err != nil {
		cancel()
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = engine.Post(ctx, &WorkflowEvent{
			ID:            faninWaitEvent,
			WorkflowRunID: runID,
			NodeID:        faninWaitNodeID,
		})
	}()

	return engine, ctx, cancel, run, runID
}

func TestEngineFanInContinueOnError(t *testing.T) {
	_, ctx, cancel, run, _ := startFanin(t, "fanin-continue-on-error", faninFailNodeID, faninFailNode{})
	defer cancel()

	sumStatus, sumErr := waitTerminal(t, ctx, run, faninSumNodeID)
	if sumStatus != NodeStatusCompleted {
		t.Fatalf("sum status = %q err=%v, want completed", sumStatus, sumErr)
	}

	failStatus, failErr := waitTerminal(t, ctx, run, faninFailNodeID)
	if failStatus != NodeStatusCompleted {
		t.Fatalf("fail status = %q, want completed under ContinueOnError", failStatus)
	}
	if failErr == nil {
		t.Fatal("fail Err = nil, want preserved error under ContinueOnError")
	}
	if status, _ := waitTerminal(t, ctx, run, faninSleepNodeID); status != NodeStatusCompleted {
		t.Fatalf("sleep status = %q, want completed", status)
	}
	if status, _ := waitTerminal(t, ctx, run, faninWaitNodeID); status != NodeStatusCompleted {
		t.Fatalf("wait status = %q, want completed", status)
	}
}

func TestEngineFanInSumOnAllSuccess(t *testing.T) {
	_, ctx, cancel, run, _ := startFanin(t, "fanin-sum", faninOKNodeID, faninOKNode{})
	defer cancel()

	sumStatus, sumErr := waitTerminal(t, ctx, run, faninSumNodeID)
	if sumStatus != NodeStatusCompleted {
		t.Fatalf("sum status = %q err=%v, want completed", sumStatus, sumErr)
	}
	if status, _ := waitTerminal(t, ctx, run, faninOKNodeID); status != NodeStatusCompleted {
		t.Fatalf("ok status = %q, want completed", status)
	}
	if status, _ := waitTerminal(t, ctx, run, faninSleepNodeID); status != NodeStatusCompleted {
		t.Fatalf("sleep status = %q, want completed", status)
	}
	if status, _ := waitTerminal(t, ctx, run, faninWaitNodeID); status != NodeStatusCompleted {
		t.Fatalf("wait status = %q, want completed", status)
	}
}

const (
	chainANodeID = "chain_a"
	chainBNodeID = "chain_b"
	chainCNodeID = "chain_c"
)

type chainANode struct{}

func (chainANode) Execute(ctx context.Context) error {
	return Set(ctx, chainANodeID, "n", 1)
}

type chainBNode struct{}

func (chainBNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	return Set(ctx, chainBNodeID, "n", n+1)
}

type chainCNode struct{}

func (chainCNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	if n != 2 {
		return errors.New("chain value mismatch")
	}
	return Set(ctx, chainCNodeID, "done", true)
}

func TestEngineLinearChain(t *testing.T) {
	Register(chainANodeID, chainANode{})
	Register(chainBNodeID, chainBNode{})
	Register(chainCNodeID, chainCNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {
			NextNodeIDs: []string{chainANodeID},
		},
		chainANodeID: {
			NextNodeIDs: []string{chainBNodeID},
		},
		chainBNodeID: {
			NextNodeIDs: []string{chainCNodeID},
			Inputs: NodeInputConfig{
				"n": chainANodeID,
			},
		},
		chainCNodeID: {
			Inputs: NodeInputConfig{
				"n": chainBNodeID,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, w := createTestWorkflow(t, ctx, "linear-chain", configs, WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	runID, err := w.Run(ctx, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)

	status, execErr := waitTerminal(t, ctx, run, chainCNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("chain_c status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx, run, chainANodeID); status != NodeStatusCompleted {
		t.Fatalf("chain_a status = %q, want completed", status)
	}
	if status, _ := waitTerminal(t, ctx, run, chainBNodeID); status != NodeStatusCompleted {
		t.Fatalf("chain_b status = %q, want completed", status)
	}
}

const (
	callbackSetNodeID = "callback_set"
	callbackEvt       = "callback_set_event"
)

type callbackSetNode struct{}

func (callbackSetNode) Execute(ctx context.Context) error {
	return WaitFor(ctx, Condition{EventID: callbackEvt})
}

type callbackReadNode struct{}

func (callbackReadNode) Execute(ctx context.Context) error {
	ok, err := Get[bool](ctx, "approved")
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("approved is false")
	}
	return nil
}

func TestEngineCallbackSetsPoolThenResume(t *testing.T) {
	Register(callbackSetNodeID, callbackSetNode{})
	Register("callback_read", callbackReadNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {
			NextNodeIDs: []string{callbackSetNodeID},
		},
		callbackSetNodeID: {
			NextNodeIDs: []string{"callback_read"},
		},
		"callback_read": {
			Inputs: NodeInputConfig{
				"approved": callbackSetNodeID,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, w := createTestWorkflow(t, ctx, "callback-set", configs, WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	if err := engine.RegisterCallback(callbackEvt, func(ctx context.Context, _ any) error {
		if err := Set(ctx, callbackSetNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}

	runID, err := w.Run(ctx, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = engine.Post(ctx, &WorkflowEvent{
			ID:            callbackEvt,
			WorkflowRunID: runID,
			NodeID:        callbackSetNodeID,
		})
	}()

	status, execErr := waitTerminal(t, ctx, run, "callback_read")
	if status != NodeStatusCompleted {
		t.Fatalf("callback_read status = %q err=%v, want completed", status, execErr)
	}
}

func TestEnginePauseResumeLinear(t *testing.T) {
	const (
		waitNodeID = "mem_pause_wait"
		nextNodeID = "mem_pause_next"
		eventID    = "mem_pause_event"
	)

	Register(waitNodeID, memPauseWaitNode{eventID: eventID})
	Register(nextNodeID, memPauseNextNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{waitNodeID}},
		waitNodeID: {NextNodeIDs: []string{nextNodeID}},
		nextNodeID: {
			Inputs: NodeInputConfig{"approved": waitNodeID},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, w := createTestWorkflow(t, ctx, "mem-pause-resume", cloneNodeConfigs(configs), WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	if err := engine.RegisterCallback(eventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, waitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}

	runID, err := w.Run(ctx, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)
	waitUntilStatus(t, ctx, run, waitNodeID, NodeStatusPause)

	if err := engine.Post(ctx, &WorkflowEvent{
		ID:            eventID,
		WorkflowRunID: runID,
		NodeID:        waitNodeID,
	}); err != nil {
		t.Fatalf("Post: %v", err)
	}

	status, execErr := waitTerminal(t, ctx, run, nextNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("next status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx, run, waitNodeID); status != NodeStatusCompleted {
		t.Fatalf("wait status = %q, want completed", status)
	}
}

func TestEngineSeededLinearFromRoot(t *testing.T) {
	const (
		workNodeID = "mem_seed_work"
		doneNodeID = "mem_seed_done"
	)
	Register(workNodeID, memSeedWorkNode{})
	Register(doneNodeID, memSeedDoneNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{workNodeID}},
		workNodeID: {
			NextNodeIDs: []string{doneNodeID},
			Inputs:      NodeInputConfig{"n": RootNodeID},
		},
		doneNodeID: {
			Inputs: NodeInputConfig{"n": workNodeID},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, w := createTestWorkflow(t, ctx, "mem-seeded-linear", cloneNodeConfigs(configs), WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	runID, err := w.Run(ctx, map[string]any{"n": 3})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)

	status, execErr := waitTerminal(t, ctx, run, doneNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("done status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx, run, workNodeID); status != NodeStatusCompleted {
		t.Fatalf("work status = %q, want completed", status)
	}
}

type memPauseWaitNode struct{ eventID string }

func (n memPauseWaitNode) Execute(ctx context.Context) error {
	return WaitFor(ctx, Condition{EventID: n.eventID})
}

type memPauseNextNode struct{}

func (memPauseNextNode) Execute(ctx context.Context) error {
	ok, err := Get[bool](ctx, "approved")
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("approved is false")
	}
	return nil
}

type memSeedWorkNode struct{}

func (memSeedWorkNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	return Set(ctx, "mem_seed_work", "n", n+1)
}

type memSeedDoneNode struct{}

func (memSeedDoneNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("done value mismatch")
	}
	return nil
}

const (
	failHardNodeID = "fail_hard"
	failSkipMidID  = "fail_skip_mid"
	failSkipLeafID = "fail_skip_leaf"
)

type failHardNode struct{}

func (failHardNode) Execute(context.Context) error {
	return errors.New("hard fail")
}

type failSkipProbeNode struct {
	id string
}

func (n failSkipProbeNode) Execute(context.Context) error {
	return errors.New("successor should have been skipped: " + n.id)
}

func startFailSkipWorkflow(t *testing.T) (context.Context, context.CancelFunc, *Engine, *workflowRun) {
	t.Helper()

	Register(failHardNodeID, failHardNode{})
	Register(failSkipMidID, failSkipProbeNode{id: failSkipMidID})
	Register(failSkipLeafID, failSkipProbeNode{id: failSkipLeafID})

	configs := map[string]*NodeConfig{
		RootNodeID: {
			NextNodeIDs: []string{failHardNodeID},
		},
		failHardNodeID: {
			NextNodeIDs: []string{failSkipMidID},
		},
		failSkipMidID: {
			NextNodeIDs: []string{failSkipLeafID},
		},
		failSkipLeafID: {},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	engine, w := createTestWorkflow(t, ctx, "fail-skip", cloneNodeConfigs(configs), WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)

	runID, err := w.Run(ctx, nil)
	if err != nil {
		cancel()
		t.Fatalf("Run: %v", err)
	}
	return ctx, cancel, engine, activeRun(t, engine, runID)
}

func TestEngineFailKeepsFailedWithoutContinueOnError(t *testing.T) {
	ctx, cancel, _, run := startFailSkipWorkflow(t)
	defer cancel()

	waitUntilStatus(t, ctx, run, failHardNodeID, NodeStatusFailed)
	status, execErr, ok := executionOf(run, failHardNodeID)
	if !ok {
		t.Fatal("fail node execution missing")
	}
	if status != NodeStatusFailed {
		t.Fatalf("fail status = %q, want failed", status)
	}
	if execErr == nil {
		t.Fatal("fail Err = nil, want hard fail error")
	}
}

func TestEngineFailSkipsSuccessors(t *testing.T) {
	ctx, cancel, _, run := startFailSkipWorkflow(t)
	defer cancel()

	waitUntilStatus(t, ctx, run, failHardNodeID, NodeStatusFailed)
	waitUntilStatus(t, ctx, run, failSkipMidID, NodeStatusSkipped)
	waitUntilStatus(t, ctx, run, failSkipLeafID, NodeStatusSkipped)

	if status, execErr, _ := executionOf(run, failSkipMidID); status != NodeStatusSkipped || execErr != nil {
		t.Fatalf("mid status=%q err=%v, want skipped with nil err", status, execErr)
	}
	if status, execErr, _ := executionOf(run, failSkipLeafID); status != NodeStatusSkipped || execErr != nil {
		t.Fatalf("leaf status=%q err=%v, want skipped with nil err", status, execErr)
	}
}

const (
	condWaitNodeID = "cond_wait"
	condNextNodeID = "cond_next"
	condEventID    = "cond_event"
	condCheckerID  = "cond_ready"
)

type condWaitNode struct{}

func (condWaitNode) Execute(ctx context.Context) error {
	id := condCheckerID
	return WaitFor(ctx, Condition{
		ID:      &id,
		EventID: condEventID,
		Payload: map[string]any{"token": "cond-payload"},
	})
}

type condNextNode struct{}

func (condNextNode) Execute(ctx context.Context) error {
	ok, err := Get[bool](ctx, "approved")
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("approved is false")
	}
	return nil
}

func TestEngineConditionCheckerResumesPausedNode(t *testing.T) {
	Register(condWaitNodeID, condWaitNode{})
	Register(condNextNodeID, condNextNode{})

	configs := map[string]*NodeConfig{
		RootNodeID:     {NextNodeIDs: []string{condWaitNodeID}},
		condWaitNodeID: {NextNodeIDs: []string{condNextNodeID}},
		condNextNodeID: {
			Inputs: NodeInputConfig{"approved": condWaitNodeID},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	engine, w := createTestWorkflow(t, ctx, "cond-check", cloneNodeConfigs(configs), WithNodeExecutorNum(2))
	startExecutors(t, ctx, engine)
	startConditionChecker(t, ctx, engine)

	var ready atomic.Bool
	if err := engine.RegisterCondition(condCheckerID, func(_ context.Context, payload any) (bool, error) {
		got, ok := payload.(map[string]any)
		if !ok {
			return false, fmt.Errorf("payload type %T, want map[string]any", payload)
		}
		if got["token"] != "cond-payload" {
			return false, fmt.Errorf("payload = %#v, want token=cond-payload", got)
		}
		return ready.Load(), nil
	}); err != nil {
		t.Fatalf("RegisterCondition: %v", err)
	}
	if err := engine.RegisterCallback(condEventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, condWaitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback: %v", err)
	}

	runID, err := w.Run(ctx, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine, runID)
	waitUntilStatus(t, ctx, run, condWaitNodeID, NodeStatusPause)

	ready.Store(true)

	status, execErr := waitTerminal(t, ctx, run, condNextNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("next status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx, run, condWaitNodeID); status != NodeStatusCompleted {
		t.Fatalf("wait status = %q, want completed", status)
	}
}
