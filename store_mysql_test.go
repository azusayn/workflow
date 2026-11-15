package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func mysqlTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("WORKFLOW_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WORKFLOW_MYSQL_DSN not set; skip MySQL store tests")
	}
	return dsn
}

func newMySQLStore(t *testing.T) (*MySQLStore, context.Context) {
	t.Helper()

	db, err := sql.Open("mysql", mysqlTestDSN(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Skipf("MySQL unavailable (%v); check WORKFLOW_MYSQL_DSN", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store := NewMySQLStore(db)
	resetCtx := context.Background()
	for _, table := range []string{"node_executions", "workflow_runs", "subscriber_events", "workflow_conditions", "workflows"} {
		if _, err := db.ExecContext(resetCtx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
	}
	for _, stmt := range store.Schema() {
		if _, err := db.ExecContext(resetCtx, stmt); err != nil {
			t.Fatalf("create schema: %v\nstmt: %s", err, stmt)
		}
	}
	return store, context.Background()
}

func pendingIDs(runs []*workflowRunModel) []string {
	ids := make([]string, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.ID)
	}
	return ids
}

func pendingRun(t *testing.T, runs []*workflowRunModel, runID string) *workflowRunModel {
	t.Helper()
	for _, run := range runs {
		if run.ID == runID {
			return run
		}
	}
	t.Fatalf("run %q not in pending: %v", runID, pendingIDs(runs))
	return nil
}

func sampleConfigs(n int) map[string]*NodeConfig {
	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{}},
	}
	for i := 1; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		configs[id] = &NodeConfig{}
		configs[RootNodeID].NextNodeIDs = append(configs[RootNodeID].NextNodeIDs, id)
	}
	return configs
}

func TestMySQLStoreFullAPI(t *testing.T) {
	store, ctx := newMySQLStore(t)

	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{"n1", "n2"}},
		"n1":       {},
		"n2":       {},
	}
	wf1, err := store.GetOrCreateWorkflow(ctx, "full-api", configs)
	if err != nil {
		t.Fatalf("GetOrCreateWorkflow create: %v", err)
	}
	if wf1.ID == "" || wf1.TotalNodes != 3 {
		t.Fatalf("unexpected workflow: %+v", wf1)
	}

	overwritten := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{"n1"}},
		"n1":       {Inputs: NodeInputConfig{"x": RootNodeID}},
	}
	wf2, err := store.GetOrCreateWorkflow(ctx, "full-api", overwritten)
	if err != nil {
		t.Fatalf("GetOrCreateWorkflow overwrite: %v", err)
	}
	if wf2.ID != wf1.ID {
		t.Fatalf("workflow id changed: %q -> %q", wf1.ID, wf2.ID)
	}
	if wf2.TotalNodes != 2 {
		t.Fatalf("TotalNodes after overwrite = %d, want 2", wf2.TotalNodes)
	}
	listed, err := store.ListWorkflows(ctx, []string{wf1.ID})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(listed) != 1 || listed[0].TotalNodes != 2 || listed[0].NodeConfigs["n1"] == nil {
		t.Fatalf("listed after overwrite = %+v", listed)
	}

	// Restore three-node graph for pending semantics in this test.
	if _, err := store.GetOrCreateWorkflow(ctx, "full-api", configs); err != nil {
		t.Fatalf("restore configs: %v", err)
	}

	data := json.RawMessage(`[{"node_id":"root","key":"seed","value":"v1"}]`)
	runID, err := store.CreateWorkflowRun(ctx, &workflowRunModel{
		WorkflowID: wf1.ID,
		Data:       data,
		NodeExecutions: map[string]*NodeExecution{
			RootNodeID: {Status: NodeStatusPending},
			"n1":       {Status: NodeStatusPending},
			"n2":       {Status: NodeStatusPending},
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowRun: %v", err)
	}

	pending, err := store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		t.Fatalf("ListPendingWorkflowRuns after create: %v", err)
	}
	got := pendingRun(t, pending, runID)
	if string(got.Data) != string(data) {
		t.Fatalf("data = %s, want %s", got.Data, data)
	}
	if got.CompletedNodes != 0 || len(got.NodeExecutions) != 3 {
		t.Fatalf("run = completed=%d execs=%d", got.CompletedNodes, len(got.NodeExecutions))
	}

	subscriberID := runID + ":n1"
	if err := store.UpsertSubscriberMap(ctx, subscriberID, []string{"evt-a", "evt-b"}); err != nil {
		t.Fatalf("UpsertSubscriberMap: %v", err)
	}
	subs, err := store.LoadSubscriberMap(ctx)
	if err != nil {
		t.Fatalf("LoadSubscriberMap: %v", err)
	}
	if _, ok := subs[subscriberID]["evt-a"]; !ok {
		t.Fatalf("subscriber map missing evt-a: %+v", subs)
	}
	if err := store.UpsertSubscriberMap(ctx, subscriberID, nil); err != nil {
		t.Fatalf("UpsertSubscriberMap empty: %v", err)
	}
	subs, err = store.LoadSubscriberMap(ctx)
	if err != nil {
		t.Fatalf("LoadSubscriberMap after empty: %v", err)
	}
	if len(subs[subscriberID]) != 0 {
		t.Fatalf("subscriber events after empty upsert = %+v", subs[subscriberID])
	}

	payload := json.RawMessage(`{"work_order_ids":["1","2"]}`)
	if err := store.UpsertCondition(ctx, subscriberID, "ready_check", "evt-a", payload); err != nil {
		t.Fatalf("UpsertCondition: %v", err)
	}
	conds, err := store.LoadConditions(ctx)
	if err != nil {
		t.Fatalf("LoadConditions: %v", err)
	}
	if got := conds[subscriberID]; got.ID != "ready_check" || got.EventID != "evt-a" || string(got.Payload) != string(payload) {
		t.Fatalf("condition = %+v, want ready_check/evt-a/%s", got, payload)
	}

	loaded, err := store.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("store.GetWorkflowRun: %v", err)
	}
	if loaded.ID != runID || loaded.CompletedNodes != 0 {
		t.Fatalf("store.GetWorkflowRun = %+v, want id=%s completed=0", loaded, runID)
	}
	if _, err := store.GetWorkflowRun(ctx, "missing-run"); err == nil {
		t.Fatal("store.GetWorkflowRun missing: want error")
	}

	engine := NewEngine(WithStore(store))
	status, err := engine.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("Engine.GetWorkflowRun: %v", err)
	}
	if status.ID != runID || status.WorkflowID != wf1.ID || status.CompletedNodes != 0 {
		t.Fatalf("Engine.GetWorkflowRun = %+v", status)
	}
	if len(status.NodeExecutions) != 3 {
		t.Fatalf("NodeExecutions len = %d, want 3", len(status.NodeExecutions))
	}
	if _, ok := status.NodeExecutions[RootNodeID]; !ok {
		t.Fatalf("NodeExecutions missing root: %+v", status.NodeExecutions)
	}
	// Mutating the returned map must not affect the store model pointers.
	rootExec := status.NodeExecutions[RootNodeID]
	rootExec.Status = NodeStatusCompleted
	status.NodeExecutions[RootNodeID] = rootExec
	reload, err := store.GetWorkflowRun(ctx, runID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if reload.NodeExecutions[RootNodeID].Status != NodeStatusPending {
		t.Fatalf("store mutated via status copy: %q", reload.NodeExecutions[RootNodeID].Status)
	}
	if err := store.DeleteCondition(ctx, subscriberID); err != nil {
		t.Fatalf("DeleteCondition: %v", err)
	}
	conds, err = store.LoadConditions(ctx)
	if err != nil {
		t.Fatalf("LoadConditions after delete: %v", err)
	}
	if _, ok := conds[subscriberID]; ok {
		t.Fatalf("condition still present after delete: %+v", conds[subscriberID])
	}

	n1 := "n1"
	newData := json.RawMessage(`[{"node_id":"n1","key":"x","value":9}]`)
	if err := store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
		WorkflowRunID: runID,
		NodeID:        &n1,
		Execution: &NodeExecution{
			ReadyInDegree: 1,
			Status:        NodeStatusPause,
		},
		Data: &newData,
	}); err != nil {
		t.Fatalf("UpdateWorkflowRun pause: %v", err)
	}
	pending, err = store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		t.Fatalf("ListPendingWorkflowRuns after pause: %v", err)
	}
	got = pendingRun(t, pending, runID)
	if got.CompletedNodes != 0 {
		t.Fatalf("CompletedNodes after pause = %d, want 0", got.CompletedNodes)
	}
	if got.NodeExecutions["n1"].Status != NodeStatusPause || got.NodeExecutions["n1"].ReadyInDegree != 1 {
		t.Fatalf("n1 = %+v", got.NodeExecutions["n1"])
	}
	if string(got.Data) != string(newData) {
		t.Fatalf("data after pause = %s", got.Data)
	}

	if err := store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
		WorkflowRunID: runID,
		NodeID:        &n1,
		Execution: &NodeExecution{
			ReadyInDegree: 1,
			Status:        NodeStatusCompleted,
			Err:           errors.New("kept"),
		},
		IncrementCompleted: true,
	}); err != nil {
		t.Fatalf("UpdateWorkflowRun complete: %v", err)
	}
	pending, err = store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		t.Fatalf("ListPendingWorkflowRuns after complete: %v", err)
	}
	got = pendingRun(t, pending, runID)
	if got.CompletedNodes != 1 {
		t.Fatalf("CompletedNodes = %d, want 1", got.CompletedNodes)
	}
	if got.NodeExecutions["n1"].Err == nil || got.NodeExecutions["n1"].Err.Error() != "kept" {
		t.Fatalf("n1 err = %v", got.NodeExecutions["n1"].Err)
	}

	for _, nodeID := range []string{RootNodeID, "n2"} {
		id := nodeID
		if err := store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
			WorkflowRunID:      runID,
			NodeID:             &id,
			Execution:          &NodeExecution{Status: NodeStatusCompleted},
			IncrementCompleted: true,
		}); err != nil {
			t.Fatalf("complete %s: %v", nodeID, err)
		}
	}
	pending, err = store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		t.Fatalf("ListPending after done: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after done = %v, want empty", pendingIDs(pending))
	}

	boom := errors.New("rollback")
	err = store.Transaction(ctx, func(ctx context.Context) error {
		other, err := store.CreateWorkflowRun(ctx, &workflowRunModel{
			WorkflowID: wf1.ID,
			NodeExecutions: map[string]*NodeExecution{
				RootNodeID: {Status: NodeStatusPending},
			},
		})
		if err != nil {
			return err
		}
		root := RootNodeID
		if err := store.UpdateWorkflowRun(ctx, UpdateWorkflowRunInput{
			WorkflowRunID:      other,
			NodeID:             &root,
			Execution:          &NodeExecution{Status: NodeStatusCompleted},
			IncrementCompleted: true,
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transaction error = %v, want boom", err)
	}
	pending, err = store.ListPendingWorkflowRuns(ctx)
	if err != nil {
		t.Fatalf("ListPending after rollback: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("rolled-back run leaked into pending: %v", pendingIDs(pending))
	}

	nestedOK := false
	if err := store.Transaction(ctx, func(ctx context.Context) error {
		return store.Transaction(ctx, func(ctx context.Context) error {
			nestedOK = true
			_, err := store.GetOrCreateWorkflow(ctx, "full-api-nested", sampleConfigs(1))
			return err
		})
	}); err != nil {
		t.Fatalf("nested Transaction: %v", err)
	}
	if !nestedOK {
		t.Fatal("nested transaction body not executed")
	}
}

func TestMySQLStoreRecoverPausedWorkflow(t *testing.T) {
	store, _ := newMySQLStore(t)

	const (
		waitNodeID = "mysql_recover_wait"
		nextNodeID = "mysql_recover_next"
		eventID    = "mysql_recover_event"
	)

	Register(waitNodeID, recoverWaitNode{eventID: eventID})
	Register(nextNodeID, recoverNextNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{waitNodeID}},
		waitNodeID: {NextNodeIDs: []string{nextNodeID}},
		nextNodeID: {
			Inputs: NodeInputConfig{"approved": waitNodeID},
		},
	}

	name := fmt.Sprintf("mysql-recover-pause-%d", time.Now().UnixNano())

	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()

	engine1, w1 := createTestWorkflow(t, ctx1, name, cloneNodeConfigs(configs), WithStore(store), WithNodeExecutorNum(2))
	startExecutors(t, ctx1, engine1)
	if err := engine1.RegisterCallback(eventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, waitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback phase1: %v", err)
	}

	runID, err := w1.Run(ctx1, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine1, runID)
	waitUntilStatus(t, ctx1, run, waitNodeID, NodeStatusPause)
	cancel1()
	time.Sleep(50 * time.Millisecond)

	pending, err := store.ListPendingWorkflowRuns(context.Background())
	if err != nil {
		t.Fatalf("ListPending paused: %v", err)
	}
	persisted := pendingRun(t, pending, runID)
	if persisted.NodeExecutions[waitNodeID].Status != NodeStatusPause {
		t.Fatalf("persisted wait status = %q, want pause", persisted.NodeExecutions[waitNodeID].Status)
	}
	if persisted.CompletedNodes != 1 { // root completed
		t.Fatalf("CompletedNodes while paused = %d, want 1", persisted.CompletedNodes)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()

	engine2, w2 := createTestWorkflow(t, ctx2, name, cloneNodeConfigs(configs), WithStore(store), WithNodeExecutorNum(2))
	if w2.ID != w1.ID {
		t.Fatalf("workflow id mismatch after recover create: %q vs %q", w2.ID, w1.ID)
	}
	startExecutors(t, ctx2, engine2)
	if err := engine2.RegisterCallback(eventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, waitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback phase2: %v", err)
	}
	if err := engine2.RecoverWorkflowRuns(ctx2); err != nil {
		t.Fatalf("RecoverWorkflowRuns: %v", err)
	}

	recovered := activeRun(t, engine2, runID)
	if status, _, ok := executionOf(recovered, waitNodeID); !ok || status != NodeStatusPause {
		t.Fatalf("recovered wait status = %q ok=%v, want pause", status, ok)
	}

	if err := engine2.Post(ctx2, &WorkflowEvent{
		ID:            eventID,
		WorkflowRunID: runID,
		NodeID:        waitNodeID,
	}); err != nil {
		t.Fatalf("Post: %v", err)
	}

	status, execErr := waitTerminal(t, ctx2, recovered, nextNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("next status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, ctx2, recovered, waitNodeID); status != NodeStatusCompleted {
		t.Fatalf("wait status after resume = %q, want completed", status)
	}

	pending, err = store.ListPendingWorkflowRuns(ctx2)
	if err != nil {
		t.Fatalf("ListPending final: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after recover complete = %v", pendingIDs(pending))
	}
}

func TestMySQLStoreRecoverPendingReadyNodes(t *testing.T) {
	store, ctx := newMySQLStore(t)

	const (
		workNodeID = "mysql_recover_work"
		doneNodeID = "mysql_recover_done"
	)
	Register(workNodeID, recoverWorkNode{})
	Register(doneNodeID, recoverDoneNode{})

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
	name := fmt.Sprintf("mysql-recover-pending-%d", time.Now().UnixNano())

	// Persist with InDegree already applied the same way CreateWorkflow does.
	persistedConfigs := cloneNodeConfigs(configs)
	for _, nodeConfig := range persistedConfigs {
		for _, nextNodeID := range nodeConfig.NextNodeIDs {
			persistedConfigs[nextNodeID].InDegree++
		}
	}
	wf, err := store.GetOrCreateWorkflow(ctx, name, persistedConfigs)
	if err != nil {
		t.Fatalf("GetOrCreateWorkflow: %v", err)
	}

	data, err := json.Marshal([]VariableRecord{{
		NodeID: RootNodeID,
		Key:    "n",
		Value:  3,
	}})
	if err != nil {
		t.Fatalf("marshal pool: %v", err)
	}
	runID, err := store.CreateWorkflowRun(ctx, &workflowRunModel{
		WorkflowID:     wf.ID,
		Data:           data,
		CompletedNodes: 1,
		NodeExecutions: map[string]*NodeExecution{
			RootNodeID: {Status: NodeStatusCompleted},
			workNodeID: {Status: NodeStatusPending, ReadyInDegree: 1},
			doneNodeID: {Status: NodeStatusPending},
		},
	})
	if err != nil {
		t.Fatalf("CreateWorkflowRun: %v", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	engine, _ := createTestWorkflow(t, runCtx, name, cloneNodeConfigs(configs), WithStore(store), WithNodeExecutorNum(2))
	startExecutors(t, runCtx, engine)
	if err := engine.RecoverWorkflowRuns(runCtx); err != nil {
		t.Fatalf("RecoverWorkflowRuns: %v", err)
	}

	run := activeRun(t, engine, runID)
	status, execErr := waitTerminal(t, runCtx, run, doneNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("done status = %q err=%v, want completed", status, execErr)
	}
	if status, _ := waitTerminal(t, runCtx, run, workNodeID); status != NodeStatusCompleted {
		t.Fatalf("work status = %q, want completed", status)
	}

	pending, err := store.ListPendingWorkflowRuns(runCtx)
	if err != nil {
		t.Fatalf("ListPending final: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after recover complete = %v", pendingIDs(pending))
	}
}

func TestMySQLStoreRecoverActiveCondition(t *testing.T) {
	store, _ := newMySQLStore(t)

	const (
		waitNodeID  = "mysql_cond_wait"
		nextNodeID  = "mysql_cond_next"
		eventID     = "mysql_cond_event"
		conditionID = "mysql_cond_ready"
	)

	condPayload := map[string]any{"token": "recover-payload"}
	wantPayload := json.RawMessage(`{"token":"recover-payload"}`)
	Register(waitNodeID, recoverCondWaitNode{
		eventID:     eventID,
		conditionID: conditionID,
		payload:     condPayload,
	})
	Register(nextNodeID, recoverNextNode{})

	configs := map[string]*NodeConfig{
		RootNodeID: {NextNodeIDs: []string{waitNodeID}},
		waitNodeID: {NextNodeIDs: []string{nextNodeID}},
		nextNodeID: {
			Inputs: NodeInputConfig{"approved": waitNodeID},
		},
	}
	name := fmt.Sprintf("mysql-recover-cond-%d", time.Now().UnixNano())

	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()

	engine1, w1 := createTestWorkflow(t, ctx1, name, cloneNodeConfigs(configs), WithStore(store), WithNodeExecutorNum(2))
	startExecutors(t, ctx1, engine1)
	if err := engine1.RegisterCondition(conditionID, func(context.Context, any) (bool, error) {
		return false, nil
	}); err != nil {
		t.Fatalf("RegisterCondition phase1: %v", err)
	}
	if err := engine1.RegisterCallback(eventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, waitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback phase1: %v", err)
	}

	runID, err := w1.Run(ctx1, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	run := activeRun(t, engine1, runID)
	waitUntilStatus(t, ctx1, run, waitNodeID, NodeStatusPause)
	cancel1()
	time.Sleep(50 * time.Millisecond)

	subscriberID := buildSubscriberID(runID, waitNodeID)
	conds, err := store.LoadConditions(context.Background())
	if err != nil {
		t.Fatalf("LoadConditions: %v", err)
	}
	if got := conds[subscriberID]; got.ID != conditionID || got.EventID != eventID || string(got.Payload) != string(wantPayload) {
		t.Fatalf("persisted condition = %+v, want %s/%s/%s", got, conditionID, eventID, wantPayload)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()

	engine2, _ := createTestWorkflow(t, ctx2, name, cloneNodeConfigs(configs), WithStore(store), WithNodeExecutorNum(2))
	startExecutors(t, ctx2, engine2)
	startConditionChecker(t, ctx2, engine2)

	var ready atomic.Bool
	if err := engine2.RegisterCondition(conditionID, func(_ context.Context, payload any) (bool, error) {
		raw, ok := payload.(json.RawMessage)
		if !ok {
			return false, fmt.Errorf("payload type %T, want json.RawMessage", payload)
		}
		if string(raw) != string(wantPayload) {
			return false, fmt.Errorf("payload = %s, want %s", raw, wantPayload)
		}
		return ready.Load(), nil
	}); err != nil {
		t.Fatalf("RegisterCondition phase2: %v", err)
	}
	if err := engine2.RegisterCallback(eventID, func(ctx context.Context, _ any) error {
		if err := Set(ctx, waitNodeID, "approved", true); err != nil {
			return err
		}
		return Resume(ctx)
	}); err != nil {
		t.Fatalf("RegisterCallback phase2: %v", err)
	}
	if err := engine2.RecoverWorkflowRuns(ctx2); err != nil {
		t.Fatalf("RecoverWorkflowRuns: %v", err)
	}

	recovered := activeRun(t, engine2, runID)
	if status, _, ok := executionOf(recovered, waitNodeID); !ok || status != NodeStatusPause {
		t.Fatalf("recovered wait status = %q ok=%v, want pause", status, ok)
	}

	ready.Store(true)

	status, execErr := waitTerminal(t, ctx2, recovered, nextNodeID)
	if status != NodeStatusCompleted {
		t.Fatalf("next status = %q err=%v, want completed", status, execErr)
	}

	conds, err = store.LoadConditions(ctx2)
	if err != nil {
		t.Fatalf("LoadConditions final: %v", err)
	}
	if _, ok := conds[subscriberID]; ok {
		t.Fatalf("condition still present after resume: %+v", conds[subscriberID])
	}
}

type recoverCondWaitNode struct {
	eventID     string
	conditionID string
	payload     any
}

func (n recoverCondWaitNode) Execute(ctx context.Context) error {
	id := n.conditionID
	return WaitFor(ctx, Condition{
		ID:      &id,
		EventID: n.eventID,
		Payload: n.payload,
	})
}

type recoverWaitNode struct{ eventID string }

func (n recoverWaitNode) Execute(ctx context.Context) error {
	return WaitFor(ctx, Condition{EventID: n.eventID})
}

type recoverNextNode struct{}

func (recoverNextNode) Execute(ctx context.Context) error {
	ok, err := Get[bool](ctx, "approved")
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("approved is false")
	}
	return nil
}

type recoverWorkNode struct{}

func (recoverWorkNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	return Set(ctx, "mysql_recover_work", "n", n+1)
}

type recoverDoneNode struct{}

func (recoverDoneNode) Execute(ctx context.Context) error {
	n, err := Get[int](ctx, "n")
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("done value = %d, want 4", n)
	}
	return nil
}
