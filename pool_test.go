package workflow

import (
	"context"
	"testing"
)

func testRunContext(configs map[string]*NodeConfig, pool *VariablePool, nodeID string) context.Context {
	run := &workflowRun{
		pool:        pool,
		nodeConfigs: configs,
	}
	ctx := withWorkflowRun(context.Background(), run)
	return withNodeID(ctx, nodeID)
}

func TestVariablePoolGetIntAfterDumpLoad(t *testing.T) {
	configs := map[string]*NodeConfig{
		"producer": {},
		"consumer": {
			Inputs: NodeInputConfig{"score": "producer"},
		},
	}
	pool := NewVariablePool(configs)
	pool.Set("producer", "score", 95)

	raw, err := pool.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	reloaded := NewVariablePool(configs)
	if err := reloaded.Load(raw); err != nil {
		t.Fatalf("Load: %v", err)
	}

	got, err := Get[int](testRunContext(configs, reloaded, "consumer"), "score")
	if err != nil {
		t.Fatalf("Get[int]: %v", err)
	}
	if got != 95 {
		t.Fatalf("Get[int] = %d, want 95", got)
	}
}

func TestVariablePoolBindIntAfterDumpLoad(t *testing.T) {
	configs := map[string]*NodeConfig{
		"producer": {},
		"consumer": {
			Inputs: NodeInputConfig{
				"score": "producer",
				"name":  "producer",
			},
		},
	}
	pool := NewVariablePool(configs)
	pool.Set("producer", "score", 7)
	pool.Set("producer", "name", "alpha")

	raw, err := pool.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	reloaded := NewVariablePool(configs)
	if err := reloaded.Load(raw); err != nil {
		t.Fatalf("Load: %v", err)
	}

	type input struct {
		Score int    `workflow:"score"`
		Name  string `workflow:"name"`
		Skip  string `workflow:"missing,optional"`
	}
	var in input
	if err := Bind(testRunContext(configs, reloaded, "consumer"), &in); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if in.Score != 7 || in.Name != "alpha" {
		t.Fatalf("Bind result = %+v, want score=7 name=alpha", in)
	}
}

func TestVariablePoolBindStructAfterDumpLoad(t *testing.T) {
	type payload struct {
		N int    `json:"n"`
		S string `json:"s"`
	}
	configs := map[string]*NodeConfig{
		"producer": {},
		"consumer": {
			Inputs: NodeInputConfig{"body": "producer"},
		},
	}
	pool := NewVariablePool(configs)
	pool.Set("producer", "body", payload{N: 3, S: "x"})

	raw, err := pool.Dump()
	if err != nil {
		t.Fatalf("Dump: %v", err)
	}
	reloaded := NewVariablePool(configs)
	if err := reloaded.Load(raw); err != nil {
		t.Fatalf("Load: %v", err)
	}

	type input struct {
		Body payload `workflow:"body"`
	}
	var in input
	if err := Bind(testRunContext(configs, reloaded, "consumer"), &in); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if in.Body.N != 3 || in.Body.S != "x" {
		t.Fatalf("Bind body = %+v, want {3 x}", in.Body)
	}
}
