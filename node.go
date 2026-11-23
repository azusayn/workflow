package workflow

import (
	"context"
	"errors"
	"sync"
)

// TODO: distinguish between node types and node IDs.
const RootNodeID = "root"

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "pending"
	NodeStatusFailed    NodeStatus = "failed"
	NodeStatusPause     NodeStatus = "pause"
	NodeStatusCompleted NodeStatus = "completed"
	NodeStatusSkipped   NodeStatus = "skipped"
)

var (
	registryMu sync.Mutex
	registry   = map[string]Node{
		RootNodeID: rootNode{},
	}
)

type Node interface {
	Execute(ctx context.Context) error
}

type rootNode struct{}

func (rootNode) Execute(context.Context) error {
	return nil
}

func Register(nodeType string, node Node) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[nodeType] = node
}

func Lookup(nodeType string) (Node, bool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	found, ok := registry[nodeType]
	return found, ok
}

func WaitFor(ctx context.Context, condition Condition) error {
	status, err := extractNodeStatus(ctx)
	if err != nil {
		return err
	}
	*status = NodeStatusPause

	ctxCondition, err := extractCondition(ctx)
	if err != nil {
		return err
	}
	*ctxCondition = condition

	return nil
}

func Resume(ctx context.Context) error {
	nodeStatus, err := extractNodeStatus(ctx)
	if err != nil {
		return err
	}

	if *nodeStatus != NodeStatusPause {
		return errors.New("cannot resume unsuspend node")
	}
	*nodeStatus = NodeStatusCompleted

	return nil
}
