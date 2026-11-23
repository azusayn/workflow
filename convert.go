package workflow

import (
	"encoding/json"

	"github.com/azusayn/workflow/store"
)

func convertToStoreNodeStatus(status NodeStatus) store.NodeStatus {
	return store.NodeStatus(status)
}

func convertToNodeStatus(status store.NodeStatus) NodeStatus {
	return NodeStatus(status)
}

func convertToStoreNodeInputConfig(nodeInputConfig NodeInputConfig) store.NodeInputConfig {
	if nodeInputConfig == nil {
		return nil
	}
	storeNodeInputConfig := make(store.NodeInputConfig, len(nodeInputConfig))
	for inputKey, sourceNodeID := range nodeInputConfig {
		storeNodeInputConfig[inputKey] = sourceNodeID
	}
	return storeNodeInputConfig
}

func convertToNodeInputConfig(storeNodeInputConfig store.NodeInputConfig) NodeInputConfig {
	if storeNodeInputConfig == nil {
		return nil
	}
	workflowNodeInputConfig := make(NodeInputConfig, len(storeNodeInputConfig))
	for inputKey, sourceNodeID := range storeNodeInputConfig {
		workflowNodeInputConfig[inputKey] = sourceNodeID
	}
	return workflowNodeInputConfig
}

func convertToStoreNodeConfig(config *NodeConfig) *store.NodeConfig {
	if config == nil {
		return nil
	}
	return &store.NodeConfig{
		Inputs:          convertToStoreNodeInputConfig(config.Inputs),
		InDegree:        config.InDegree,
		NextNodeIDs:     append([]string(nil), config.NextNodeIDs...),
		ContinueOnError: config.ContinueOnError,
	}
}

func convertToNodeConfig(config *store.NodeConfig) *NodeConfig {
	if config == nil {
		return nil
	}
	return &NodeConfig{
		Inputs:          convertToNodeInputConfig(config.Inputs),
		InDegree:        config.InDegree,
		NextNodeIDs:     append([]string(nil), config.NextNodeIDs...),
		ContinueOnError: config.ContinueOnError,
	}
}

func convertToStoreNodeConfigs(configs map[string]*NodeConfig) map[string]*store.NodeConfig {
	if configs == nil {
		return nil
	}
	storeNodeConfigs := make(map[string]*store.NodeConfig, len(configs))
	for nodeID, nodeConfig := range configs {
		storeNodeConfigs[nodeID] = convertToStoreNodeConfig(nodeConfig)
	}
	return storeNodeConfigs
}

func convertToNodeConfigs(configs map[string]*store.NodeConfig) map[string]*NodeConfig {
	if configs == nil {
		return nil
	}
	workflowNodeConfigs := make(map[string]*NodeConfig, len(configs))
	for nodeID, nodeConfig := range configs {
		workflowNodeConfigs[nodeID] = convertToNodeConfig(nodeConfig)
	}
	return workflowNodeConfigs
}

func convertToStoreNodeExecution(exec *NodeExecution) *store.NodeExecution {
	if exec == nil {
		return nil
	}
	return &store.NodeExecution{
		ReadyInDegree: exec.ReadyInDegree,
		Err:           exec.Err,
		Status:        convertToStoreNodeStatus(exec.Status),
	}
}

func convertToNodeExecution(exec *store.NodeExecution) *NodeExecution {
	if exec == nil {
		return nil
	}
	return &NodeExecution{
		ReadyInDegree: exec.ReadyInDegree,
		Err:           exec.Err,
		Status:        convertToNodeStatus(exec.Status),
	}
}

func convertToStoreNodeExecutions(executions map[string]*NodeExecution) map[string]*store.NodeExecution {
	if executions == nil {
		return nil
	}
	storeNodeExecutions := make(map[string]*store.NodeExecution, len(executions))
	for nodeID, nodeExecution := range executions {
		storeNodeExecutions[nodeID] = convertToStoreNodeExecution(nodeExecution)
	}
	return storeNodeExecutions
}

func convertToNodeExecutions(executions map[string]*store.NodeExecution) map[string]*NodeExecution {
	if executions == nil {
		return nil
	}
	workflowNodeExecutions := make(map[string]*NodeExecution, len(executions))
	for nodeID, nodeExecution := range executions {
		workflowNodeExecutions[nodeID] = convertToNodeExecution(nodeExecution)
	}
	return workflowNodeExecutions
}

func convertToStoreWorkflowRunModel(workflowID string, data json.RawMessage, executions map[string]*NodeExecution, traceCarrier map[string]string) *store.WorkflowRun {
	return &store.WorkflowRun{
		WorkflowID:     workflowID,
		Data:           data,
		NodeExecutions: convertToStoreNodeExecutions(executions),
		TraceCarrier:   traceCarrier,
	}
}

func convertToStoreUpdateWorkflowRunInput(
	workflowRunID string,
	nodeID *string,
	execution *NodeExecution,
	data *json.RawMessage,
	incrementCompleted bool,
) store.UpdateWorkflowRunInput {
	var storeNodeExecution *store.NodeExecution
	if execution != nil {
		storeNodeExecution = convertToStoreNodeExecution(execution)
	}
	return store.UpdateWorkflowRunInput{
		WorkflowRunID:      workflowRunID,
		NodeID:             nodeID,
		Execution:          storeNodeExecution,
		Data:               data,
		IncrementCompleted: incrementCompleted,
	}
}

func convertToCondition(model store.Condition) Condition {
	conditionID := model.ID
	payload := append(json.RawMessage(nil), model.Payload...)
	return Condition{
		ID:      &conditionID,
		EventID: model.EventID,
		Payload: payload,
	}
}
