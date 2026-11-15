package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

const (
	tagNameWorkflow   = "workflow"
	tagOptionOptional = "optional"
)

type VariablePool struct {
	values sync.Map
}

type VariableKey struct {
	NodeID string
	Key    string
}

func NewVariablePool(nodeConfigs map[string]*NodeConfig) *VariablePool {
	return &VariablePool{}
}

type VariableRecord struct {
	NodeID string `json:"node_id"`
	Key    string `json:"key"`
	Value  any    `json:"value"`
}

func (p *VariablePool) Dump() (json.RawMessage, error) {
	var records []VariableRecord
	p.values.Range(func(key, value any) bool {
		k := key.(VariableKey)
		records = append(records, VariableRecord{
			NodeID: k.NodeID,
			Key:    k.Key,
			Value:  value,
		})
		return true
	})
	return json.Marshal(records)
}

func (p *VariablePool) Load(data []byte) error {
	var records []VariableRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}
	p.values.Range(func(key, _ any) bool {
		p.values.Delete(key)
		return true
	})
	for _, entry := range records {
		p.values.Store(VariableKey{NodeID: entry.NodeID, Key: entry.Key}, entry.Value)
	}
	return nil
}

func (p *VariablePool) Set(nodeID, key string, value any) {
	p.values.Store(VariableKey{NodeID: nodeID, Key: key}, value)
}

func Set(ctx context.Context, nodeID, key string, value any) error {
	workflowRun, err := extractWorkflowRun(ctx)
	if err != nil {
		return err
	}
	workflowRun.pool.Set(nodeID, key, value)
	return nil
}

func Get[T any](ctx context.Context, key string) (T, error) {
	var zero T
	workflowRun, err := extractWorkflowRun(ctx)
	if err != nil {
		return zero, err
	}
	nodeID, err := extractNodeID(ctx)
	if err != nil {
		return zero, err
	}

	config, ok := workflowRun.nodeConfigs[nodeID]
	if !ok {
		return zero, fmt.Errorf("failed to find input config for node %s", nodeID)
	}
	sourceNodeID, ok := config.Inputs[key]
	if !ok {
		return zero, fmt.Errorf("failed to find input %s for node %s", key, nodeID)
	}
	variableKey := VariableKey{NodeID: sourceNodeID, Key: key}
	value, ok := workflowRun.pool.values.Load(variableKey)
	if !ok {
		return zero, fmt.Errorf("failed to find value for workflow key %+v", variableKey)
	}
	if typedValue, ok := value.(T); ok {
		return typedValue, nil
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("workflow input %s.%s (type %T) marshal failed: %w", nodeID, key, value, err)
	}
	var typedValue T
	if err := json.Unmarshal(raw, &typedValue); err != nil {
		return zero, fmt.Errorf("workflow input %s.%s has unexpected type %T: %w", nodeID, key, value, err)
	}
	return typedValue, nil
}

func Bind(ctx context.Context, input any) error {
	val := reflect.ValueOf(input)
	if val.Kind() != reflect.Pointer || val.Elem().Kind() != reflect.Struct {
		return errors.New("input must be a pointer to a struct")
	}

	workflowRun, err := extractWorkflowRun(ctx)
	if err != nil {
		return err
	}
	nodeID, err := extractNodeID(ctx)
	if err != nil {
		return err
	}

	config, ok := workflowRun.nodeConfigs[nodeID]
	if !ok {
		return fmt.Errorf("failed to find input config for node %s", nodeID)
	}

	structValue := val.Elem()
	structType := structValue.Type()

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		tagValue := field.Tag.Get(tagNameWorkflow)
		if tagValue == "" {
			continue
		}

		parts := strings.Split(tagValue, ",")
		tag := parts[0]
		optional := false
		if len(parts) > 1 && parts[1] == tagOptionOptional {
			optional = true
		}

		sourceNodeID, ok := config.Inputs[tag]
		if !ok {
			if optional {
				continue
			}
			return fmt.Errorf("failed to find input %s for node %s (field %s)", tag, nodeID, field.Name)
		}

		variableKey := VariableKey{NodeID: sourceNodeID, Key: tag}
		value, ok := workflowRun.pool.values.Load(variableKey)
		if !ok {
			if optional {
				continue
			}
			return fmt.Errorf("failed to find value for workflow key %+v (field %s)", variableKey, field.Name)
		}

		fieldValue := structValue.Field(i)
		if !fieldValue.CanSet() {
			continue
		}

		valToSet := reflect.ValueOf(value)
		if valToSet.IsValid() && valToSet.Type().AssignableTo(fieldValue.Type()) {
			fieldValue.Set(valToSet)
			continue
		}

		raw, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("workflow input %s.%s (type %T) marshal for field %s failed: %w",
				nodeID, tag, value, field.Name, err)
		}
		dest := reflect.New(fieldValue.Type())
		if err := json.Unmarshal(raw, dest.Interface()); err != nil {
			return fmt.Errorf("workflow input %s.%s (type %T) is not assignable to field %s (type %s): %w",
				nodeID, tag, value, field.Name, fieldValue.Type(), err)
		}
		fieldValue.Set(dest.Elem())
	}

	return nil
}
