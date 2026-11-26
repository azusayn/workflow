package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type WorkflowEvent struct {
	ID            string
	WorkflowRunID string
	NodeID        string
	Payload       json.RawMessage
}

type Condition struct {
	ID      *string
	EventID string
	Payload any
}

// subscriberMap maps subscriberID (workflowRunID + nodeID) to eventIDs.
type subscriberMap map[string]map[string]struct{}

func (e *Engine) Post(ctx context.Context, event *WorkflowEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.eventCh <- event:
		return nil
	}
}

func (e *Engine) RegisterCondition(conditionID string, check func(context.Context, any) (bool, error)) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.checkerByConditionID[conditionID]; ok {
		return fmt.Errorf("condition %s already registered", conditionID)
	}

	e.checkerByConditionID[conditionID] = check
	return nil
}

func (e *Engine) RegisterCallback(eventID string, callback func(context.Context, any) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.callbackByEventID[eventID]; ok {
		return fmt.Errorf("event %s already registered", eventID)
	}

	e.callbackByEventID[eventID] = callback

	return nil
}

func (e *Engine) StartConditionChecker(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		ticker := time.NewTicker(defaultConditionCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				e.mu.Lock()
				activeConditions := make(map[string]Condition, len(e.activeConditions))
				for subscriberID, cond := range e.activeConditions {
					activeConditions[subscriberID] = cond
				}
				e.mu.Unlock()
				for subscriberID, condition := range activeConditions {
					workflowRunID, nodeID, err := parseSubscriberID(subscriberID)
					if err != nil {
						e.logger.ErrorContext(ctx, "failed to parse subscriber ID", slog.Any("err", err))
						continue
					}
					if err := e.checkCondition(ctx, workflowRunID, nodeID, condition); err != nil {
						e.logger.ErrorContext(ctx, "failed to check condition", slog.Any("workflow_run_id", workflowRunID), slog.Any("node_id", nodeID), slog.Any("err", err))
					}
				}

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return g.Wait()
}

func (e *Engine) StartEventProcessor(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case event := <-e.eventCh:
				if match, err := e.match(event); err != nil {
					e.logger.ErrorContext(ctx, "failed to match event", slog.Any("event", event))
				} else if !match {
					continue
				}
				if err := e.handleWorkflowEvent(ctx, event); err != nil {
					e.logger.ErrorContext(ctx, "failed to process workflow event", slog.Any("err", err))
				}
			}
		}
	})

	return g.Wait()
}

func (e *Engine) checkCondition(ctx context.Context, workflowRunID, nodeID string, condition Condition) error {
	if condition.ID == nil {
		return errors.New("nil condition ID")
	}

	checker, err := e.getCheckerByConditionID(*condition.ID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, defaultConditionCheckTimout)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		met, err := checker(ctx, condition.Payload)
		if err != nil {
			errCh <- err
			return
		}
		if met {
			e.eventCh <- &WorkflowEvent{
				ID:            condition.EventID,
				WorkflowRunID: workflowRunID,
				NodeID:        nodeID,
			}
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case err := <-errCh:
		return err
	}
}

func (e *Engine) getCheckerByConditionID(conditionID string) (func(context.Context, any) (bool, error), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	checker, ok := e.checkerByConditionID[conditionID]
	if !ok {
		return nil, fmt.Errorf("failed to get checker for %q", conditionID)
	}
	return checker, nil
}

func (e *Engine) handleWorkflowEvent(ctx context.Context, event *WorkflowEvent) error {
	callback, err := e.getRegisteredCallback(event.ID)
	if err != nil {
		e.logger.ErrorContext(ctx, "failed to get registered callback", slog.Any("err", err))
		return nil
	}

	run, err := e.getWorkflowRun(event.WorkflowRunID)
	if err != nil {
		return err
	}
	exec, ok := run.executions[event.NodeID]
	if !ok {
		return fmt.Errorf("failed to get node status for node %q", event.NodeID)
	}

	ctx = withNodeStatus(ctx, &exec.Status)
	ctx = withNodeID(ctx, event.NodeID)
	ctx = withWorkflowRun(ctx, run)
	if err := callback(ctx, event.Payload); err != nil {
		exec.Err = err
		exec.Status = NodeStatusFailed
	}

	switch exec.Status {
	case NodeStatusPause:
		return nil

	case NodeStatusFailed,
		NodeStatusCompleted:

	default:
		return fmt.Errorf("invalid node status %q after resuming", exec.Status)
	}

	if err := e.unsubscribeEvent(ctx, event.WorkflowRunID, event.NodeID, event.ID); err != nil {
		return err
	}

	if err := e.store.Transaction(ctx, func(ctx context.Context) error {
		run.mu.Lock()
		defer run.mu.Unlock()

		run.executions[event.NodeID] = exec
		if err := e.store.UpdateWorkflowRun(ctx, convertToStoreUpdateWorkflowRunInput(
			run.id,
			&event.NodeID,
			exec,
			nil,
			exec.Status == NodeStatusCompleted || exec.Status == NodeStatusFailed,
		)); err != nil {
			return err
		}

		if exec.Status == NodeStatusFailed {
			return run.skipSuccessors(ctx, event.NodeID)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to resume: %w", err)
	}

	e.checkAndPurgeInactiveRun(run)

	return e.schedule(ctx, run.nodeConfigs, run.id, event.NodeID)
}

func (e *Engine) getRegisteredCallback(eventID string) (func(context.Context, any) error, error) {
	callback, ok := e.callbackByEventID[eventID]
	if !ok {
		return nil, fmt.Errorf("callback name not found for event: %q", eventID)
	}

	return callback, nil
}

func (e *Engine) subscribeEvent(ctx context.Context, workflowRunID string, nodeID string, condition Condition) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	subscriberID := buildSubscriberID(workflowRunID, nodeID)

	if _, ok := e.subscriberMap[subscriberID]; ok {
		return fmt.Errorf("subscriber %s already subscribed", subscriberID)
	}

	if _, ok := e.subscriberMap[subscriberID]; !ok {
		e.subscriberMap[subscriberID] = make(map[string]struct{})
	}
	e.subscriberMap[subscriberID][condition.EventID] = struct{}{}

	eventIDs := collectEventIDs(e.subscriberMap, subscriberID)
	return e.store.Transaction(ctx, func(ctx context.Context) error {
		if err := e.store.UpsertSubscriberMap(ctx, subscriberID, eventIDs); err != nil {
			return err
		}
		if condition.ID == nil {
			return nil
		}
		payload, err := json.Marshal(condition.Payload)
		if err != nil {
			return err
		}
		if err := e.store.UpsertCondition(ctx, subscriberID, *condition.ID, condition.EventID, payload); err != nil {
			return err
		}
		e.activeConditions[subscriberID] = condition
		return nil
	})
}

func (e *Engine) unsubscribeEvent(ctx context.Context, workflowRunID, nodeID, eventID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	subscriberID := buildSubscriberID(workflowRunID, nodeID)
	if _, ok := e.subscriberMap[subscriberID]; !ok {
		return fmt.Errorf("subscriber %s not found", subscriberID)
	}
	delete(e.subscriberMap[subscriberID], eventID)
	delete(e.activeConditions, subscriberID)

	eventIDs := collectEventIDs(e.subscriberMap, subscriberID)

	return e.store.Transaction(ctx, func(ctx context.Context) error {
		if err := e.store.DeleteCondition(ctx, subscriberID); err != nil {
			return err
		}
		return e.store.UpsertSubscriberMap(ctx, subscriberID, eventIDs)
	})
}

func (e *Engine) match(event *WorkflowEvent) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for subscriberID, eventIDs := range e.subscriberMap {
		workflowRunID, nodeID, err := parseSubscriberID(subscriberID)
		if err != nil {
			return false, err
		}
		if (workflowRunID != event.WorkflowRunID) || (nodeID != event.NodeID) {
			continue
		}
		if _, ok := eventIDs[event.ID]; !ok {
			continue
		}
		return true, nil
	}

	return false, nil
}

func collectEventIDs(subscriberMap map[string]map[string]struct{}, subscriberID string) []string {
	eventIDs := make([]string, 0)
	for eventID := range subscriberMap[subscriberID] {
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs
}

func buildSubscriberID(workflowRunID string, nodeID string) string {
	return fmt.Sprintf("%s:%s", workflowRunID, nodeID)
}

func parseSubscriberID(subscriberID string) (workflowRunID, nodeID string, err error) {
	tokens := strings.Split(subscriberID, ":")
	if len(tokens) != 2 {
		return "", "", fmt.Errorf("invalid subscriber ID %q", subscriberID)
	}
	return tokens[0], tokens[1], nil
}
