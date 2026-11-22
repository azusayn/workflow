package workflow

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	attrKeyWorkflowRunID = attribute.Key("workflow.workflow_run_id")
)

type TraceConfig struct {
	Tracer     trace.Tracer
	Attrs      []attribute.KeyValue
	Propagator propagation.TextMapPropagator
}

func defaultTraceConfig() TraceConfig {
	return TraceConfig{
		Tracer: noop.NewTracerProvider().Tracer("workflow"),
		Propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}
}

func (e *Engine) withSpan(ctx context.Context, spanName string, fn func(ctx context.Context, span trace.Span) error) error {
	ctx, span := e.traceConfig.Tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(e.traceConfig.Attrs...),
	)
	defer span.End()

	err := fn(ctx, span)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
