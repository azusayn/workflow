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
	tracer     trace.Tracer
	attrs      []attribute.KeyValue
	propagator propagation.TextMapPropagator
}

func defaultTraceConfig() TraceConfig {
	return TraceConfig{
		tracer: noop.NewTracerProvider().Tracer("workflow"),
		propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}
}

func (e *Engine) withSpan(ctx context.Context, spanName string, fn func(ctx context.Context, span trace.Span) error) error {
	ctx, span := e.traceConfig.tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(e.traceConfig.attrs...),
	)
	defer span.End()

	err := fn(ctx, span)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
