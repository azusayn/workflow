package workflow

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type AttributeKey string

const (
	AttributeKeyWorkflowRunID AttributeKey = "workflow.workflow_run_id"
)

type SetSpanAttributeFunc func(key AttributeKey, value string)

type Tracer interface {
	WithSpan(ctx context.Context, spanName string, fn func(ctx context.Context, setAttribute SetSpanAttributeFunc) error) error
}

type Propagator interface {
	Extract(ctx context.Context, carrier map[string]string) context.Context
	Inject(ctx context.Context, carrier map[string]string)
}

type noopTracer struct{}

func newNoopTracer() Tracer {
	return &noopTracer{}
}

func (t *noopTracer) WithSpan(
	ctx context.Context,
	spanName string,
	fn func(ctx context.Context, setAttribute SetSpanAttributeFunc) error,
) error {
	setAttribute := func(AttributeKey, string) {}
	return fn(ctx, setAttribute)
}

type noopPropagator struct{}

func newNoopPropagator() Propagator {
	return &noopPropagator{}
}
func (p *noopPropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

func (p *noopPropagator) Inject(_ context.Context, _ map[string]string) {}

type otelTracer struct {
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

func NewOTelTracer(tracer trace.Tracer, attributes ...attribute.KeyValue) Tracer {
	return &otelTracer{
		tracer:     tracer,
		attributes: attributes,
	}
}

func (t *otelTracer) WithSpan(ctx context.Context, spanName string, fn func(ctx context.Context, setAttribute SetSpanAttributeFunc) error) error {
	ctx, span := t.tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(t.attributes...),
	)
	defer span.End()

	setAttribute := func(key AttributeKey, value string) {
		span.SetAttributes(attribute.Key(key).String(value))
	}

	if err := fn(ctx, setAttribute); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return nil
}

type OTelPropagator struct {
	propagator propagation.TextMapPropagator
}

func NewOTelPropagator(propagator propagation.TextMapPropagator) Propagator {
	return &OTelPropagator{
		propagator: propagator,
	}
}

func (p *OTelPropagator) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return p.propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

func (p *OTelPropagator) Inject(ctx context.Context, carrier map[string]string) {
	p.propagator.Inject(ctx, propagation.MapCarrier(carrier))
}
