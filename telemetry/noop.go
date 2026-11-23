package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type noopTracer struct{}

func NewNoopTracer() Tracer {
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

func NewNoopPropagator() Propagator {
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
