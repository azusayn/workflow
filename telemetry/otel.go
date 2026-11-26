package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

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
		return err
	}

	return nil
}

type otelPropagator struct {
	propagator propagation.TextMapPropagator
}

func NewOTelPropagator(propagator propagation.TextMapPropagator) Propagator {
	return &otelPropagator{
		propagator: propagator,
	}
}

func (p *otelPropagator) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return p.propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

func (p *otelPropagator) Inject(ctx context.Context, carrier map[string]string) {
	p.propagator.Inject(ctx, propagation.MapCarrier(carrier))
}
