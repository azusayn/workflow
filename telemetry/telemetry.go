package telemetry

import (
	"context"
)

type AttributeKey string

type SetSpanAttributeFunc func(key AttributeKey, value string)

type Tracer interface {
	WithSpan(ctx context.Context, spanName string, fn func(ctx context.Context, setAttribute SetSpanAttributeFunc) error) error
}

type Propagator interface {
	Extract(ctx context.Context, carrier map[string]string) context.Context
	Inject(ctx context.Context, carrier map[string]string)
}
