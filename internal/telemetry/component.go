package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/pomerium/pomerium/internal/log"
)

var tracer = otel.Tracer("")

// A Component represents a component of Pomerium and is used to trace and log operations
type Component struct {
	logLevel   zerolog.Level
	component  string
	attributes []attribute.KeyValue
}

// NewComponent creates a new Component.
func NewComponent(logLevel zerolog.Level, component, provider string, attributes ...attribute.KeyValue) *Component {
	c := &Component{
		logLevel:  logLevel,
		component: component,
		attributes: append([]attribute.KeyValue{
			attribute.String("component", component),
			attribute.String("provider", provider),
		}, attributes...),
	}
	l := logger(context.Background(), c.attributes...)
	l.Info().Msgf("%s: using %s provider", component, provider)
	return c
}

// Start starts an operation.
func (c *Component) Start(ctx context.Context, operationName string, attributes ...attribute.KeyValue) (context.Context, Operation) {
	attributes = append(c.attributes, attributes...)

	// setup tracing
	ctx, span := tracer.Start(ctx, c.component+"."+operationName, trace.WithAttributes(attributes...))

	// setup logging
	ctx = logger(ctx, attributes...).WithContext(ctx)

	op := Operation{
		c:     c,
		name:  operationName,
		ctx:   ctx,
		span:  span,
		start: time.Now(),
	}

	return ctx, op
}

// An Operation represents an operation that can be traced and logged.
type Operation struct {
	c     *Component
	name  string
	ctx   context.Context
	span  trace.Span
	done  bool
	start time.Time
}

// Failure logs and traces the operation as an error and returns a wrapped error with additional info.
func (op *Operation) Failure(err error, attributes ...attribute.KeyValue) error {
	op.complete(err, attributes...)
	return fmt.Errorf("%s: %s failed: %w", op.c.component, op.name, err)
}

// Complete completes an operation.
func (op *Operation) Complete(attributes ...attribute.KeyValue) {
	op.complete(nil, attributes...)
}

func (op *Operation) complete(err error, attributes ...attribute.KeyValue) {
	if op.done {
		return
	}
	op.done = true

	if err == nil {
		l := logger(op.ctx, attributes...)
		l.WithLevel(op.c.logLevel).Msgf("%s.%s succeeded", op.c.component, op.name)

		op.span.SetStatus(codes.Ok, "ok")
	} else {
		l := logger(op.ctx, attributes...)
		l.Error().Err(err).Msgf("%s.%s failed", op.c.component, op.name)

		op.span.RecordError(err)
		op.span.SetStatus(codes.Error, err.Error())
	}
	op.span.End()
}

func logger(ctx context.Context, attributes ...attribute.KeyValue) zerolog.Logger {
	logCtx := log.Ctx(ctx).With()
	for _, a := range attributes {
		logCtx = logCtx.Interface(string(a.Key), a.Value.AsInterface())
	}
	return logCtx.Logger()
}
