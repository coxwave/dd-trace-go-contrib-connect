package connect

import (
	"context"

	"connectrpc.com/connect"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var _ connect.StreamingHandlerConn = (*wrappedStreamingHandlerConn)(nil)

type wrappedStreamingHandlerConn struct {
	connect.StreamingHandlerConn
	cfg *config
	ctx context.Context
}

func (c *wrappedStreamingHandlerConn) Receive(m any) (err error) {
	methodName := c.Spec().Procedure
	_, im := c.cfg.ignoredMethods[methodName]
	_, um := c.cfg.untracedMethods[methodName]
	if c.cfg.traceStreamMessages && !im && !um {
		span, _ := startSpan(
			c.ctx,
			c.RequestHeader(),
			methodName,
			"connect.message",
			c.cfg.serviceName,
			c.cfg.startSpanOptions(tracer.Measured())...,
		)
		defer func() {
			finishWithError(span, err, c.cfg)
		}()
	}
	err = c.StreamingHandlerConn.Receive(m)
	return err
}

func (c *wrappedStreamingHandlerConn) Send(m any) (err error) {
	methodName := c.Spec().Procedure
	_, im := c.cfg.ignoredMethods[methodName]
	_, um := c.cfg.untracedMethods[methodName]
	if c.cfg.traceStreamMessages && !im && !um {
		span, _ := startSpan(
			c.ctx,
			c.RequestHeader(),
			methodName,
			"grpc.message",
			c.cfg.serviceName,
			c.cfg.startSpanOptions(tracer.Measured())...,
		)
		defer func() { finishWithError(span, err, c.cfg) }()
	}
	err = c.StreamingHandlerConn.Send(m)
	return err
}

var _ connect.Interceptor = (*serverInterceptor)(nil)

type serverInterceptor struct {
	cfg *config
}

func (s serverInterceptor) WrapUnary(unaryFunc connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		spec := req.Spec()
		_, im := s.cfg.ignoredMethods[spec.Procedure]
		_, um := s.cfg.untracedMethods[spec.Procedure]
		if im || um {
			return unaryFunc(ctx, req)
		}
		span, ctx := startSpan(
			ctx,
			req.Header(),
			spec.Procedure,
			s.cfg.spanName,
			s.cfg.serviceName,
			s.cfg.startSpanOptions(tracer.Measured(),
				tracer.Tag(ext.SpanKind, ext.SpanKindServer))...,
		)
		span.SetTag(tagMethodKind, methodKindUnary)
		resp, err := unaryFunc(ctx, req)
		finishWithError(span, err, s.cfg)
		return resp, err
	}
}

func (s serverInterceptor) WrapStreamingClient(clientFunc connect.StreamingClientFunc) connect.StreamingClientFunc {
	return clientFunc
}

func (s serverInterceptor) WrapStreamingHandler(handlerFunc connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) (err error) {
		spec := conn.Spec()
		_, im := s.cfg.ignoredMethods[spec.Procedure]
		_, um := s.cfg.untracedMethods[spec.Procedure]
		if s.cfg.traceStreamCalls && !im && !um {
			var span ddtrace.Span
			span, ctx = startSpan(
				ctx,
				conn.RequestHeader(),
				spec.Procedure,
				s.cfg.spanName,
				s.cfg.serviceName,
				s.cfg.startSpanOptions(tracer.Measured(),
					tracer.Tag(ext.SpanKind, ext.SpanKindServer))...,
			)
			switch conn.Spec().StreamType {
			case connect.StreamTypeBidi:
				span.SetTag(tagMethodKind, methodKindBidiStream)
			case connect.StreamTypeServer:
				span.SetTag(tagMethodKind, methodKindServerStream)
			case connect.StreamTypeClient:
				span.SetTag(tagMethodKind, methodKindClientStream)
			}
			defer func() { finishWithError(span, err, s.cfg) }()
		}

		// call the original handler with a new stream, which traces each send
		// and recv if message tracing is enabled
		err = handlerFunc(ctx, &wrappedStreamingHandlerConn{
			StreamingHandlerConn: conn,
			cfg:                  s.cfg,
			ctx:                  ctx,
		})
		return err
	}
}

func NewServerInterceptor(opts ...Option) connect.Interceptor {
	cfg := new(config)
	serverDefaults(cfg)
	for _, opt := range opts {
		opt(cfg)
	}
	return &serverInterceptor{cfg: cfg}
}
