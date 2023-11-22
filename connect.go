package connect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// cache a constant option: saves one allocation per call
var spanTypeRPC = tracer.SpanType(ext.AppTypeRPC)

func (cfg *config) startSpanOptions(opts ...tracer.StartSpanOption) []tracer.StartSpanOption {
	if len(cfg.tags) == 0 && len(cfg.spanOpts) == 0 {
		return opts
	}

	ret := make([]tracer.StartSpanOption, 0, 1+len(cfg.tags)+len(opts))
	for _, opt := range opts {
		ret = append(ret, opt)
	}
	for _, opt := range cfg.spanOpts {
		ret = append(ret, opt)
	}
	for key, tag := range cfg.tags {
		ret = append(ret, tracer.Tag(key, tag))
	}
	return ret
}

func startSpan(
	ctx context.Context,
	headers http.Header,
	method string,
	operation string,
	serviceFn func() string,
	opts ...tracer.StartSpanOption,
) (ddtrace.Span, context.Context) {
	// common stuff
	opts = append(opts,
		tracer.ServiceName(serviceFn()),
		tracer.ResourceName(method),
		tracer.Tag(tagMethodName, method),
	)

	// gRPC Spec
	methodElements := strings.SplitN(strings.TrimPrefix(method, "/"), "/", 2)
	opts = append(opts,
		spanTypeRPC,
		tracer.Tag(ext.RPCSystem, extRPCSystemConnect),
		tracer.Tag(ext.GRPCFullMethod, method),
		tracer.Tag(ext.RPCService, methodElements[0]),
	)

	// http Spec
	if sctx, err := tracer.Extract(tracer.HTTPHeadersCarrier(headers)); err == nil {
		opts = append(opts, tracer.ChildOf(sctx))
	}
	return tracer.StartSpanFromContext(ctx, operation, opts...)
}

// finishWithError applies finish option and a tag with gRPC status code, disregarding OK, EOF and Canceled errors.
func finishWithError(span ddtrace.Span, err error, cfg *config) {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		errcode := connect.CodeOf(err)
		if cfg.nonErrorCodes[errcode] {
			err = nil
		}
		span.SetTag(tagCode, errcode.String())
	}

	// only allocate finishOptions if needed, and allocate the exact right size
	var finishOptions []tracer.FinishOption
	if err != nil {
		if cfg.noDebugStack {
			finishOptions = []tracer.FinishOption{tracer.WithError(err), tracer.NoDebugStack()}
		} else {
			finishOptions = []tracer.FinishOption{tracer.WithError(err)}
		}
	}
	span.Finish(finishOptions...)
}
