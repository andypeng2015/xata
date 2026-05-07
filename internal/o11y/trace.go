package o11y

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"xata/internal/o11y/version"
)

type tracing struct {
	out             sdktrace.SpanProcessor
	defaultResource *resource.Resource
}

func initTracing(
	ctx context.Context,
	logger *zerolog.Logger,
	res *resource.Resource,
) *tracing {
	traceLogger := logger.With().Str("component", "trace").Logger()

	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithDialOption(
			grpc.WithUnaryInterceptor(GRPCLoggingUnaryClientInterceptor(&traceLogger)),
			grpc.WithStreamInterceptor(GRPCLoggingStreamClientInterceptor(&traceLogger)),
		),
	)
	if err != nil {
		logger.Fatal().AnErr("error", err).Msg("failed to create trace exporter")
		return nil
	}

	return &tracing{
		out:             sdktrace.NewBatchSpanProcessor(traceExporter),
		defaultResource: res,
	}
}

func (t *tracing) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	return t.out.Shutdown(ctx)
}

func (t *tracing) Provider(
	ctx context.Context,
	logger *zerolog.Logger,
	serviceNamespace, serviceName string,
) trace.TracerProvider {
	if t == nil {
		return noop.NewTracerProvider()
	}

	serviceResource := resource.NewSchemaless(
		semconv.ServiceNameKey.String(fmt.Sprintf("%s_%s", serviceNamespace, serviceName)),
		semconv.ServiceVersionKey.String(version.Get()),
	)
	// merge can not error on schemaless resource
	res, _ := resource.Merge(t.defaultResource, serviceResource)

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(t.out),
	)
}

func GRPCLoggingUnaryClientInterceptor(logger *zerolog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		startTime := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		endTime := time.Now()

		evt := grpcLogEvent(logger, err, logger.Debug)
		if !evt.Enabled() {
			return err
		}

		evt.
			Dur("duration", endTime.Sub(startTime)).
			Msg("finished grpc call")
		return err
	}
}

func GRPCLoggingStreamClientInterceptor(logger *zerolog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		startTime := time.Now()
		stream, err := streamer(ctx, desc, cc, method, opts...)
		endTime := time.Now()

		evt := grpcLogEvent(logger, err, logger.Debug)
		if evt.Enabled() {
			evt.
				Dur("duration", endTime.Sub(startTime)).
				Msg("finished client streaming call")
		}

		return stream, err
	}
}

// GRPCLoggingUnaryServerInterceptor returns a new unary server interceptors that logs the payloads of requests.
func GRPCLoggingUnaryServerInterceptor(logger *zerolog.Logger, o *O) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		service, method := callFields(info.FullMethod)

		start := time.Now()

		logCtx := logger.With()

		if md, found := metadata.FromIncomingContext(ctx); found {
			if requestID := md.Get(keyLogRequestID); len(requestID) > 0 {
				logCtx = logCtx.Str(keyLogRequestID, requestID[0])
			}

			if clientID := md.Get(keyLogClientID); len(clientID) > 0 {
				logCtx = logCtx.Str(keyLogClientID, clientID[0])
			}

			if sessionID := md.Get(keyLogSessionID); len(sessionID) > 0 {
				logCtx = logCtx.Str(keyLogSessionID, sessionID[0])
			}
		}

		if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
			kTraceID, vTraceID := o.system.idStyle.TraceID(spanCtx.TraceID())
			kSpanID, vSpanID := o.system.idStyle.SpanID(spanCtx.SpanID())
			logCtx = logCtx.Str(kTraceID, vTraceID).Str(kSpanID, vSpanID)
		}

		reqLogger := logCtx.Logger()
		ctx = reqLogger.WithContext(ctx)

		resp, err := handler(ctx, req)

		stop := time.Now()

		evt := grpcLogEvent(&reqLogger, err, reqLogger.Info)

		if evt.Enabled() {
			evt.Str("grpc.service", service).
				Str("grpc.method", method).
				Dur("latency", stop.Sub(start)).Send()
		}

		return resp, err
	}
}

// GRPCLoggingStreamServerInterceptor returns a new unary server interceptors that logs the payloads of requests.
func GRPCLoggingStreamServerInterceptor(logger *zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		service, method := callFields(info.FullMethod)

		start := time.Now()

		err := handler(srv, stream)

		stop := time.Now()

		evt := grpcLogEvent(logger, err, logger.Info)

		if evt.Enabled() {
			evt.Str("grpc.service", service).
				Str("grpc.method", method).
				Dur("latency", stop.Sub(start)).Send()
		}

		return err
	}
}

func callFields(fullMethodString string) (string, string) {
	service := path.Dir(fullMethodString)[1:]
	method := path.Base(fullMethodString)

	return service, method
}

func grpcLogEvent(logger *zerolog.Logger, err error, successEvent func() *zerolog.Event) *zerolog.Event {
	if err == nil {
		return successEvent()
	}
	return logger.WithLevel(grpcLogLevel(err)).Err(err)
}

func grpcLogLevel(err error) zerolog.Level {
	if status.Code(err) == codes.NotFound {
		return zerolog.WarnLevel
	}
	return zerolog.ErrorLevel
}
