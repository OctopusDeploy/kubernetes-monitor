package run

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	noopmetric "go.opentelemetry.io/otel/metric/noop"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

var serviceNamespace, _ = resource.New(context.Background(),
	resource.WithAttributes(semconv.ServiceNameKey.String("kubernetes-monitor")),
)

type OtelConfig struct {
	TracingEnabled bool
	PrintTraces    bool
}

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context, config OtelConfig) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	var traceExporter trace.SpanExporter
	var metricExporter metric.Exporter
	if config.TracingEnabled {
		if config.PrintTraces {
			traceExporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
			handleErr(err)
			metricExporter, err = stdoutmetric.New(stdoutmetric.WithPrettyPrint())
			handleErr(err)
		} else {
			traceExporter, err = otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
			handleErr(err)
			metricExporter, err = otlpmetrichttp.New(ctx, otlpmetrichttp.WithInsecure())
			handleErr(err)
		}
	} else {
		otel.SetTracerProvider(nooptrace.NewTracerProvider())
		otel.SetMeterProvider(noopmetric.NewMeterProvider())
		return
	}

	// Set up trace provider
	tracerProvider := newTracerProvider(ctx, traceExporter)
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// Set up meter provider.
	meterProvider := newMeterProvider(ctx, metricExporter)
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider(_ context.Context, exporter trace.SpanExporter) *trace.TracerProvider {
	tracerProvider := trace.NewTracerProvider(
		trace.WithResource(serviceNamespace),
		trace.WithBatcher(exporter),
	)
	return tracerProvider
}

func newMeterProvider(_ context.Context, exporter metric.Exporter) *metric.MeterProvider {
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(serviceNamespace),
		metric.WithReader(metric.NewPeriodicReader(exporter)),
	)
	return meterProvider
}
