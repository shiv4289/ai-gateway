// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package usageevents

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	metricExportedTotal  = "usage_events_exported_total"
	metricFailedTotal    = "usage_events_failed_total"
	metricDroppedTotal   = "usage_events_dropped_total"
	metricExportDuration = "usage_event_export_duration_ms"
)

type Telemetry struct {
	exportedCounter metric.Int64Counter
	failedCounter   metric.Int64Counter
	droppedCounter  metric.Int64Counter
	durationHist    metric.Float64Histogram
}

func NewTelemetry(meter metric.Meter) (*Telemetry, error) {
	exported, err := meter.Int64Counter(metricExportedTotal, metric.WithDescription("Number of successfully exported usage events."))
	if err != nil {
		return nil, fmt.Errorf("failed to register %s: %w", metricExportedTotal, err)
	}
	failed, err := meter.Int64Counter(metricFailedTotal, metric.WithDescription("Number of usage event export attempts that failed."))
	if err != nil {
		return nil, fmt.Errorf("failed to register %s: %w", metricFailedTotal, err)
	}
	dropped, err := meter.Int64Counter(metricDroppedTotal, metric.WithDescription("Number of usage events intentionally dropped."))
	if err != nil {
		return nil, fmt.Errorf("failed to register %s: %w", metricDroppedTotal, err)
	}
	duration, err := meter.Float64Histogram(metricExportDuration,
		metric.WithUnit("ms"),
		metric.WithDescription("Usage event export duration in milliseconds."),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register %s: %w", metricExportDuration, err)
	}
	return &Telemetry{
		exportedCounter: exported,
		failedCounter:   failed,
		droppedCounter:  dropped,
		durationHist:    duration,
	}, nil
}

func (t *Telemetry) recordExported(ctx context.Context, mode, sink string, durationMs float64) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("usage_events.mode", mode),
		attribute.String("usage_events.sink", sink),
	)
	t.exportedCounter.Add(ctx, 1, attrs)
	t.durationHist.Record(ctx, durationMs, attrs)
}

func (t *Telemetry) recordFailed(ctx context.Context, mode, sink string, durationMs float64) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("usage_events.mode", mode),
		attribute.String("usage_events.sink", sink),
	)
	t.failedCounter.Add(ctx, 1, attrs)
	t.durationHist.Record(ctx, durationMs, attrs)
}

func (t *Telemetry) recordDropped(ctx context.Context, mode, sink string) {
	if t == nil {
		return
	}
	t.droppedCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("usage_events.mode", mode),
			attribute.String("usage_events.sink", sink),
		),
	)
}
